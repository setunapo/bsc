// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package trie implements Merkle Patricia Tries.
package trie

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyState is the known hash of an empty state trie entry.
	emptyState = crypto.Keccak256Hash(nil)
)

// LeafCallback is a callback type invoked when a trie operation reaches a leaf
// node.
//
// The paths is a path tuple identifying a particular trie node either in a single
// trie (account) or a layered trie (account -> storage). Each path in the tuple
// is in the raw format(32 bytes).
//
// The hexpath is a composite hexary path identifying the trie node. All the key
// bytes are converted to the hexary nibbles and composited with the parent path
// if the trie node is in a layered trie.
//
// It's used by state sync and commit to allow handling external references
// between account and storage tries. And also it's used in the state healing
// for extracting the raw states(leaf nodes) with corresponding paths.
type LeafCallback func(paths [][]byte, hexpath []byte, leaf []byte, parent common.Hash) error

// Trie is a Merkle Patricia Trie.
// The zero value is an empty trie with no database.
// Use New to create a trie that sits on top of a database.
//
// Trie is not safe for concurrent use.
type Trie struct {
	db   *Database
	sndb ShadowNodeStorage // only storage trie using it, account trie needn't the shadow node
	root node
	// Keep track of the number leafs which have been inserted since the last
	// hashing operation. This number will not directly map to the number of
	// actually unhashed nodes
	unhashed int

	// fields for shadow tree & state epoch
	withShadowNodes bool
	currentEpoch    types.StateEpoch

	// fields for rootNode
	shadowTreeRoot common.Hash
	rootEpoch      types.StateEpoch
	trieRoot       common.Hash
}

// newFlag returns the cache flag value for a newly created node.
func (t *Trie) newFlag() nodeFlag {
	return nodeFlag{dirty: true}
}

// New creates a trie with an existing root node from db.
//
// If root is the zero hash or the sha3 hash of an empty string, the
// trie is initially empty and does not require a database. Otherwise,
// New will panic if db is nil and returns a MissingNodeError if root does
// not exist in the database. Accessing the trie loads nodes from db on demand.
func New(root common.Hash, db *Database) (*Trie, error) {
	if db == nil {
		panic("trie.New called without a database")
	}
	trie := &Trie{
		db: db,
	}
	if root != (common.Hash{}) && root != emptyRoot {
		rootnode, err := trie.resolveHash(root[:], nil)
		if err != nil {
			return nil, err
		}
		trie.root = rootnode
	}
	return trie, nil
}

func NewWithShadowNode(curEpoch types.StateEpoch, rootNode *rootNode, db *Database, sndb ShadowNodeStorage) (*Trie, error) {
	if db == nil || sndb == nil {
		panic("trie.New called without a database")
	}
	if curEpoch < rootNode.Epoch {
		return nil, errors.New("open trie at a wrong epoch")
	}

	useShadowTree := false
	// only enable after first state expiry's hard fork
	if curEpoch > types.StateEpoch0 {
		useShadowTree = true
		log.Debug("withShadowNodes trie open", "rootNodeHash", rootNode.cachedHash, "RootHash", rootNode.TrieRoot, "ShadowTreeRoot", rootNode.ShadowTreeRoot)
	}

	trie := &Trie{
		db:              db,
		sndb:            sndb,
		currentEpoch:    curEpoch,
		withShadowNodes: useShadowTree,
		shadowTreeRoot:  rootNode.ShadowTreeRoot,
		rootEpoch:       rootNode.Epoch,
		trieRoot:        rootNode.TrieRoot,
	}

	if rootNode.TrieRoot != (common.Hash{}) && rootNode.TrieRoot != emptyRoot {
		// check root if expired, if expired just set as hashNode
		if types.EpochExpired(rootNode.Epoch, curEpoch) {
			trie.root = hashNode(rootNode.TrieRoot.Bytes())
			return trie, nil
		}
		root, err := trie.resolveHash(rootNode.TrieRoot[:], nil)
		if err != nil {
			return nil, err
		}
		if err = trie.resolveShadowNode(rootNode.Epoch, root, nil); err != nil {
			return nil, err
		}
		trie.root = root
	}
	return trie, nil
}

// NodeIterator returns an iterator that returns nodes of the trie. Iteration starts at
// the key after the given start key.
func (t *Trie) NodeIterator(start []byte) NodeIterator {
	return newNodeIterator(t, start)
}

// Get returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
func (t *Trie) Get(key []byte) []byte {
	res, err := t.TryGet(key)
	if err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
	return res
}

// TryGet returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryGet(key []byte) (value []byte, err error) {
	var newroot node
	var didResolve bool
	if t.withShadowNodes {
		value, newroot, didResolve, err = t.tryGetWithEpoch(t.root, keybytesToHex(key), 0, t.getRootEpoch(), false)
	} else {
		value, newroot, didResolve, err = t.tryGet(t.root, keybytesToHex(key), 0)
	}

	if err == nil && didResolve {
		t.root = newroot
	}
	return value, err
}

func (t *Trie) TryGetAndUpdateEpoch(key []byte) ([]byte, error) {
	value, newroot, didResolve, err := t.tryGetWithEpoch(t.root, keybytesToHex(key), 0, t.getRootEpoch(), true)

	if err == nil && didResolve {
		t.root = newroot
	}
	return value, err
}

func (t *Trie) tryGet(origNode node, key []byte, pos int) (value []byte, newnode node, didResolve bool, err error) {
	switch n := (origNode).(type) {
	case nil:
		return nil, nil, false, nil
	case valueNode:
		return n, n, false, nil
	case *shortNode:
		if len(key)-pos < len(n.Key) || !bytes.Equal(n.Key, key[pos:pos+len(n.Key)]) {
			// key not found in trie
			return nil, n, false, nil
		}
		value, newnode, didResolve, err = t.tryGet(n.Val, key, pos+len(n.Key))
		if err == nil && didResolve {
			n = n.copy()
			n.Val = newnode
		}
		return value, n, didResolve, err
	case *fullNode:
		value, newnode, didResolve, err = t.tryGet(n.Children[key[pos]], key, pos+1)
		if err == nil && didResolve {
			n = n.copy()
			n.Children[key[pos]] = newnode
		}
		return value, n, didResolve, err
	case hashNode:
		child, err := t.resolveHash(n, key[:pos])
		if err != nil {
			return nil, n, true, err
		}
		value, newnode, _, err := t.tryGet(child, key, pos)
		return value, newnode, true, err
	case *rootNode:
		return nil, n, false, nil // TODO(asyukii): temporary fix
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

func (t *Trie) tryGetWithEpoch(origNode node, key []byte, pos int, epoch types.StateEpoch, updateEpoch bool) (value []byte, newnode node, didResolve bool, err error) {
	if t.epochExpired(origNode, epoch) {
		return nil, nil, false, NewExpiredNodeError(key[:pos], epoch)
	}

	switch n := (origNode).(type) {
	case nil:
		return nil, nil, false, nil
	case valueNode:
		return n, n, false, nil
	case *shortNode:
		if len(key)-pos < len(n.Key) || !bytes.Equal(n.Key, key[pos:pos+len(n.Key)]) {
			// key not found in trie
			return nil, n, false, nil
		}

		value, newnode, didResolve, err = t.tryGetWithEpoch(n.Val, key, pos+len(n.Key), t.currentEpoch, updateEpoch)
		if err == nil && t.renewNode(epoch, didResolve, updateEpoch) {
			n = n.copy()
			n.Val = newnode
			if updateEpoch {
				n.setEpoch(t.currentEpoch)
			}
			didResolve = true
		}
		return value, n, didResolve, err
	case *fullNode:
		value, newnode, didResolve, err = t.tryGetWithEpoch(n.Children[key[pos]], key, pos+1, n.GetChildEpoch(int(key[pos])), updateEpoch)
		if err == nil && t.renewNode(epoch, didResolve, updateEpoch) {
			n = n.copy()
			n.Children[key[pos]] = newnode
			if updateEpoch {
				n.setEpoch(t.currentEpoch)
			}
			if updateEpoch && newnode != nil {
				n.UpdateChildEpoch(int(key[pos]), t.currentEpoch)
			}
			didResolve = true
		}

		return value, n, didResolve, err
	case hashNode:
		child, err := t.resolveHash(n, key[:pos])
		if err != nil {
			return nil, n, true, err
		}
		if err = t.resolveShadowNode(epoch, child, key[:pos]); err != nil {
			return nil, nil, false, err
		}

		value, newnode, _, err = t.tryGetWithEpoch(child, key, pos, epoch, updateEpoch)
		return value, newnode, true, err
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

// TryGetNode attempts to retrieve a trie node by compact-encoded path. It is not
// possible to use keybyte-encoding as the path might contain odd nibbles.
func (t *Trie) TryGetNode(path []byte) ([]byte, int, error) {
	item, newroot, resolved, err := t.tryGetNode(t.root, compactToHex(path), 0)
	if err != nil {
		return nil, resolved, err
	}
	if resolved > 0 {
		t.root = newroot
	}
	if item == nil {
		return nil, resolved, nil
	}
	return item, resolved, err
}

func (t *Trie) tryGetNode(origNode node, path []byte, pos int) (item []byte, newnode node, resolved int, err error) {
	// If non-existent path requested, abort
	if origNode == nil {
		return nil, nil, 0, nil
	}
	// If we reached the requested path, return the current node
	if pos >= len(path) {
		// Although we most probably have the original node expanded, encoding
		// that into consensus form can be nasty (needs to cascade down) and
		// time consuming. Instead, just pull the hash up from disk directly.
		var hash hashNode
		if node, ok := origNode.(hashNode); ok {
			hash = node
		} else {
			hash, _ = origNode.cache()
		}
		if hash == nil {
			return nil, origNode, 0, errors.New("non-consensus node")
		}
		blob, err := t.db.Node(common.BytesToHash(hash))
		return blob, origNode, 1, err
	}
	// Path still needs to be traversed, descend into children
	switch n := (origNode).(type) {
	case valueNode:
		// Path prematurely ended, abort
		return nil, nil, 0, nil

	case *shortNode:
		if len(path)-pos < len(n.Key) || !bytes.Equal(n.Key, path[pos:pos+len(n.Key)]) {
			// Path branches off from short node
			return nil, n, 0, nil
		}
		item, newnode, resolved, err = t.tryGetNode(n.Val, path, pos+len(n.Key))
		if err == nil && resolved > 0 {
			n = n.copy()
			n.Val = newnode
		}
		return item, n, resolved, err

	case *fullNode:
		item, newnode, resolved, err = t.tryGetNode(n.Children[path[pos]], path, pos+1)
		if err == nil && resolved > 0 {
			n = n.copy()
			n.Children[path[pos]] = newnode
		}
		return item, n, resolved, err

	case hashNode:
		child, err := t.resolveHash(n, path[:pos])
		if err != nil {
			return nil, n, 1, err
		}
		item, newnode, resolved, err := t.tryGetNode(child, path, pos)
		return item, newnode, resolved + 1, err

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

// Update associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
func (t *Trie) Update(key, value []byte) {
	if err := t.TryUpdate(key, value); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

func (t *Trie) TryUpdateAccount(key []byte, acc *types.StateAccount) error {
	data, err := rlp.EncodeToBytes(acc)
	if err != nil {
		return fmt.Errorf("can't encode object at %x: %w", key[:], err)
	}
	return t.TryUpdate(key, data)
}

// TryUpdate associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
//
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryUpdate(key, value []byte) error {
	t.unhashed++
	k := keybytesToHex(key)
	rootEpoch := t.getRootEpoch()
	if len(value) != 0 {
		_, n, err := t.insert(t.root, nil, k, valueNode(value), rootEpoch)
		if err != nil {
			return err
		}
		t.root = n
	} else {
		_, n, err := t.delete(t.root, nil, k, rootEpoch)
		if err != nil {
			return err
		}
		t.root = n
	}
	return nil
}

func (t *Trie) insert(n node, prefix, key []byte, value node, epoch types.StateEpoch) (bool, node, error) {
	if t.epochExpired(n, epoch) {
		return false, nil, NewExpiredNodeError(prefix, epoch)
	}

	if len(key) == 0 {
		if v, ok := n.(valueNode); ok {
			return !bytes.Equal(v, value.(valueNode)), value, nil
		}
		return true, value, nil
	}
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key, n.Key)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		if matchlen == len(n.Key) {
			dirty, nn, err := t.insert(n.Val, append(prefix, key[:matchlen]...), key[matchlen:], value, n.epoch)
			if !t.renewNode(epoch, dirty, true) || err != nil {
				return false, n, err
			}
			return true, &shortNode{Key: n.Key, Val: nn, flags: t.newFlag(), epoch: t.currentEpoch}, nil
		}
		// Otherwise branch out at the index where they differ.
		branch := &fullNode{flags: t.newFlag()}
		var err error
		_, branch.Children[n.Key[matchlen]], err = t.insert(nil, append(prefix, n.Key[:matchlen+1]...), n.Key[matchlen+1:], n.Val, t.currentEpoch)
		if err != nil {
			return false, nil, err
		}
		_, branch.Children[key[matchlen]], err = t.insert(nil, append(prefix, key[:matchlen+1]...), key[matchlen+1:], value, t.currentEpoch)
		if err != nil {
			return false, nil, err
		}
		if t.withShadowNodes {
			branch.setEpoch(t.currentEpoch)
			branch.UpdateChildEpoch(int(n.Key[matchlen]), t.currentEpoch)
			branch.UpdateChildEpoch(int(key[matchlen]), t.currentEpoch)
		}
		// Replace this shortNode with the branch if it occurs at index 0.
		if matchlen == 0 {
			return true, branch, nil
		}
		// Otherwise, replace it with a short node leading up to the branch.
		return true, &shortNode{Key: key[:matchlen], Val: branch, flags: t.newFlag(), epoch: t.currentEpoch}, nil

	case *fullNode:
		dirty, nn, err := t.insert(n.Children[key[0]], append(prefix, key[0]), key[1:], value, n.GetChildEpoch(int(key[0])))
		if !t.renewNode(epoch, dirty, true) || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[0]] = nn
		if t.withShadowNodes {
			n.setEpoch(t.currentEpoch)
			n.UpdateChildEpoch(int(key[0]), t.currentEpoch)
		}
		return true, n, nil

	case nil:
		return true, &shortNode{Key: key, Val: value, flags: t.newFlag(), epoch: t.currentEpoch}, nil

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and insert into it. This leaves all child nodes on
		// the path to the value in the trie.
		rn, err := t.resolveHash(n, prefix)
		if err != nil {
			return false, nil, err
		}
		if err = t.resolveShadowNode(epoch, rn, prefix); err != nil {
			return false, nil, err
		}

		dirty, nn, err := t.insert(rn, prefix, key, value, epoch)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// Delete removes any existing value for key from the trie.
func (t *Trie) Delete(key []byte) {
	if err := t.TryDelete(key); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryDelete removes any existing value for key from the trie.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryDelete(key []byte) error {
	t.unhashed++
	k := keybytesToHex(key)
	_, n, err := t.delete(t.root, nil, k, t.getRootEpoch())
	if err != nil {
		return err
	}
	t.root = n
	return nil
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
func (t *Trie) delete(n node, prefix, key []byte, epoch types.StateEpoch) (bool, node, error) {
	if t.epochExpired(n, epoch) {
		return false, nil, NewExpiredNodeError(prefix, epoch)
	}

	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key, n.Key)
		if matchlen < len(n.Key) {
			return false, n, nil // don't replace n on mismatch
		}
		if matchlen == len(key) {
			return true, nil, nil // remove n entirely for whole matches
		}
		// The key is longer than n.Key. Remove the remaining suffix
		// from the subtrie. Child can never be nil here since the
		// subtrie must contain at least two other values with keys
		// longer than n.Key.
		dirty, child, err := t.delete(n.Val, append(prefix, key[:len(n.Key)]...), key[len(n.Key):], n.epoch)
		if !t.renewNode(epoch, dirty, true) || err != nil {
			return false, n, err
		}
		switch child := child.(type) {
		case *shortNode:
			// Deleting from the subtrie reduced it to another
			// short node. Merge the nodes to avoid creating a
			// shortNode{..., shortNode{...}}. Use concat (which
			// always creates a new slice) instead of append to
			// avoid modifying n.Key since it might be shared with
			// other nodes.
			return true, &shortNode{Key: concat(n.Key, child.Key...), Val: child.Val, flags: t.newFlag(), epoch: t.currentEpoch}, nil
		default:
			return true, &shortNode{Key: n.Key, Val: child, flags: t.newFlag(), epoch: t.currentEpoch}, nil
		}

	case *fullNode:
		dirty, nn, err := t.delete(n.Children[key[0]], append(prefix, key[0]), key[1:], n.GetChildEpoch(int(key[0])))
		if !t.renewNode(epoch, dirty, true) || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[0]] = nn
		if t.withShadowNodes {
			n.setEpoch(t.currentEpoch)
		}
		if t.withShadowNodes && nn != nil {
			n.UpdateChildEpoch(int(key[0]), t.currentEpoch)
		}

		// Because n is a full node, it must've contained at least two children
		// before the delete operation. If the new child value is non-nil, n still
		// has at least two children after the deletion, and cannot be reduced to
		// a short node.
		if nn != nil {
			return true, n, nil
		}
		// Reduction:
		// Check how many non-nil entries are left after deleting and
		// reduce the full node to a short node if only one entry is
		// left. Since n must've contained at least two children
		// before deletion (otherwise it would not be a full node) n
		// can never be reduced to nil.
		//
		// When the loop is done, pos contains the index of the single
		// value that is left in n or -2 if n contains at least two
		// values.
		pos := -1
		for i, cld := range &n.Children {
			if cld != nil {
				if pos == -1 {
					pos = i
				} else {
					pos = -2
					break
				}
			}
		}
		if pos >= 0 {
			if pos != 16 {
				// If the remaining entry is a short node, it replaces
				// n and its key gets the missing nibble tacked to the
				// front. This avoids creating an invalid
				// shortNode{..., shortNode{...}}.  Since the entry
				// might not be loaded yet, resolve it just for this
				// check.
				cnode, err := t.resolve(n.Children[pos], prefix)
				if err != nil {
					return false, nil, err
				}
				if cnode, ok := cnode.(*shortNode); ok {
					k := append([]byte{byte(pos)}, cnode.Key...)
					return true, &shortNode{Key: k, Val: cnode.Val, flags: t.newFlag(), epoch: t.currentEpoch}, nil
				}
			}
			// Otherwise, n is replaced by a one-nibble short node
			// containing the child.
			return true, &shortNode{Key: []byte{byte(pos)}, Val: n.Children[pos], flags: t.newFlag(), epoch: t.currentEpoch}, nil
		}
		// n still contains at least two values and cannot be reduced.
		return true, n, nil

	case valueNode:
		return true, nil, nil

	case nil:
		return false, nil, nil

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and delete from it. This leaves all child nodes on
		// the path to the value in the trie.
		rn, err := t.resolveHash(n, prefix)
		if err != nil {
			return false, nil, err
		}
		if err = t.resolveShadowNode(epoch, rn, prefix); err != nil {
			return false, nil, err
		}

		dirty, nn, err := t.delete(rn, prefix, key, epoch)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		panic(fmt.Sprintf("%T: invalid node: %v (%v)", n, n, key))
	}
}

// ExpireByPrefix is used to simulate the expiration of a trie by prefix key.
// It is not used in the actual trie implementation. ExpireByPrefix makes sure
// only a child node of a full node is expired, if not an error is returned.
func (t *Trie) ExpireByPrefix(prefixKeyHex []byte) error {
	hn, _, err := t.expireByPrefix(t.root, prefixKeyHex)
	if prefixKeyHex == nil && hn != nil {
		t.root = hn
	}
	if err != nil {
		return err
	}
	return nil
}

func (t *Trie) expireByPrefix(n node, prefixKeyHex []byte) (node, bool, error) {
	// Loop through prefix key
	// When prefix key is empty, generate the hash node of the current node
	// Replace current node with the hash node

	// If length of prefix key is empty
	if len(prefixKeyHex) == 0 {
		hasher := newHasher(false)
		defer returnHasherToPool(hasher)
		var hn node
		_, hn = hasher.proofHash(n)
		if _, ok := hn.(hashNode); ok {
			return hn, false, nil
		}

		return nil, true, nil
	}

	switch n := n.(type) {
	case *shortNode:
		matchLen := prefixLen(prefixKeyHex, n.Key)
		hn, didUpdateEpoch, err := t.expireByPrefix(n.Val, prefixKeyHex[matchLen:])
		if err != nil {
			return nil, didUpdateEpoch, err
		}

		if hn != nil {
			return nil, didUpdateEpoch, fmt.Errorf("can only expire child short node")
		}

		return nil, didUpdateEpoch, err
	case *fullNode:
		childIndex := int(prefixKeyHex[0])
		hn, didUpdateEpoch, err := t.expireByPrefix(n.Children[childIndex], prefixKeyHex[1:])
		if err != nil {
			return nil, didUpdateEpoch, err
		}

		// Replace child node with hash node
		if hn != nil {
			n.Children[prefixKeyHex[0]] = hn
		}

		// Update the epoch so that it is expired
		if !didUpdateEpoch {
			n.UpdateChildEpoch(childIndex, 0)
			didUpdateEpoch = true
		}

		return nil, didUpdateEpoch, err
	default:
		return nil, false, fmt.Errorf("invalid node type")
	}
}

func concat(s1 []byte, s2 ...byte) []byte {
	r := make([]byte, len(s1)+len(s2))
	copy(r, s1)
	copy(r[len(s1):], s2)
	return r
}

func (t *Trie) resolve(n node, prefix []byte) (node, error) {
	if n, ok := n.(hashNode); ok {
		return t.resolveHash(n, prefix)
	}
	return n, nil
}

func (t *Trie) resolveHash(n hashNode, prefix []byte) (node, error) {
	hash := common.BytesToHash(n)
	if t.db == nil {
		return nil, fmt.Errorf("empty trie database")
	}
	if node := t.db.node(hash); node != nil {
		return node, nil
	}
	return nil, &MissingNodeError{NodeHash: hash, Path: prefix}
}

// Hash returns the root hash of the trie. It does not write to the
// database and can be used even if the trie doesn't have one.
func (t *Trie) Hash() common.Hash {
	hash, cached, _ := t.hashRoot()
	t.root = cached
	newRootHash := common.BytesToHash(hash.(hashNode))
	t.trieRoot = newRootHash
	if t.withShadowNodes {
		newShadowTreeRoot := emptyRoot
		shadowTreeRoot, err := t.ShadowHash()
		if err != nil {
			panic(fmt.Sprintf("trie hash err, when ShadowHash, err %v", err))
		}
		// replace shadowTreeRoot for rootNode
		if shadowTreeRoot != nil {
			newShadowTreeRoot = *shadowTreeRoot
		}
		rn := newRootNode(t.getRootEpoch(), newRootHash, newShadowTreeRoot)
		return rn.cachedHash
	}
	return newRootHash
}

// Commit writes all nodes to the trie's memory database, tracking the internal
// and external (for account tries) references.
func (t *Trie) Commit(onleaf LeafCallback) (common.Hash, int, error) {
	if t.db == nil {
		panic("commit called on trie with nil database")
	}
	if t.root == nil {
		return emptyRoot, 0, nil
	}
	// Derive the hash for all dirty nodes first. We hold the assumption
	// in the following procedure that all nodes are hashed.
	hash, cached, _ := t.hashRoot()
	t.root = cached
	newRootHash := common.BytesToHash(hash.(hashNode))
	newShadowTreeRoot := emptyRoot
	if t.withShadowNodes {
		shadowTreeRoot, err := t.ShadowHash()
		if err != nil {
			return common.Hash{}, 0, err
		}
		// replace shadowTreeRoot for rootNode
		if shadowTreeRoot != nil {
			newShadowTreeRoot = *shadowTreeRoot
		}
		// commit shadow nodes after ShadowHash in Commit
		if err = t.commitShadowNodes(t.root, nil, t.getRootEpoch()); err != nil {
			return common.Hash{}, 0, err
		}
	}

	h := newCommitter()
	defer returnCommitterToPool(h)

	// Do a quick check if we really need to commit, before we spin
	// up goroutines. This can happen e.g. if we load a trie for reading storage
	// values, but don't write to it.
	if _, dirty := t.root.cache(); !dirty {
		if t.withShadowNodes {
			rootNodeHash, err := t.storeRootNodeTrieDb(h, newRootHash, newShadowTreeRoot)
			if err != nil {
				return common.Hash{}, 0, err
			}
			return rootNodeHash, 0, nil
		}
		return newRootHash, 0, nil
	}
	var wg sync.WaitGroup
	if onleaf != nil {
		h.onleaf = onleaf
		h.leafCh = make(chan *leaf, leafChanSize)
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.commitLoop(t.db)
		}()
	}
	newRoot, committed, err := h.Commit(t.root, t.db)
	if onleaf != nil {
		// The leafch is created in newCommitter if there was an onleaf callback
		// provided. The commitLoop only _reads_ from it, and the commit
		// operation was the sole writer. Therefore, it's safe to close this
		// channel here.
		close(h.leafCh)
		wg.Wait()
	}
	if err != nil {
		return common.Hash{}, 0, err
	}
	if t.withShadowNodes {
		rootNodeHash, err := t.storeRootNodeTrieDb(h, newRootHash, newShadowTreeRoot)
		if err != nil {
			return common.Hash{}, 0, err
		}
		// update root & root node
		t.rootEpoch = t.getRootEpoch()
		t.shadowTreeRoot = newShadowTreeRoot
		t.trieRoot = newRootHash
		t.root = newRoot
		log.Debug("withShadowNodes trie commit", "rootNodeHash", rootNodeHash, "newRootHash", newRootHash, "newShadowTreeRoot", newShadowTreeRoot)
		return rootNodeHash, committed, nil
	}
	t.root = newRoot
	return newRootHash, committed, nil
}

// hashRoot calculates the root hash of the given trie
func (t *Trie) hashRoot() (node, node, error) {
	if t.root == nil {
		return hashNode(emptyRoot.Bytes()), nil, nil
	}
	// If the number of changes is below 100, we let one thread handle it
	h := newHasher(t.unhashed >= 100)
	defer returnHasherToPool(h)
	newRootHash, cached := h.hash(t.root, true)
	t.unhashed = 0
	return newRootHash, cached, nil
}

// Reset drops the referenced root node and cleans all internal state.
func (t *Trie) Reset() {
	t.root = nil
	t.unhashed = 0
}

func (t *Trie) Size() int {
	return estimateSize(t.root)
}

// ReviveTrie attempts to revive a trie from a list of MPTProofNubs.
// ReviveTrie performs full or partial revive and returns a list of successful
// nubs. ReviveTrie does not guarantee that a value will be revived completely,
// if the proof is not fully valid.
func (t *Trie) ReviveTrie(proof []*MPTProofNub) (successNubs []*MPTProofNub) {
	successNubs, err := t.TryRevive(proof)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to revive trie: %v", err))
	}
	return successNubs
}

func (t *Trie) TryRevive(proof []*MPTProofNub) (successNubs []*MPTProofNub, err error) {

	// Revive trie with each proof nub
	for _, nub := range proof {
		path := []byte{}
		rootExpired := types.EpochExpired(t.getRootEpoch(), t.currentEpoch)
		newNode, didRevive, err := t.tryRevive(t.root, nub.n1PrefixKey, *nub, t.currentEpoch, path, rootExpired)
		if err != nil {
			log.Error("tryRevive err", "prefix", nub.n1PrefixKey, "didRevive", didRevive, "err", err)
		}
		if didRevive && err == nil {
			successNubs = append(successNubs, nub)
			t.root = newNode
		}
	}

	// If no nubs were successful, return error
	if len(successNubs) == 0 && len(proof) != 0 {
		return successNubs, fmt.Errorf("all nubs failed to revive trie")
	}

	return successNubs, nil
}

func (t *Trie) tryRevive(n node, key []byte, nub MPTProofNub, epoch types.StateEpoch, path []byte, isExpired bool) (node, bool, error) {

	// To revive a node, few conditions must be met:
	// 1. key length must be 0, indicating that the targeted node is reached
	// 2. the node must be expired
	// 3. the node must be a hash node
	// 4. the node hash must match the hash value of nub
	if len(key) == 0 {
		if !isExpired {
			return nil, false, fmt.Errorf("key %v not found", key)
		}

		hn, ok := n.(hashNode)
		if !ok {
			return nil, false, fmt.Errorf("node is not a hash node")
		}

		cachedHash, _ := nub.n1.cache()
		if !bytes.Equal(cachedHash, hn) {
			return nil, false, fmt.Errorf("hash values does not match")
		}

		tryUpdateNodeEpoch(nub.n1, t.currentEpoch)
		if nub.n2 != nil {
			tryUpdateNodeEpoch(nub.n2, t.currentEpoch)
			if n1, ok := nub.n1.(*shortNode); ok { // n2 can only be followed by a short node
				n1.Val = nub.n2
			} else {
				return nil, false, fmt.Errorf("invalid node type")
			}
		}
		return nub.n1, true, nil
	}

	if isExpired { // the node is expired but targeted node is not reached
		return nil, false, NewExpiredNodeError(path, 0) // Set default value, will change later
	}

	switch n := n.(type) {
	case *shortNode:
		if len(key) < len(n.Key) || !bytes.Equal(key[:len(n.Key)], n.Key) {
			return nil, false, fmt.Errorf("key %v not found", key)
		}
		newNode, didRevive, err := t.tryRevive(n.Val, key[len(n.Key):], nub, epoch, append(path, key[:len(n.Key)]...), isExpired)
		if didRevive && err == nil {
			n = n.copy()
			n.Val = newNode
			n.setEpoch(t.currentEpoch)
		}
		return n, didRevive, err
	case *fullNode:
		childIndex := int(key[0])
		isExpired, _ := n.ChildExpired(nil, childIndex, t.currentEpoch) // TODO (asyukii): t.currentEpoch or t.root.getEpoch()?
		newNode, didRevive, err := t.tryRevive(n.Children[childIndex], key[1:], nub, epoch, append(path, key[0]), isExpired)
		if didRevive && err == nil {
			n = n.copy()
			n.Children[childIndex] = newNode
			n.setEpoch(t.currentEpoch)
			n.UpdateChildEpoch(childIndex, t.currentEpoch)
		}

		if e, ok := err.(*ExpiredNodeError); ok {
			e.Epoch = n.GetChildEpoch(childIndex)
			return n, didRevive, e
		}

		return n, didRevive, err
	case hashNode:
		tn, err := t.resolveHash(n, path) // TODO(asyukii): may need to copy resolved hash node
		if err != nil {
			return nil, false, err
		}
		if err = t.resolveShadowNode(epoch, tn, path); err != nil {
			return nil, false, err
		}
		return t.tryRevive(tn, key, nub, epoch, path, isExpired)
	case valueNode:
		return nil, false, nil
	case nil:
		return nil, false, nil
	default:
		panic(fmt.Sprintf("invalid node: %T", n))
	}
}

func (t *Trie) resolveShadowNode(epoch types.StateEpoch, origin node, prefix []byte) error {
	if !t.withShadowNodes {
		return nil
	}

	if t.sndb == nil {
		return errors.New("cannot resolve shadow node")
	}

	switch n := origin.(type) {
	case *shortNode:
		n.setEpoch(epoch)
		n.shadowNode.ShadowHash = nil
		return t.resolveShadowNode(epoch, n.Val, safeAppendBytes(prefix, n.Key...))
	case *fullNode:
		n.setEpoch(epoch)
		val, err := t.sndb.Get(string(hexToSuffixCompact(prefix)))
		if err != nil {
			return err
		}
		if len(val) == 0 {
			// set default epoch map
			n.shadowNode.EpochMap = [16]types.StateEpoch{}
			n.shadowNode.ShadowHash = nil
		} else {
			tmp, decErr := decodeShadowBranchNode(val)
			if decErr != nil {
				return decErr
			}
			n.shadowNode = *tmp
		}
		for i := byte(0); i < BranchNodeLength-1; i++ {
			if err := t.resolveShadowNode(n.shadowNode.EpochMap[i], n.Children[i], safeAppendBytes(prefix, i)); err != nil {
				return err
			}
		}
		return nil
	case valueNode, hashNode, nil:
		// just skip
		return nil
	default:
		return errors.New("resolveShadowNode unsupported node type")
	}
}

func (t *Trie) ShadowHash() (*common.Hash, error) {
	if t.root == nil {
		return nil, nil
	}
	h := newHasher(true)
	defer returnHasherToPool(h)
	sh, _, err := t.shadowHash(t.root, h, nil, t.getRootEpoch())
	return sh, err
}

// shadowHash calculate node's shadow node hash, recalculate needn't a copy
// epoch param only use in resolve hashNode
func (t *Trie) shadowHash(origin node, h *hasher, prefix []byte, epoch types.StateEpoch) (*common.Hash, types.StateEpoch, error) {
	switch n := origin.(type) {
	case *shortNode:
		var err error
		if n.shadowNode.ShadowHash, _, err = t.shadowHash(n.Val, h, append(prefix, n.Key...), n.epoch); err != nil {
			return nil, types.StateEpoch0, err
		}
		return n.shadowNode.ShadowHash, n.epoch, nil
	case *fullNode:
		epochSelf := n.epoch
		epochMap := n.shadowNode.EpochMap
		hashList := make([]*common.Hash, 0, BranchNodeLength-1)
		for i := byte(0); i < BranchNodeLength-1; i++ {
			child := n.Children[i]
			if child == nil {
				continue
			}
			// skip expired node.
			if epochSelf >= epochMap[i]+2 {
				n.shadowNode.EpochMap[i] = 0
				continue
			}

			subHash, subEpoch, err := t.shadowHash(child, h, append(prefix, i), epochMap[i])
			if err != nil {
				return nil, types.StateEpoch0, err
			}
			if subHash != nil {
				hashList = append(hashList, subHash)
			}
			n.shadowNode.EpochMap[i] = subEpoch
		}
		n.shadowNode.ShadowHash = h.shadowNodeHashListToHash(hashList)
		return h.shadowBranchNodeToHash(&n.shadowNode), n.epoch, nil
	case valueNode:
		return nil, epoch, nil
	case hashNode:
		if t.db == nil || t.sndb == nil {
			return nil, types.StateEpoch0, errors.New("ShadowHash db or sndb is nil")
		}
		// resolve temporary, not add to trie
		rn, err := t.resolveHash(n, prefix)
		if err != nil {
			return nil, types.StateEpoch0, err
		}
		if err = t.resolveShadowNode(epoch, rn, prefix); err != nil {
			return nil, types.StateEpoch0, err
		}
		return t.shadowHash(rn, h, prefix, epoch)
	default:
		return nil, types.StateEpoch0, errors.New("cannot get shortNode's child shadow node")
	}
}

// commitShadowNodes commit node's shadow node, must call after shadowHash
func (t *Trie) commitShadowNodes(origin node, prefix []byte, epoch types.StateEpoch) error {
	if t.sndb == nil {
		return errors.New("ShadowHash sndb is nil")
	}
	switch n := origin.(type) {
	case *shortNode:
		if err := t.commitShadowNodes(n.Val, append(prefix, n.Key...), epoch); err != nil {
			return err
		}
		return nil
	case *fullNode:
		epochSelf := n.epoch
		epochMap := n.shadowNode.EpochMap
		for i := byte(0); i < BranchNodeLength-1; i++ {
			child := n.Children[i]
			if child == nil {
				continue
			}
			// skip expired node.
			if epochSelf >= epochMap[i]+2 {
				continue
			}

			if err := t.commitShadowNodes(child, append(prefix, i), epochMap[i]); err != nil {
				return err
			}
		}

		encBuf := rlp.NewEncoderBuffer(nil)
		n.shadowNode.encode(encBuf)
		if err := t.sndb.Put(string(hexToSuffixCompact(prefix)), encBuf.ToBytes()); err != nil {
			return err
		}
		return nil
	case valueNode, hashNode:
		return nil
	default:
		return errors.New("cannot get shortNode's child shadow node")
	}
}

func (t *Trie) storeRootNodeTrieDb(c *committer, newRootHash, newShadowTreeRoot common.Hash) (common.Hash, error) {
	rn := newRootNode(t.getRootEpoch(), newRootHash, newShadowTreeRoot)
	hn, _, err := c.Commit(rn, t.db)
	return common.BytesToHash(hn), err
}

func (t *Trie) storeRootNode(newRootHash, newShadowTreeRoot common.Hash) (common.Hash, error) {
	rn := newRootNode(t.getRootEpoch(), newRootHash, newShadowTreeRoot)
	if err := t.sndb.Put(ShadowTreeRootNodePath, rn.cachedEnc); err != nil {
		return common.Hash{}, err
	}
	return rn.cachedHash, nil
}

// getRootEpoch parse root and resolve its epoch, when root is hashNode,
// the epoch should be t.rootEpoch, not default types.StateEpoch0
func (t *Trie) getRootEpoch() types.StateEpoch {
	ret := t.rootEpoch
	switch n := t.root.(type) {
	case *shortNode:
		ret = n.getEpoch()
	case *fullNode:
		ret = n.getEpoch()
	}

	return ret
}

func (t *Trie) epochExpired(n node, epoch types.StateEpoch) bool {
	// when node is nil, skip epoch check
	if !t.withShadowNodes || n == nil {
		return false
	}
	return types.EpochExpired(epoch, t.currentEpoch)
}

// renewNode check if renew node, according to trie node epoch and childDirty,
// childDirty or updateEpoch need copy for prevent reuse trie cache
func (t *Trie) renewNode(epoch types.StateEpoch, childDirty bool, updateEpoch bool) bool {
	// when !updateEpoch, it same as !t.withShadowNodes
	if !t.withShadowNodes || !updateEpoch {
		return childDirty
	}

	// when no epoch update, same as before
	if epoch == t.currentEpoch {
		return childDirty
	}

	// node need update epoch, just renew
	return true
}

func resolveRootNodeTrieDb(db *Database, root common.Hash) (*rootNode, error) {
	expectHash := common.BytesToHash(root[:])
	rn := db.node(root)
	n, ok := rn.(*rootNode)
	if !ok {
		return newEpoch0RootNode(root), nil
	}

	if n.cachedHash != expectHash {
		return nil, errors.New("found the wrong rootNode")
	}
	return n, nil
}

func resolveRootNode(sndb ShadowNodeStorage, root common.Hash) (*rootNode, error) {
	expectHash := common.BytesToHash(root[:])
	val, err := sndb.Get(ShadowTreeRootNodePath)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return newEpoch0RootNode(root), nil
	}
	n, err := DecodeRootNode(val)
	if err != nil {
		return nil, err
	}

	if n.cachedHash != expectHash {
		return nil, errors.New("found the wrong rootNode")
	}
	return n, nil
}

// SUFFIX-COMPACT encoding is used for encoding trie node path in the trie node
// storage key. The main difference with COMPACT encoding is that the key flag
// is put at the end of the key.
//
// e.g.
// - the key [] is encoded as [0x00]
// - the key [0x1, 0x2, 0x3] is encoded as [0x12, 0x31]
// - the key [0x1, 0x2, 0x3, 0x0] is encoded as [0x12, 0x30, 0x00]
//
// The main benefit of this format is the continuous paths can retain the shared
// path prefix after encoding.
func hexToSuffixCompact(hex []byte) []byte {
	terminator := byte(0)
	if hasTerm(hex) {
		terminator = 1
		hex = hex[:len(hex)-1]
	}
	buf := make([]byte, len(hex)/2+1)
	buf[len(buf)-1] = terminator << 1 // the flag byte
	if len(hex)&1 == 1 {
		buf[len(buf)-1] |= 1                    // odd flag
		buf[len(buf)-1] |= hex[len(hex)-1] << 4 // last nibble is contained in the last byte
		hex = hex[:len(hex)-1]
	}
	decodeNibbles(hex, buf[:len(buf)-1])
	return buf
}

func tryUpdateNodeEpoch(origin node, epoch types.StateEpoch) {
	switch n := origin.(type) {
	case *shortNode:
		n.setEpoch(epoch)
	case *fullNode:
		n.setEpoch(epoch)
	}
}

func safeAppendBytes(src []byte, added ...byte) []byte {
	ret := make([]byte, len(src)+len(added))
	copy(ret, src)
	copy(ret[len(src):], added)
	return ret
}
