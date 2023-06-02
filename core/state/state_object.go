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

package state

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum/go-ethereum/core/state/snapshot"

	"github.com/ethereum/go-ethereum/trie"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
)

var emptyCodeHash = crypto.Keccak256(nil)

type Code []byte

func (c Code) String() string {
	return string(c) //strings.Join(Disassemble(c), " ")
}

type Storage map[common.Hash]common.Hash

func (s Storage) String() (str string) {
	for key, value := range s {
		str += fmt.Sprintf("%X : %X\n", key, value)
	}

	return
}

func (s Storage) Copy() Storage {
	cpy := make(Storage, len(s))
	for key, value := range s {
		cpy[key] = value
	}

	return cpy
}

// StateObject represents an Ethereum account which is being modified.
//
// The usage pattern is as follows:
// First you need to obtain a state object.
// Account values can be accessed and modified through the object.
// Finally, call CommitTrie to write the modified storage trie into a database.
type StateObject struct {
	address       common.Address
	addrHash      common.Hash // hash of ethereum address of the account
	data          types.StateAccount
	db            *StateDB
	rootCorrected bool // To indicate whether the root has been corrected in pipecommit mode

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be returned
	// by StateDB.Commit.
	dbErr error

	// Write caches.
	trie Trie // storage trie, which becomes non-nil on first access, it's committed trie
	code Code // contract bytecode, which gets set when code is loaded

	// TODO(0xbundler) Attention: it's a shared storage between stateDBs, will expired, just disable sharedOriginStorage now
	sharedOriginStorage *sync.Map // Point to the entry of the stateObject in sharedPool
	originStorage       Storage   // Storage cache of original entries to dedup rewrites, reset for every transaction

	pendingStorage Storage // Storage entries that need to be flushed to disk, at the end of an entire block
	dirtyStorage   Storage // Storage entries that have been modified in the current transaction execution
	fakeStorage    Storage // Fake storage which constructed by caller for debugging purpose.

	// revive state
	pendingReviveTrie Trie // pendingReviveTrie it contains pending revive trie nodes, could update & commit later
	dirtyReviveTrie   Trie // dirtyReviveTrie for tx

	// when R&W, access revive state first
	pendingReviveState map[string]common.Hash // pendingReviveState for block, it cannot flush to trie, just cache
	dirtyReviveState   map[string]common.Hash // dirtyReviveState for tx, for cache dirtyReviveTrie

	// accessed state, don't record revive state, don't record nonexist state or any err access
	pendingAccessedState map[common.Hash]int // pendingAccessedState record which state is accessed, it will update epoch index late
	dirtyAccessedState   map[common.Hash]int // dirtyAccessedState record which state is accessed, it will update epoch index later

	// Cache flags.
	// When an object is marked suicided it will be delete from the trie
	// during the "update" phase of the state transition.
	dirtyCode bool // true if the code was updated
	suicided  bool
	deleted   bool

	//encode
	encodeData  []byte
	targetEpoch types.StateEpoch
}

// empty returns whether the account is considered empty.
func (s *StateObject) empty() bool {
	return s.data.Nonce == 0 && s.data.Balance.Sign() == 0 && bytes.Equal(s.data.CodeHash, emptyCodeHash)
}

// newObject creates a state object.
func newObject(db *StateDB, address common.Address, data types.StateAccount) *StateObject {
	if data.Balance == nil {
		data.Balance = new(big.Int)
	}
	if data.CodeHash == nil {
		data.CodeHash = emptyCodeHash
	}
	if data.Root == (common.Hash{}) {
		data.Root = emptyRoot
	}
	var storageMap *sync.Map
	// Check whether the storage exist in pool, new originStorage if not exist
	if db != nil && db.storagePool != nil {
		storageMap = db.GetStorage(address)
	}

	return &StateObject{
		db:                   db,
		address:              address,
		addrHash:             crypto.Keccak256Hash(address[:]),
		data:                 data,
		sharedOriginStorage:  storageMap,
		originStorage:        make(Storage),
		pendingStorage:       make(Storage),
		dirtyStorage:         make(Storage),
		dirtyReviveState:     make(map[string]common.Hash),
		pendingReviveState:   make(map[string]common.Hash),
		dirtyAccessedState:   make(map[common.Hash]int),
		pendingAccessedState: make(map[common.Hash]int),
		targetEpoch:          db.targetEpoch,
	}
}

// EncodeRLP implements rlp.Encoder.
func (s *StateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &s.data)
}

// setError remembers the first non-nil error it is called with.
func (s *StateObject) setError(err error) {
	if s.dbErr == nil {
		s.dbErr = err
	}
}

func (s *StateObject) markSuicided() {
	s.suicided = true
}

func (s *StateObject) touch() {
	s.db.journal.append(touchChange{
		account: &s.address,
	})
	if s.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		s.db.journal.dirty(s.address)
	}
}

func (s *StateObject) getTrie(db Database) Trie {
	if s.trie == nil {
		// Try fetching from prefetcher first
		// We don't prefetch empty tries
		prefetcher := s.db.prefetcher
		if s.data.Root != emptyRoot && prefetcher != nil {
			// When the miner is creating the pending state, there is no
			// prefetcher
			s.trie = prefetcher.trie(s.data.Root)
		}
		if s.trie != nil {
			return s.trie
		}
		var err error
		// check if enable state epoch
		if s.db.enableAccStateEpoch(false, s.address) {
			log.Debug("Open StorageTrie with shadow nodes", "addr", s.address, "targetEpoch", s.targetEpoch)
			s.trie, err = db.OpenStorageTrieWithShadowNode(s.addrHash, s.data.Root, s.targetEpoch, s.db.openShadowStorage(s.addrHash))
			if err != nil {
				log.Error("OpenStorageTrieWithShadowNode err", "targetEpoch", s.targetEpoch, "err", err)
				s.trie, _ = db.OpenStorageTrieWithShadowNode(s.addrHash, common.Hash{}, s.targetEpoch, s.db.openShadowStorage(s.addrHash))
				s.setError(fmt.Errorf("can't create storage trie with shadowNode: %v", err))
			}
			return s.trie
		}

		log.Debug("Open StorageTrie normal", "addr", s.address, "targetEpoch", s.targetEpoch, "addr", s.address)
		s.trie, err = db.OpenStorageTrie(s.addrHash, s.data.Root)
		if err != nil {
			s.trie, _ = db.OpenStorageTrie(s.addrHash, common.Hash{})
			s.setError(fmt.Errorf("can't create storage trie: %v", err))
		}
	}
	return s.trie
}

func (s *StateObject) getPendingReviveTrie(db Database) Trie {
	if s.pendingReviveTrie == nil {
		s.pendingReviveTrie = s.db.db.CopyTrie(s.getTrie(db))
	}
	return s.pendingReviveTrie
}

func (s *StateObject) getDirtyReviveTrie(db Database) Trie {
	if s.dirtyReviveTrie == nil {
		s.dirtyReviveTrie = s.db.db.CopyTrie(s.getPendingReviveTrie(db))
	}
	return s.dirtyReviveTrie
}

// GetState retrieves a value from the account storage trie.
func (s *StateObject) GetState(db Database, key common.Hash) (common.Hash, error) {
	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if s.fakeStorage != nil {
		return s.fakeStorage[key], nil
	}
	// If we have a dirty value for this state entry, return it
	if value, dirty := s.dirtyStorage[key]; dirty {
		s.accessState(key)
		return value, nil
	}
	if s.db.enableAccStateEpoch(true, s.address) {
		if revived, revive := s.queryFromReviveState(db, s.dirtyReviveState, key); revive {
			s.accessState(key)
			return revived, nil
		}
	}

	// Otherwise return the entry's original value
	committed, err := s.GetCommittedState(db, key)
	if err == nil && committed != (common.Hash{}) {
		s.accessState(key)
	}
	return committed, err
}

func (s *StateObject) getOriginStorage(key common.Hash) (common.Hash, bool) {
	if value, cached := s.originStorage[key]; cached {
		return value, true
	}
	// if L1 cache miss, try to get it from shared pool
	if s.sharedOriginStorage != nil {
		val, ok := s.sharedOriginStorage.Load(key)
		if !ok {
			return common.Hash{}, false
		}
		storage := val.(common.Hash)
		s.originStorage[key] = storage
		return storage, true
	}
	return common.Hash{}, false
}

func (s *StateObject) setOriginStorage(key common.Hash, value common.Hash) {
	if s.db.writeOnSharedStorage && s.sharedOriginStorage != nil {
		s.sharedOriginStorage.Store(key, value)
	}
	s.originStorage[key] = value
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (s *StateObject) GetCommittedState(db Database, key common.Hash) (common.Hash, error) {
	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if s.fakeStorage != nil {
		return s.fakeStorage[key], nil
	}
	// If we have a pending write or clean cached, return that
	if value, pending := s.pendingStorage[key]; pending {
		return value, nil
	}
	if s.db.enableAccStateEpoch(true, s.address) {
		if revived, revive := s.queryFromReviveState(db, s.pendingReviveState, key); revive {
			return revived, nil
		}
	}

	if value, cached := s.getOriginStorage(key); cached {
		return value, nil
	}
	// If no live objects are available, attempt to use snapshots
	if s.db.snap != nil {
		// If the object was destructed in *this* block (and potentially resurrected),
		// the storage has been cleared out, and we should *not* consult the previous
		// snapshot about any storage values. The only possible alternatives are:
		//   1) resurrect happened, and new slot values were set -- those should
		//      have been handles via pendingStorage above.
		//   2) we don't have new values, and can deliver empty response back
		if _, destructed := s.db.snapDestructs[s.address]; destructed {
			return common.Hash{}, nil
		}
		start := time.Now()
		enc, err := s.db.snap.Storage(s.addrHash, crypto.Keccak256Hash(key.Bytes()))
		if metrics.EnabledExpensive {
			s.db.SnapshotStorageReads += time.Since(start)
		}

		// snapshot val encode is different from trie, so handle independent
		if err == nil {
			var value common.Hash
			if len(enc) > 0 {
				sv, err := snapshot.ParseSnapValFromBytes(enc)
				if err != nil {
					s.setError(err)
				}
				if err == nil && s.db.enableAccStateEpoch(true, s.address) &&
					types.EpochExpired(sv.Epoch, s.targetEpoch) {
					// query from dirty revive trie, got the newest expired info
					_, err = s.getDirtyReviveTrie(db).TryGet(key.Bytes())
					if enErr, ok := err.(*trie.ExpiredNodeError); ok {
						return common.Hash{}, NewExpiredStateError(s.address, key, enErr).Reason("snap query")
					}
					return common.Hash{}, NewSnapExpiredStateError(s.address, key, sv.Epoch)
				}
				value.SetBytes(sv.Val.Bytes())
			}

			s.setOriginStorage(key, value)
			return value, nil
		}
	}

	// If snapshot unavailable or reading from it failed, load from the database
	start := time.Now()
	//if metrics.EnabledExpensive {
	//	meter = &s.db.StorageReads
	//}
	enc, err := s.getTrie(db).TryGet(key.Bytes())
	if metrics.EnabledExpensive {
		s.db.StorageReads += time.Since(start)
	}
	if err != nil {
		if enErr, ok := err.(*trie.ExpiredNodeError); ok {
			// query from dirty revive trie, got the newest expired info
			_, err = s.getDirtyReviveTrie(db).TryGet(key.Bytes())
			if enErr, ok := err.(*trie.ExpiredNodeError); ok {
				return common.Hash{}, NewExpiredStateError(s.address, key, enErr)
			}
			return common.Hash{}, NewExpiredStateError(s.address, key, enErr)
		}
		s.setError(err)
		return common.Hash{}, nil
	}
	var value common.Hash
	if len(enc) > 0 {
		_, content, _, err := rlp.Split(enc)
		if err != nil {
			s.setError(err)
		}
		value.SetBytes(content)
	}
	s.setOriginStorage(key, value)
	return value, nil
}

// SetState updates a value in account storage.
func (s *StateObject) SetState(db Database, key, value common.Hash) error {
	// If the fake storage is set, put the temporary state update here.
	if s.fakeStorage != nil {
		s.fakeStorage[key] = value
		return nil
	}
	// If the new value is the same as old, don't set
	prev, err := s.GetState(db, key)
	if exErr, ok := err.(*ExpiredStateError); ok {
		exErr.Reason("query from insert")
		return exErr
	}
	if err != nil {
		return err
	}
	if prev == value {
		s.accessState(key)
		return nil
	}
	// when state insert, check if valid to insert new state
	if s.db.enableAccStateEpoch(true, s.address) && prev == (common.Hash{}) {
		_, err = s.getDirtyReviveTrie(db).TryGet(key.Bytes())
		if err != nil {
			if enErr, ok := err.(*trie.ExpiredNodeError); ok {
				return NewInsertExpiredStateError(s.address, key, enErr)
			}
			s.setError(err)
			return nil
		}
	}
	// New value is different, update and journal the change
	s.accessState(key)
	s.db.journal.append(storageChange{
		account:  &s.address,
		key:      key,
		prevalue: prev,
	})
	s.setState(key, value)
	return nil
}

// SetStorage replaces the entire state storage with the given one.
//
// After this function is called, all original state will be ignored and state
// lookup only happens in the fake state storage.
//
// Note this function should only be used for debugging purpose.
func (s *StateObject) SetStorage(storage map[common.Hash]common.Hash) {
	// Allocate fake storage if it's nil.
	if s.fakeStorage == nil {
		s.fakeStorage = make(Storage)
	}
	for key, value := range storage {
		s.fakeStorage[key] = value
	}
	// Don't bother journal since this function should only be used for
	// debugging and the `fake` storage won't be committed to database.
}

func (s *StateObject) setState(key, value common.Hash) {
	s.dirtyStorage[key] = value
}

// finalise moves all dirty storage slots into the pending area to be hashed or
// committed later. It is invoked at the end of every transaction.
func (s *StateObject) finalise(prefetch bool) {
	slotsToPrefetch := make([][]byte, 0, len(s.dirtyStorage))
	for key, value := range s.dirtyStorage {
		s.pendingStorage[key] = value
		if value != s.originStorage[key] {
			slotsToPrefetch = append(slotsToPrefetch, common.CopyBytes(key[:])) // Copy needed for closure
		}
	}
	for key, value := range s.dirtyReviveState {
		s.pendingReviveState[key] = value
	}
	for key, value := range s.dirtyAccessedState {
		count := s.pendingAccessedState[key]
		s.pendingAccessedState[key] = count + value
	}

	prefetcher := s.db.prefetcher
	if prefetcher != nil && prefetch && len(slotsToPrefetch) > 0 && s.data.Root != emptyRoot {
		prefetcher.prefetch(s.data.Root, slotsToPrefetch, s.addrHash)
	}
	if len(s.dirtyStorage) > 0 {
		s.dirtyStorage = make(Storage)
	}
	if len(s.dirtyReviveState) > 0 {
		s.dirtyReviveState = make(map[string]common.Hash)
	}
	if len(s.dirtyAccessedState) > 0 {
		s.dirtyAccessedState = make(map[common.Hash]int)
	}
	if s.dirtyReviveTrie != nil {
		s.pendingReviveTrie = s.dirtyReviveTrie
		s.dirtyReviveTrie = nil
	}
}

// updateTrie writes cached storage modifications into the object's storage trie.
// It will return nil if the trie has not been loaded and no changes have been made
func (s *StateObject) updateTrie(db Database) Trie {
	// Make sure all dirty slots are finalized into the pending storage area
	s.finalise(false) // Don't prefetch anymore, pull directly if need be
	if len(s.pendingStorage) == 0 && len(s.pendingReviveState) == 0 {
		return s.trie
	}
	// Track the amount of time wasted on updating the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) {
			s.db.MetricsMux.Lock()
			s.db.StorageUpdates += time.Since(start)
			s.db.MetricsMux.Unlock()
		}(time.Now())
	}
	// Insert all the pending updates into the pending trie
	tr := s.getPendingReviveTrie(db)

	usedStorage := make([][]byte, 0, len(s.pendingStorage))
	dirtyStorage := make(map[common.Hash]common.Hash)
	accessStorage := make(map[common.Hash]struct{})
	for k := range s.pendingAccessedState {
		accessStorage[k] = struct{}{}
	}
	for key, value := range s.pendingStorage {
		// Skip noop changes, persist actual changes
		if value == s.originStorage[key] {
			continue
		}
		s.originStorage[key] = value
		dirtyStorage[key] = value
		delete(accessStorage, key)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for key, value := range dirtyStorage {
			var v []byte
			if value != (common.Hash{}) {
				// Encoding []byte cannot fail, ok to ignore the error.
				v, _ = rlp.EncodeToBytes(common.TrimLeftZeroes(value[:]))
			}
			if len(v) == 0 {
				s.setError(tr.TryDelete(key[:]))
			} else {
				s.setError(tr.TryUpdate(key[:], v))
			}
			usedStorage = append(usedStorage, common.CopyBytes(key[:]))
		}
		// refresh accessed slots' epoch
		for key := range accessStorage {
			s.setError(tr.TryUpdateEpoch(key[:]))
			usedStorage = append(usedStorage, common.CopyBytes(key[:]))
		}
	}()
	if s.db.snap != nil {
		// If state snapshotting is active, cache the data til commit
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.db.snapStorageMux.Lock()
			// The snapshot storage map for the object
			storage := s.db.snapStorage[s.address]
			if storage == nil {
				storage = make(map[string][]byte, len(dirtyStorage))
				s.db.snapStorage[s.address] = storage
			}
			s.db.snapStorageMux.Unlock()
			for key, value := range dirtyStorage {
				enc, err := snapshot.NewSnapValBytes(s.targetEpoch, value)
				if err != nil {
					s.setError(err)
				}
				storage[string(key[:])] = enc
			}
		}()
	}
	wg.Wait()

	prefetcher := s.db.prefetcher
	if prefetcher != nil {
		prefetcher.used(s.data.Root, usedStorage)
	}

	if len(s.pendingStorage) > 0 {
		s.pendingStorage = make(Storage)
	}
	if len(s.pendingReviveState) > 0 {
		s.pendingReviveState = make(map[string]common.Hash)
	}
	if len(s.pendingAccessedState) > 0 {
		s.pendingAccessedState = make(map[common.Hash]int)
	}
	if s.pendingReviveTrie != nil {
		s.pendingReviveTrie = nil
	}

	// reset trie as pending trie, will commit later
	if tr != nil {
		s.trie = s.db.db.CopyTrie(tr)
	}
	return tr
}

// UpdateRoot sets the trie root to the current root hash of
func (s *StateObject) updateRoot(db Database) {
	// If node runs in no trie mode, set root to empty.
	defer func() {
		if db.NoTries() {
			s.data.Root = common.Hash{}
		}
	}()

	// If nothing changed, don't bother with hashing anything
	if s.updateTrie(db) == nil {
		return
	}
	// Track the amount of time wasted on hashing the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) {
			s.db.MetricsMux.Lock()
			s.db.StorageHashes += time.Since(start)
			s.db.MetricsMux.Unlock()
		}(time.Now())
	}
	s.data.Root = s.trie.Hash()
}

// CommitTrie the storage trie of the object to db.
// This updates the trie root.
func (s *StateObject) CommitTrie(db Database) (int, error) {
	// If nothing changed, don't bother with hashing anything
	if s.updateTrie(db) == nil {
		if s.trie != nil && s.data.Root != emptyRoot {
			db.CacheStorage(s.addrHash, s.data.Root, s.trie)
		}
		return 0, nil
	}
	if s.dbErr != nil {
		return 0, s.dbErr
	}
	// Track the amount of time wasted on committing the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageCommits += time.Since(start) }(time.Now())
	}
	root, committed, err := s.trie.Commit(nil)
	if err != nil {
		log.Error("obj CommitTrie", "addr", s.address, "root", root, "err", err)
	}
	log.Debug("obj CommitTrie", "addr", s.address, "root", root, "err", err)
	if err == nil {
		s.data.Root = root
	}
	if s.data.Root != emptyRoot {
		db.CacheStorage(s.addrHash, s.data.Root, s.trie)
	}
	return committed, err
}

// AddBalance adds amount to s's balance.
// It is used to add funds to the destination account of a transfer.
func (s *StateObject) AddBalance(amount *big.Int) {
	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.Sign() == 0 {
		if s.empty() {
			s.touch()
		}
		return
	}
	s.SetBalance(new(big.Int).Add(s.Balance(), amount))
}

// SubBalance removes amount from s's balance.
// It is used to remove funds from the origin account of a transfer.
func (s *StateObject) SubBalance(amount *big.Int) {
	if amount.Sign() == 0 {
		return
	}
	s.SetBalance(new(big.Int).Sub(s.Balance(), amount))
}

func (s *StateObject) SetBalance(amount *big.Int) {
	s.db.journal.append(balanceChange{
		account: &s.address,
		prev:    new(big.Int).Set(s.data.Balance),
	})
	s.setBalance(amount)
}

func (s *StateObject) setBalance(amount *big.Int) {
	s.data.Balance = amount
}

func (s *StateObject) deepCopy(db *StateDB) *StateObject {
	stateObject := newObject(db, s.address, s.data)
	if s.trie != nil {
		stateObject.trie = db.db.CopyTrie(s.trie)
	}
	stateObject.code = s.code
	stateObject.dirtyStorage = s.dirtyStorage.Copy()
	stateObject.originStorage = s.originStorage.Copy()
	stateObject.pendingStorage = s.pendingStorage.Copy()
	stateObject.suicided = s.suicided
	stateObject.dirtyCode = s.dirtyCode
	stateObject.deleted = s.deleted

	if s.dirtyReviveTrie != nil {
		stateObject.dirtyReviveTrie = db.db.CopyTrie(s.dirtyReviveTrie)
	}
	if s.pendingReviveTrie != nil {
		stateObject.pendingReviveTrie = db.db.CopyTrie(s.pendingReviveTrie)
	}
	stateObject.dirtyReviveState = make(map[string]common.Hash, len(s.dirtyReviveState))
	for k, v := range s.dirtyReviveState {
		stateObject.dirtyReviveState[k] = v
	}
	stateObject.pendingReviveState = make(map[string]common.Hash, len(s.pendingReviveState))
	for k, v := range s.pendingReviveState {
		stateObject.pendingReviveState[k] = v
	}
	stateObject.dirtyAccessedState = make(map[common.Hash]int, len(s.dirtyAccessedState))
	for k, v := range s.dirtyAccessedState {
		stateObject.dirtyAccessedState[k] = v
	}
	stateObject.pendingAccessedState = make(map[common.Hash]int, len(s.pendingAccessedState))
	for k, v := range s.pendingAccessedState {
		stateObject.pendingAccessedState[k] = v
	}
	return stateObject
}

//
// Attribute accessors
//

// Returns the address of the contract/account
func (s *StateObject) Address() common.Address {
	return s.address
}

// Code returns the contract code associated with this object, if any.
func (s *StateObject) Code(db Database) []byte {
	if s.code != nil {
		return s.code
	}
	if bytes.Equal(s.CodeHash(), emptyCodeHash) {
		return nil
	}
	code, err := db.ContractCode(s.addrHash, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.setError(fmt.Errorf("can't load code hash %x: %v", s.CodeHash(), err))
	}
	s.code = code
	return code
}

// CodeSize returns the size of the contract code associated with this object,
// or zero if none. This method is an almost mirror of Code, but uses a cache
// inside the database to avoid loading codes seen recently.
func (s *StateObject) CodeSize(db Database) int {
	if s.code != nil {
		return len(s.code)
	}
	if bytes.Equal(s.CodeHash(), emptyCodeHash) {
		return 0
	}
	size, err := db.ContractCodeSize(s.addrHash, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.setError(fmt.Errorf("can't load code size %x: %v", s.CodeHash(), err))
	}
	return size
}

func (s *StateObject) SetCode(codeHash common.Hash, code []byte) {
	prevcode := s.Code(s.db.db)
	s.db.journal.append(codeChange{
		account:  &s.address,
		prevhash: s.CodeHash(),
		prevcode: prevcode,
	})
	s.setCode(codeHash, code)
}

func (s *StateObject) setCode(codeHash common.Hash, code []byte) {
	s.code = code
	s.data.CodeHash = codeHash[:]
	s.dirtyCode = true
}

func (s *StateObject) SetNonce(nonce uint64) {
	s.db.journal.append(nonceChange{
		account: &s.address,
		prev:    s.data.Nonce,
	})
	s.setNonce(nonce)
}

func (s *StateObject) setNonce(nonce uint64) {
	s.data.Nonce = nonce
}

func (s *StateObject) CodeHash() []byte {
	return s.data.CodeHash
}

func (s *StateObject) Balance() *big.Int {
	return s.data.Balance
}

func (s *StateObject) Nonce() uint64 {
	return s.data.Nonce
}

// Never called, but must be present to allow StateObject to be used
// as a vm.Account interface that also satisfies the vm.ContractRef
// interface. Interfaces are awesome.
func (s *StateObject) Value() *big.Int {
	panic("Value on StateObject should never be called")
}

func (s *StateObject) ReviveStorageTrie(proofCache trie.MPTProofCache) error {
	dr := s.getDirtyReviveTrie(s.db.db)
	s.db.journal.append(reviveStorageTrieNodeChange{
		address: &s.address,
	})
	// revive nub and cache revive state
	for _, nub := range dr.ReviveTrie(proofCache.CacheNubs()) {
		kv, err := nub.ResolveKV()
		if err != nil {
			return err
		}
		for k, enc := range kv {
			var value common.Hash
			if len(enc) > 0 {
				_, content, _, err := rlp.Split(enc)
				if err != nil {
					return err
				}
				value.SetBytes(content)
			}
			s.dirtyReviveState[k] = value
		}
	}
	return nil
}

func (s *StateObject) accessState(key common.Hash) {
	if !s.db.enableAccStateEpoch(false, s.address) {
		return
	}
	s.db.journal.append(accessedStorageStateChange{
		address: &s.address,
		slot:    &key,
	})
	count := s.dirtyAccessedState[key]
	s.dirtyAccessedState[key] = count + 1
}

// TODO(0xbundler): add hash key cache later
func (s *StateObject) queryFromReviveState(db Database, reviveState map[string]common.Hash, key common.Hash) (common.Hash, bool) {
	hashKey := string(s.getTrie(db).HashKey(key.Bytes()))
	val, ok := reviveState[hashKey]
	return val, ok
}
