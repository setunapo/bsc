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

// Package state provides a caching layer atop the Ethereum state trie.
package state

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/gopool"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

const defaultNumOfSlots = 100

type revision struct {
	id           int
	journalIndex int
}

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	emptyAddr = crypto.Keccak256Hash(common.Address{}.Bytes())
)

type proofList [][]byte

func (n *proofList) Put(key []byte, value []byte) error {
	*n = append(*n, value)
	return nil
}

func (n *proofList) Delete(key []byte) error {
	panic("not supported")
}

type StateKeys map[common.Hash]struct{}

type StateObjectSyncMap struct {
	sync.Map
}

func (s *StateObjectSyncMap) LoadStateObject(addr common.Address) (*StateObject, bool) {
	stateObject, ok := s.Load(addr)
	if !ok {
		return nil, ok
	}
	return stateObject.(*StateObject), ok
}

func (s *StateObjectSyncMap) StoreStateObject(addr common.Address, stateObject *StateObject) {
	s.Store(addr, stateObject)
}

// loadStateObj is the entry for loading state object from stateObjects in StateDB or stateObjects in parallel
func (s *StateDB) loadStateObj(addr common.Address) (*StateObject, bool) {
	if s.isParallel {
		return s.parallel.stateObjects.LoadStateObject(addr)
	}
	obj, ok := s.stateObjects[addr]
	return obj, ok
}

// storeStateObj is the entry for storing state object to stateObjects in StateDB or stateObjects in parallel
func (s *StateDB) storeStateObj(addr common.Address, stateObject *StateObject) {
	if s.isParallel {
		// When a state object is stored into s.parallel.stateObjects,
		// it belongs to base StateDB, it is confirmed and valid.
		if s.parallel.isSlotDB {
			// the object could be create in SlotDB, if it got the object from DB and
			// update it to the shared `s.parallel.stateObjects``
			stateObject.db = s.parallel.baseStateDB
		}
		s.parallel.stateObjects.Store(addr, stateObject)
	} else {
		s.stateObjects[addr] = stateObject
	}
}

// deleteStateObj is the entry for deleting state object to stateObjects in StateDB or stateObjects in parallel
func (s *StateDB) deleteStateObj(addr common.Address) {
	if s.isParallel {
		s.parallel.stateObjects.Delete(addr)
	} else {
		delete(s.stateObjects, addr)
	}
}

// For parallel mode only
type ParallelState struct {
	isSlotDB  bool // isSlotDB denotes StateDB is used in slot
	SlotIndex int  // fixme: to be removed
	// stateObjects holds the state objects in the base slot db
	// the reason for using stateObjects instead of stateObjects on the outside is
	// we need a thread safe map to hold state objects since there are many slots will read
	// state objects from it;
	// And we will merge all the changes made by the concurrent slot into it.
	stateObjects *StateObjectSyncMap

	baseStateDB               *StateDB // for parallel mode, there will be a base StateDB in dispatcher routine.
	baseTxIndex               int      // slotDB is created base on this tx index.
	dirtiedStateObjectsInSlot map[common.Address]*StateObject
	unconfirmedDBInShot       map[int]*StateDB // do unconfirmed reference in same slot.

	// we will record the read detail for conflict check and
	// the changed addr or key for object merge, the changed detail can be acheived from the dirty object
	nonceChangesInSlot   map[common.Address]struct{}
	nonceReadsInSlot     map[common.Address]uint64
	balanceChangesInSlot map[common.Address]struct{} // the address's balance has been changed
	balanceReadsInSlot   map[common.Address]*big.Int // the address's balance has been read and used.
	// codeSize can be derived based on code, but codeHash can not directly derived based on code
	// - codeSize is 0 for address not exist or empty code
	// - codeHash is `common.Hash{}` for address not exist, emptyCodeHash(`Keccak256Hash(nil)`) for empty code
	// so we use codeReadsInSlot & codeHashReadsInSlot to keep code and codeHash, codeSize is derived from code
	codeReadsInSlot     map[common.Address][]byte // empty if address not exist or no code in this address
	codeHashReadsInSlot map[common.Address]common.Hash
	codeChangesInSlot   map[common.Address]struct{}
	kvReadsInSlot       map[common.Address]Storage
	kvChangesInSlot     map[common.Address]StateKeys // value will be kept in dirtiedStateObjectsInSlot
	// Actions such as SetCode, Suicide will change address's state.
	// Later call like Exist(), Empty(), HasSuicided() depend on the address's state.
	addrStateReadsInSlot   map[common.Address]bool // true: exist, false: not exist or deleted
	addrStateChangesInSlot map[common.Address]bool // true: created, false: deleted

	// Transaction will pay gas fee to system address.
	// Parallel execution will clear system address's balance at first, in order to maintain transaction's
	// gas fee value. Normal transaction will access system address twice, otherwise it means the transaction
	// needs real system address's balance, the transaction will be marked redo with keepSystemAddressBalance = true
	systemAddress            common.Address
	systemAddressOpsCount    int
	keepSystemAddressBalance bool

	// we may need to redo for some specific reasons, like we read the wrong state and need to panic in sequential mode in SubRefund
	needsRedo bool
}

// StateDB structs within the ethereum protocol are used to store anything
// within the merkle trie. StateDBs take care of caching and storing
// nested states. It's the general query interface to retrieve:
// * Contracts
// * Accounts
type StateDB struct {
	db             Database
	prefetcherLock sync.Mutex
	prefetcher     *triePrefetcher
	originalRoot   common.Hash // The pre-state root, before any changes were made
	expectedRoot   common.Hash // The state root in the block header
	stateRoot      common.Hash // The calculation result of IntermediateRoot

	trie           Trie
	hasher         crypto.KeccakState
	diffLayer      *types.DiffLayer
	diffTries      map[common.Address]Trie
	diffCode       map[common.Hash][]byte
	lightProcessed bool
	fullProcessed  bool
	pipeCommit     bool

	snapMux          sync.Mutex
	snaps            *snapshot.Tree
	snap             snapshot.Snapshot
	snapParallelLock sync.RWMutex // for parallel mode, for main StateDB, slot will read snapshot, while processor will write.
	snapDestructs    map[common.Address]struct{}
	snapAccounts     map[common.Address][]byte
	snapStorage      map[common.Address]map[string][]byte

	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects         map[common.Address]*StateObject
	stateObjectsPending  map[common.Address]struct{} // State objects finalized but not yet written to the trie
	stateObjectsDirty    map[common.Address]struct{} // State objects modified in the current execution
	storagePool          *StoragePool                // sharedPool to store L1 originStorage of stateObjects
	writeOnSharedStorage bool                        // Write to the shared origin storage of a stateObject while reading from the underlying storage layer.

	isParallel bool
	parallel   ParallelState // to keep all the parallel execution elements

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be returned
	// by StateDB.Commit.
	dbErr error

	// The refund counter, also used by state transitioning.
	refund uint64

	thash, bhash common.Hash
	txIndex      int
	logs         map[common.Hash][]*types.Log
	logSize      uint

	preimages map[common.Hash][]byte

	// Per-transaction access list
	accessList *accessList

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal        *journal
	validRevisions []revision
	nextRevisionId int

	// Measurements gathered during execution for debugging purposes
	MetricsMux           sync.Mutex
	AccountReads         time.Duration
	AccountHashes        time.Duration
	AccountUpdates       time.Duration
	AccountCommits       time.Duration
	StorageReads         time.Duration
	StorageHashes        time.Duration
	StorageUpdates       time.Duration
	StorageCommits       time.Duration
	SnapshotAccountReads time.Duration
	SnapshotStorageReads time.Duration
	SnapshotCommits      time.Duration
}

// New creates a new state from a given trie.
func New(root common.Hash, db Database, snaps *snapshot.Tree) (*StateDB, error) {
	return newStateDB(root, db, snaps)
}

// NewSlotDB creates a new State DB based on the provided StateDB.
// With parallel, each execution slot would have its own StateDB.
func NewSlotDB(db *StateDB, systemAddr common.Address, txIndex int, baseTxIndex int, keepSystem bool,
	unconfirmedDBs *sync.Map /*map[int]*StateDB*/) *StateDB {
	slotDB := db.CopyForSlot()
	slotDB.txIndex = txIndex
	slotDB.originalRoot = db.originalRoot
	slotDB.parallel.baseStateDB = db
	slotDB.parallel.baseTxIndex = baseTxIndex
	slotDB.parallel.systemAddress = systemAddr
	slotDB.parallel.systemAddressOpsCount = 0
	slotDB.parallel.keepSystemAddressBalance = keepSystem
	slotDB.storagePool = NewStoragePool()
	slotDB.EnableWriteOnSharedStorage()
	for index := baseTxIndex + 1; index < slotDB.txIndex; index++ { // txIndex
		unconfirmedDB, ok := unconfirmedDBs.Load(index)
		if ok {
			slotDB.parallel.unconfirmedDBInShot[index] = unconfirmedDB.(*StateDB)
		}
	}

	// All transactions will pay gas fee to the systemAddr at the end, this address is
	// deemed to conflict, we handle it specially, clear it now and set it back to the main
	// StateDB later;
	// But there are transactions that will try to read systemAddr's balance, such as:
	// https://bscscan.com/tx/0xcd69755be1d2f55af259441ff5ee2f312830b8539899e82488a21e85bc121a2a.
	// It will trigger transaction redo and keepSystem will be marked as true.
	if !keepSystem {
		slotDB.SetBalance(systemAddr, big.NewInt(0))
	}

	return slotDB
}

// NewWithSharedPool creates a new state with sharedStorge on layer 1.5
func NewWithSharedPool(root common.Hash, db Database, snaps *snapshot.Tree) (*StateDB, error) {
	statedb, err := newStateDB(root, db, snaps)
	if err != nil {
		return nil, err
	}
	statedb.storagePool = NewStoragePool()
	return statedb, nil
}

func newStateDB(root common.Hash, db Database, snaps *snapshot.Tree) (*StateDB, error) {
	sdb := &StateDB{
		db:           db,
		originalRoot: root,
		snaps:        snaps,
		stateObjects: make(map[common.Address]*StateObject, defaultNumOfSlots),
		parallel: ParallelState{
			SlotIndex: -1,
		},
		stateObjectsPending: make(map[common.Address]struct{}, defaultNumOfSlots),
		stateObjectsDirty:   make(map[common.Address]struct{}, defaultNumOfSlots),
		txIndex:             -1,
		logs:                make(map[common.Hash][]*types.Log, defaultNumOfSlots),
		preimages:           make(map[common.Hash][]byte),
		journal:             newJournal(),
		hasher:              crypto.NewKeccakState(),
	}
	if sdb.snaps != nil {
		if sdb.snap = sdb.snaps.Snapshot(root); sdb.snap != nil {
			sdb.snapDestructs = make(map[common.Address]struct{})
			sdb.snapAccounts = make(map[common.Address][]byte)
			sdb.snapStorage = make(map[common.Address]map[string][]byte)
		}
	}

	snapVerified := sdb.snap != nil && sdb.snap.Verified()
	tr, err := db.OpenTrie(root)
	// return error when 1. failed to open trie and 2. the snap is nil or the snap is not nil and done verification
	if err != nil && (sdb.snap == nil || snapVerified) {
		return nil, err
	}
	sdb.trie = tr
	sdb.EnableWriteOnSharedStorage() // fixme:remove when s.originStorage[key] is enabled
	return sdb, nil
}

func (s *StateDB) getStateObjectFromStateObjects(addr common.Address) (*StateObject, bool) {
	return s.loadStateObj(addr)
}

// RevertSlotDB keep the Read list for conflict detect,
// discard all state changes except:
//   - nonce and balance of from address
//   - balance of system address: will be used on merge to update SystemAddress's balance
func (s *StateDB) RevertSlotDB(from common.Address) {
	s.parallel.kvChangesInSlot = make(map[common.Address]StateKeys)

	// balance := s.parallel.balanceChangesInSlot[from]
	s.parallel.nonceChangesInSlot = make(map[common.Address]struct{})
	s.parallel.balanceChangesInSlot = make(map[common.Address]struct{}, 1)
	s.parallel.addrStateChangesInSlot = make(map[common.Address]bool) // 0: created, 1: deleted

	selfStateObject := s.parallel.dirtiedStateObjectsInSlot[from]
	systemAddress := s.parallel.systemAddress
	systemStateObject := s.parallel.dirtiedStateObjectsInSlot[systemAddress]
	s.parallel.dirtiedStateObjectsInSlot = make(map[common.Address]*StateObject, 2)
	// keep these elements
	s.parallel.dirtiedStateObjectsInSlot[from] = selfStateObject
	s.parallel.dirtiedStateObjectsInSlot[systemAddress] = systemStateObject
	s.parallel.balanceChangesInSlot[from] = struct{}{}
	s.parallel.balanceChangesInSlot[systemAddress] = struct{}{}
	s.parallel.nonceChangesInSlot[from] = struct{}{}
}

// PrepareForParallel prepares for state db to be used in parallel execution mode.
func (s *StateDB) PrepareForParallel() {
	s.isParallel = true
	s.parallel.stateObjects = &StateObjectSyncMap{}
}

// MergeSlotDB is for Parallel execution mode, when the transaction has been
// finalized(dirty -> pending) on execution slot, the execution results should be
// merged back to the main StateDB.
// And it will return and keep the slot's change list for later conflict detect.
func (s *StateDB) MergeSlotDB(slotDb *StateDB, slotReceipt *types.Receipt, txIndex int) {
	// receipt.Logs use unified log index within a block
	// align slotDB's log index to the block stateDB's logSize
	for _, l := range slotReceipt.Logs {
		l.Index += s.logSize
	}
	s.logSize += slotDb.logSize

	// before merge, pay the gas fee first: AddBalance to consensus.SystemAddress
	systemAddress := slotDb.parallel.systemAddress
	if slotDb.parallel.keepSystemAddressBalance {
		s.SetBalance(systemAddress, slotDb.GetBalance(systemAddress))
	} else {
		s.AddBalance(systemAddress, slotDb.GetBalance(systemAddress))
	}

	// only merge dirty objects
	addressesToPrefetch := make([][]byte, 0, len(slotDb.stateObjectsDirty))
	for addr := range slotDb.stateObjectsDirty {
		if _, exist := s.stateObjectsDirty[addr]; !exist {
			s.stateObjectsDirty[addr] = struct{}{}
		}
		// system address is EOA account, it should have no storage change
		if addr == systemAddress {
			continue
		}

		// stateObjects: KV, balance, nonce...
		dirtyObj, ok := slotDb.parallel.dirtiedStateObjectsInSlot[addr]
		if !ok {
			log.Error("parallel merge, but dirty object not exist!", "SlotIndex", slotDb.parallel.SlotIndex, "txIndex:", slotDb.txIndex, "addr", addr)
			continue
		}
		mainObj, exist := s.loadStateObj(addr)
		if !exist { // fixme: it is also state change
			// addr not exist on main DB, do ownership transfer
			// dirtyObj.db = s
			// dirtyObj.finalise(true) // true: prefetch on dispatcher
			mainObj = dirtyObj.deepCopy(s)
			mainObj.finalise(true)
			s.storeStateObj(addr, mainObj)
			// fixme: should not delete, would cause unconfirmed DB incorrect?
			// delete(slotDb.parallel.dirtiedStateObjectsInSlot, addr) // transfer ownership, fixme: shared read?
			if dirtyObj.deleted {
				// remove the addr from snapAccounts&snapStorage only when object is deleted.
				// "deleted" is not equal to "snapDestructs", since createObject() will add an addr for
				//  snapDestructs to destroy previous object, while it will keep the addr in snapAccounts & snapAccounts
				delete(s.snapAccounts, addr)
				delete(s.snapStorage, addr)
			}
		} else {
			// addr already in main DB, do merge: balance, KV, code, State(create, suicide)
			// can not do copy or ownership transfer directly, since dirtyObj could have outdated
			// data(may be updated within the conflict window)

			var newMainObj = mainObj // we don't need to copy the object since the storages are thread safe
			if _, ok := slotDb.parallel.addrStateChangesInSlot[addr]; ok {
				// there are 3 kinds of state change:
				// 1.Suicide
				// 2.Empty Delete
				// 3.createObject
				//   a.AddBalance,SetState to an unexist or deleted(suicide, empty delete) address.
				//   b.CreateAccount: like DAO the fork, regenerate a account carry its balance without KV
				// For these state change, do ownership transafer for efficiency:
				// dirtyObj.db = s
				// newMainObj = dirtyObj
				newMainObj = dirtyObj.deepCopy(s)
				// should not delete, would cause unconfirmed DB incorrect.
				// delete(slotDb.parallel.dirtiedStateObjectsInSlot, addr) // transfer ownership, fixme: shared read?
				if dirtyObj.deleted {
					// remove the addr from snapAccounts&snapStorage only when object is deleted.
					// "deleted" is not equal to "snapDestructs", since createObject() will add an addr for
					//  snapDestructs to destroy previous object, while it will keep the addr in snapAccounts & snapAccounts
					delete(s.snapAccounts, addr)
					delete(s.snapStorage, addr)
				}
			} else {
				// deepCopy a temporary *StateObject for safety, since slot could read the address,
				// dispatch should avoid overwrite the StateObject directly otherwise, it could
				// crash for: concurrent map iteration and map write

				if _, balanced := slotDb.parallel.balanceChangesInSlot[addr]; balanced {
					newMainObj.SetBalance(dirtyObj.Balance())
				}
				if _, coded := slotDb.parallel.codeChangesInSlot[addr]; coded {
					newMainObj.code = dirtyObj.code
					newMainObj.data.CodeHash = dirtyObj.data.CodeHash
					newMainObj.dirtyCode = true
				}
				if keys, stated := slotDb.parallel.kvChangesInSlot[addr]; stated {
					newMainObj.MergeSlotObject(s.db, dirtyObj, keys)
				}
				if _, nonced := slotDb.parallel.nonceChangesInSlot[addr]; nonced {
					// dirtyObj.Nonce() should not be less than newMainObj
					newMainObj.setNonce(dirtyObj.Nonce())
				}
			}
			newMainObj.finalise(true) // true: prefetch on dispatcher
			// update the object
			s.storeStateObj(addr, newMainObj)
		}
		addressesToPrefetch = append(addressesToPrefetch, common.CopyBytes(addr[:])) // Copy needed for closure
	}

	if s.prefetcher != nil && len(addressesToPrefetch) > 0 {
		s.prefetcher.prefetch(s.originalRoot, addressesToPrefetch, emptyAddr) // prefetch for trie node of account
	}

	for addr := range slotDb.stateObjectsPending {
		if _, exist := s.stateObjectsPending[addr]; !exist {
			s.stateObjectsPending[addr] = struct{}{}
		}
	}

	// slotDb.logs: logs will be kept in receipts, no need to do merge

	for hash, preimage := range slotDb.preimages {
		s.preimages[hash] = preimage
	}
	if s.accessList != nil {
		// fixme: accessList is not enabled yet, but it should use merge rather than overwrite Copy
		s.accessList = slotDb.accessList.Copy()
	}

	if slotDb.snaps != nil {
		for k := range slotDb.snapDestructs {
			// There could be a race condition for parallel transaction execution
			// One transaction add balance 0 to an empty address, will delete it(delete empty is enabled).
			// While another concurrent transaction could add a none-zero balance to it, make it not empty
			// We fixed it by add a addr state read record for add balance 0
			s.snapParallelLock.Lock()
			s.snapDestructs[k] = struct{}{}
			s.snapParallelLock.Unlock()
		}

		// slotDb.snapAccounts should be empty, comment out and to be deleted later
		// for k, v := range slotDb.snapAccounts {
		//	s.snapAccounts[k] = v
		// }
		// slotDb.snapStorage should be empty, comment out and to be deleted later
		// for k, v := range slotDb.snapStorage {
		// 	temp := make(map[string][]byte)
		//	for kk, vv := range v {
		//		temp[kk] = vv
		//	}
		//	s.snapStorage[k] = temp
		// }
	}
}

func (s *StateDB) EnableWriteOnSharedStorage() {
	s.writeOnSharedStorage = true
}
func (s *StateDB) SetSlotIndex(index int) {
	s.parallel.SlotIndex = index
}

// StartPrefetcher initializes a new trie prefetcher to pull in nodes from the
// state trie concurrently while the state is mutated so that when we reach the
// commit phase, most of the needed data is already hot.
func (s *StateDB) StartPrefetcher(namespace string) {
	s.prefetcherLock.Lock()
	defer s.prefetcherLock.Unlock()
	if s.prefetcher != nil {
		s.prefetcher.close()
		s.prefetcher = nil
	}
	if s.snap != nil {
		s.prefetcher = newTriePrefetcher(s.db, s.originalRoot, namespace)
	}
}

// StopPrefetcher terminates a running prefetcher and reports any leftover stats
// from the gathered metrics.
func (s *StateDB) StopPrefetcher() {
	s.prefetcherLock.Lock()
	defer s.prefetcherLock.Unlock()
	if s.prefetcher != nil {
		s.prefetcher.close()
		s.prefetcher = nil
	}
}

// Mark that the block is processed by diff layer
func (s *StateDB) SetExpectedStateRoot(root common.Hash) {
	s.expectedRoot = root
}

// Mark that the block is processed by diff layer
func (s *StateDB) MarkLightProcessed() {
	s.lightProcessed = true
}

// Enable the pipeline commit function of statedb
func (s *StateDB) EnablePipeCommit() {
	if s.snap != nil {
		s.pipeCommit = true
	}
}

// Mark that the block is full processed
func (s *StateDB) MarkFullProcessed() {
	s.fullProcessed = true
}

func (s *StateDB) IsLightProcessed() bool {
	return s.lightProcessed
}

// setError remembers the first non-nil error it is called with.
func (s *StateDB) setError(err error) {
	if s.dbErr == nil {
		s.dbErr = err
	}
}

func (s *StateDB) Error() error {
	return s.dbErr
}

// Not thread safe
func (s *StateDB) Trie() (Trie, error) {
	if s.trie == nil {
		err := s.WaitPipeVerification()
		if err != nil {
			return nil, err
		}
		tr, err := s.db.OpenTrie(s.originalRoot)
		if err != nil {
			return nil, err
		}
		s.trie = tr
	}
	return s.trie, nil
}

func (s *StateDB) SetDiff(diffLayer *types.DiffLayer, diffTries map[common.Address]Trie, diffCode map[common.Hash][]byte) {
	s.diffLayer, s.diffTries, s.diffCode = diffLayer, diffTries, diffCode
}

func (s *StateDB) SetSnapData(snapDestructs map[common.Address]struct{}, snapAccounts map[common.Address][]byte,
	snapStorage map[common.Address]map[string][]byte) {
	s.snapDestructs, s.snapAccounts, s.snapStorage = snapDestructs, snapAccounts, snapStorage
}

func (s *StateDB) AddLog(log *types.Log) {
	s.journal.append(addLogChange{txhash: s.thash})

	log.TxHash = s.thash
	log.BlockHash = s.bhash
	log.TxIndex = uint(s.txIndex)
	log.Index = s.logSize
	s.logs[s.thash] = append(s.logs[s.thash], log)
	s.logSize++
}

func (s *StateDB) GetLogs(hash common.Hash) []*types.Log {
	return s.logs[hash]
}

func (s *StateDB) Logs() []*types.Log {
	var logs []*types.Log
	for _, lgs := range s.logs {
		logs = append(logs, lgs...)
	}
	return logs
}

// AddPreimage records a SHA3 preimage seen by the VM.
func (s *StateDB) AddPreimage(hash common.Hash, preimage []byte) {
	if _, ok := s.preimages[hash]; !ok {
		s.journal.append(addPreimageChange{hash: hash})
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		s.preimages[hash] = pi
	}
}

// Preimages returns a list of SHA3 preimages that have been submitted.
func (s *StateDB) Preimages() map[common.Hash][]byte {
	return s.preimages
}

// AddRefund adds gas to the refund counter
func (s *StateDB) AddRefund(gas uint64) {
	s.journal.append(refundChange{prev: s.refund})
	s.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (s *StateDB) SubRefund(gas uint64) {
	s.journal.append(refundChange{prev: s.refund})
	if gas > s.refund {
		if s.isParallel {
			// we don't need to panic here if we read the wrong state, we just need to redo this transaction
			log.Info(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, s.refund), "tx", s.thash.String())
			s.parallel.needsRedo = true
			return
		}
		panic(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, s.refund))
	}
	s.refund -= gas
}

// For Parallel Execution Mode, it can be seen as Penetrated Access:
//   -------------------------------------------------------
//   | BaseTxIndex | Unconfirmed Txs... | Current TxIndex |
//   -------------------------------------------------------
// Access from the unconfirmed DB with range&priority:  txIndex -1(previous tx) -> baseTxIndex + 1
func (s *StateDB) getBalanceFromUnconfirmedDB(addr common.Address) *big.Int {
	if addr == s.parallel.systemAddress {
		// never get systemaddress from unconfirmed DB
		return nil
	}

	for i := s.txIndex - 1; i > s.parallel.baseTxIndex; i-- {
		if db, ok := s.parallel.unconfirmedDBInShot[i]; ok {
			// 1.Refer the state of address, exist or not in dirtiedStateObjectsInSlot
			if obj, exist := db.parallel.dirtiedStateObjectsInSlot[addr]; exist {
				balanceHit := false
				if _, exist := db.parallel.addrStateChangesInSlot[addr]; exist {
					balanceHit = true
				}
				if _, exist := db.parallel.balanceChangesInSlot[addr]; exist { // only changed balance is reliable
					balanceHit = true
				}
				if !balanceHit {
					continue
				}
				balance := obj.Balance()
				if obj.deleted {
					balance = common.Big0
				}
				return balance
			}
		}
	}
	return nil
}

// Similar to getBalanceFromUnconfirmedDB
func (s *StateDB) getNonceFromUnconfirmedDB(addr common.Address) (uint64, bool) {
	if addr == s.parallel.systemAddress {
		// never get systemaddress from unconfirmed DB
		return 0, false
	}

	for i := s.txIndex - 1; i > s.parallel.baseTxIndex; i-- {
		if unconfirmedDb, ok := s.parallel.unconfirmedDBInShot[i]; ok {
			nonceHit := false
			if _, ok := unconfirmedDb.parallel.addrStateChangesInSlot[addr]; ok {
				nonceHit = true
			} else if _, ok := unconfirmedDb.parallel.nonceChangesInSlot[addr]; ok {
				nonceHit = true
			}
			if !nonceHit {
				// nonce refer not hit, try next unconfirmedDb
				continue
			}
			// nonce hit, return the nonce
			obj := unconfirmedDb.parallel.dirtiedStateObjectsInSlot[addr]
			if obj == nil {
				// could not exist, if it is changed but reverted
				// fixme: revert should remove the change record
				log.Debug("Get nonce from UnconfirmedDB, changed but object not exist, ",
					"txIndex", s.txIndex, "referred txIndex", i, "addr", addr)
				continue
			}
			nonce := obj.Nonce()
			// deleted object with nonce == 0
			if obj.deleted {
				nonce = 0
			}
			return nonce, true
		}
	}
	return 0, false
}

// Similar to getBalanceFromUnconfirmedDB
// It is not only for code, but also codeHash and codeSize, we return the *StateObject for convienence.
func (s *StateDB) getCodeFromUnconfirmedDB(addr common.Address) ([]byte, bool) {
	if addr == s.parallel.systemAddress {
		// never get systemaddress from unconfirmed DB
		return nil, false
	}

	for i := s.txIndex - 1; i > s.parallel.baseTxIndex; i-- {
		if db, ok := s.parallel.unconfirmedDBInShot[i]; ok {
			codeHit := false
			if _, exist := db.parallel.addrStateChangesInSlot[addr]; exist {
				codeHit = true
			}
			if _, exist := db.parallel.codeChangesInSlot[addr]; exist {
				codeHit = true
			}
			if !codeHit {
				// try next unconfirmedDb
				continue
			}
			obj := db.parallel.dirtiedStateObjectsInSlot[addr]
			if obj == nil {
				// could not exist, if it is changed but reverted
				// fixme: revert should remove the change record
				log.Debug("Get code from UnconfirmedDB, changed but object not exist, ",
					"txIndex", s.txIndex, "referred txIndex", i, "addr", addr)
				continue
			}
			code := obj.Code(s.db)
			if obj.deleted {
				code = nil
			}
			return code, true
		}
	}
	return nil, false
}

// Similar to getCodeFromUnconfirmedDB
// but differ when address is deleted or not exist
func (s *StateDB) getCodeHashFromUnconfirmedDB(addr common.Address) (common.Hash, bool) {
	if addr == s.parallel.systemAddress {
		// never get systemaddress from unconfirmed DB
		return common.Hash{}, false
	}

	for i := s.txIndex - 1; i > s.parallel.baseTxIndex; i-- {
		if db, ok := s.parallel.unconfirmedDBInShot[i]; ok {
			hashHit := false
			if _, exist := db.parallel.addrStateChangesInSlot[addr]; exist {
				hashHit = true
			}
			if _, exist := db.parallel.codeChangesInSlot[addr]; exist {
				hashHit = true
			}
			if !hashHit {
				// try next unconfirmedDb
				continue
			}

			obj := db.parallel.dirtiedStateObjectsInSlot[addr]
			if obj == nil {
				// could not exist, if it is changed but reverted
				// fixme: revert should remove the change record
				log.Debug("Get codeHash from UnconfirmedDB, changed but object not exist, ",
					"txIndex", s.txIndex, "referred txIndex", i, "addr", addr)
				continue
			}
			codeHash := common.Hash{}
			if !obj.deleted {
				codeHash = common.BytesToHash(obj.CodeHash())
			}
			return codeHash, true
		}
	}
	return common.Hash{}, false
}

// Similar to getCodeFromUnconfirmedDB
// It is for address state check of: Exist(), Empty() and HasSuicided()
// Since the unconfirmed DB should have done Finalise() with `deleteEmptyObjects = true`
// If the dirty address is empty or suicided, it will be marked as deleted, so we only need to return `deleted` or not.
func (s *StateDB) getAddrStateFromUnconfirmedDB(addr common.Address) (deleted bool, exist bool) {
	if addr == s.parallel.systemAddress {
		// never get systemaddress from unconfirmed DB
		return false, false
	}

	// check the unconfirmed DB with range:  baseTxIndex -> txIndex -1(previous tx)
	for i := s.txIndex - 1; i > s.parallel.baseTxIndex; i-- {
		if db, ok := s.parallel.unconfirmedDBInShot[i]; ok {
			if exist, ok := db.parallel.addrStateChangesInSlot[addr]; ok {
				if _, ok := db.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
					// could not exist, if it is changed but reverted
					// fixme: revert should remove the change record
					log.Debug("Get addr State from UnconfirmedDB, changed but object not exist, ",
						"txIndex", s.txIndex, "referred txIndex", i, "addr", addr)
					continue
				}

				return exist, true
			}
		}
	}
	return false, false
}

func (s *StateDB) getKVFromUnconfirmedDB(addr common.Address, key common.Hash) (common.Hash, bool) {
	// check the unconfirmed DB with range:  baseTxIndex -> txIndex -1(previous tx)
	for i := s.txIndex - 1; i > s.parallel.baseTxIndex; i-- {
		if db, ok := s.parallel.unconfirmedDBInShot[i]; ok {
			if obj, ok := db.parallel.dirtiedStateObjectsInSlot[addr]; ok { // if deleted on merge, can get from main StateDB, ok but fixme: concurrent safe
				if obj.deleted {
					return common.Hash{}, true
				}
				if _, ok := db.parallel.kvChangesInSlot[addr]; ok {
					if val, exist := obj.dirtyStorage.GetValue(key); exist {
						return val, true
					}
					if val, exist := obj.pendingStorage.GetValue(key); exist { // fixme: can be removed
						log.Error("Get KV from Unconfirmed StateDB, in pending",
							"my txIndex", s.txIndex, "DB's txIndex", i, "addr", addr,
							"key", key, "val", val)
						return val, true
					}
				}
			}
		}
	}
	return common.Hash{}, false
}

func (s *StateDB) getStateObjectFromUnconfirmedDB(addr common.Address) (*StateObject, bool) {
	// check the unconfirmed DB with range:  baseTxIndex -> txIndex -1(previous tx)
	for i := s.txIndex - 1; i > s.parallel.baseTxIndex; i-- {
		if db, ok := s.parallel.unconfirmedDBInShot[i]; ok {
			if obj, ok := db.parallel.dirtiedStateObjectsInSlot[addr]; ok { // if deleted on merge, can get from main StateDB, ok but fixme: concurrent safe
				return obj, true
			}
		}
	}
	return nil, false
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (s *StateDB) Exist(addr common.Address) bool {
	if s.parallel.isSlotDB {
		// 1.Try to get from dirty
		if obj, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
			// dirty object should not be deleted, since deleted is only flagged on finalise
			// and if it is suicided in contract call, suicide is taken as exist until it is finalised
			// todo: add a check here, to be removed later
			if obj.deleted || obj.suicided {
				log.Error("Exist in dirty, but marked as deleted or suicided",
					"txIndex", s.txIndex, "baseTxIndex:", s.parallel.baseTxIndex)
			}
			return true
		}
		// 2.Try to get from uncomfirmed & main DB
		// 2.1 Already read before
		if exist, ok := s.parallel.addrStateReadsInSlot[addr]; ok {
			return exist
		}
		// 2.2 Try to get from unconfirmed DB if exist
		if exist, ok := s.getAddrStateFromUnconfirmedDB(addr); ok {
			s.parallel.addrStateReadsInSlot[addr] = exist // update and cache
			return exist
		}
	}
	// 3.Try to get from main StateDB
	exist := s.getStateObjectNoSlot(addr) != nil
	if s.parallel.isSlotDB {
		s.parallel.addrStateReadsInSlot[addr] = exist // update and cache
	}
	return exist
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (s *StateDB) Empty(addr common.Address) bool {
	if s.parallel.isSlotDB {
		// 1.Try to get from dirty
		if obj, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
			// dirty object is light copied and fixup on need,
			// empty could be wrong, except it is created with this TX
			if _, ok := s.parallel.addrStateChangesInSlot[addr]; ok {
				return obj.empty()
			}
			// so we have to check it manually
			// empty means: Nonce == 0 && Balance == 0 && CodeHash == emptyCodeHash
			if s.GetNonce(addr) != 0 {
				return false
			}
			if s.GetBalance(addr).Sign() != 0 {
				return false
			}
			codeHash := s.GetCodeHash(addr)
			return bytes.Equal(codeHash.Bytes(), emptyCodeHash) // code is empty, the object is empty
		}
		// 2.Try to get from uncomfirmed & main DB
		// 2.1 Already read before
		if exist, ok := s.parallel.addrStateReadsInSlot[addr]; ok {
			// exist means not empty
			return !exist
		}
		// 2.2 Try to get from unconfirmed DB if exist
		if exist, ok := s.getAddrStateFromUnconfirmedDB(addr); ok {
			s.parallel.addrStateReadsInSlot[addr] = exist // update and cache
			return !exist
		}
	}

	so := s.getStateObjectNoSlot(addr)
	empty := (so == nil || so.empty())
	if s.parallel.isSlotDB {
		s.parallel.addrStateReadsInSlot[addr] = !empty // update and cache
	}
	return empty
}

// GetBalance retrieves the balance from the given address or 0 if object not found
// GetFrom the dirty list => from unconfirmed DB => get from main stateDB
func (s *StateDB) GetBalance(addr common.Address) *big.Int {
	if s.parallel.isSlotDB {
		if addr == s.parallel.systemAddress {
			s.parallel.systemAddressOpsCount++
		}
		// 1.Try to get from dirty
		if _, ok := s.parallel.balanceChangesInSlot[addr]; ok {
			obj := s.parallel.dirtiedStateObjectsInSlot[addr] // addr must exist in dirtiedStateObjectsInSlot
			return obj.Balance()
		}
		// 2.Try to get from uncomfirmed DB or main DB
		// 2.1 Already read before
		if balance, ok := s.parallel.balanceReadsInSlot[addr]; ok {
			return balance
		}
		// 2.2 Try to get from unconfirmed DB if exist
		if balance := s.getBalanceFromUnconfirmedDB(addr); balance != nil {
			s.parallel.balanceReadsInSlot[addr] = balance
			return balance
		}
	}
	// 3. Try to get from main StateObejct
	balance := common.Big0
	stateObject := s.getStateObjectNoSlot(addr)
	if stateObject != nil {
		balance = stateObject.Balance()
	}
	if s.parallel.isSlotDB {
		s.parallel.balanceReadsInSlot[addr] = balance
	}
	return balance
}

func (s *StateDB) GetNonce(addr common.Address) uint64 {
	if s.parallel.isSlotDB {
		// 1.Try to get from dirty
		if _, ok := s.parallel.nonceChangesInSlot[addr]; ok {
			obj := s.parallel.dirtiedStateObjectsInSlot[addr] // addr must exist
			return obj.Nonce()
		}
		// 2.Try to get from uncomfirmed DB or main DB
		// 2.1 Already read before
		if nonce, ok := s.parallel.nonceReadsInSlot[addr]; ok {
			return nonce
		}
		// 2.2 Try to get from unconfirmed DB if exist
		if nonce, ok := s.getNonceFromUnconfirmedDB(addr); ok {
			s.parallel.nonceReadsInSlot[addr] = nonce
			return nonce
		}
	}
	// 3.Try to get from main StateDB
	var nonce uint64 = 0
	stateObject := s.getStateObjectNoSlot(addr)
	if stateObject != nil {
		nonce = stateObject.Nonce()
	}
	if s.parallel.isSlotDB {
		s.parallel.nonceReadsInSlot[addr] = nonce
	}

	return nonce
}

// TxIndex returns the current transaction index set by Prepare.
func (s *StateDB) TxIndex() int {
	return s.txIndex
}

// BlockHash returns the current block hash set by Prepare.
func (s *StateDB) BlockHash() common.Hash {
	return s.bhash
}

// BaseTxIndex returns the tx index that slot db based.
func (s *StateDB) BaseTxIndex() int {
	return s.parallel.baseTxIndex
}

func (s *StateDB) IsParallelReadsValid() bool {
	slotDB := s
	if !slotDB.parallel.isSlotDB {
		log.Error("IsSlotDBReadsValid slotDB should be slot DB", "txIndex", slotDB.txIndex)
		return false
	}

	mainDB := slotDB.parallel.baseStateDB
	if mainDB.parallel.isSlotDB {
		log.Error("IsSlotDBReadsValid s should be main DB", "txIndex", slotDB.txIndex)
		return false
	}
	// for nonce
	for addr, nonceSlot := range slotDB.parallel.nonceReadsInSlot {
		nonceMain := mainDB.GetNonce(addr)
		if nonceSlot != nonceMain {
			log.Debug("IsSlotDBReadsValid nonce read is invalid", "addr", addr,
				"nonceSlot", nonceSlot, "nonceMain", nonceMain,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	// balance
	for addr, balanceSlot := range slotDB.parallel.balanceReadsInSlot {
		balanceMain := mainDB.GetBalance(addr)
		if balanceSlot.Cmp(balanceMain) != 0 {
			log.Debug("IsSlotDBReadsValid balance read is invalid", "addr", addr,
				"balanceSlot", balanceSlot, "balanceMain", balanceMain,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	// check code
	for addr, codeSlot := range slotDB.parallel.codeReadsInSlot {
		codeMain := mainDB.GetCode(addr)
		if !bytes.Equal(codeSlot, codeMain) {
			log.Debug("IsSlotDBReadsValid code read is invalid", "addr", addr,
				"len codeSlot", len(codeSlot), "len codeMain", len(codeMain),
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	// check codeHash
	for addr, codeHashSlot := range slotDB.parallel.codeHashReadsInSlot {
		codeHashMain := mainDB.GetCodeHash(addr)
		if !bytes.Equal(codeHashSlot.Bytes(), codeHashMain.Bytes()) {
			log.Debug("IsSlotDBReadsValid codehash read is invalid", "addr", addr,
				"codeHashSlot", codeHashSlot, "codeHashMain", codeHashMain,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	// check KV
	for addr, slotStorage := range slotDB.parallel.kvReadsInSlot {
		conflict := false
		slotStorage.Range(func(keySlot, valSlot interface{}) bool {
			valMain := mainDB.GetState(addr, keySlot.(common.Hash))
			if !bytes.Equal(valSlot.(common.Hash).Bytes(), valMain.Bytes()) {
				log.Debug("IsSlotDBReadsValid KV read is invalid", "addr", addr,
					"key", keySlot.(common.Hash), "valSlot", valSlot.(common.Hash), "valMain", valMain,
					"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
				conflict = true
				return false // return false, Range will be terminated.
			}
			return true // return true, Range will try next KV
		})
		if conflict {
			return false
		}
	}
	// addr state check
	for addr, stateSlot := range slotDB.parallel.addrStateReadsInSlot {
		stateMain := false // addr not exist
		if mainDB.getStateObjectNoSlot(addr) != nil {
			stateMain = true // addr exist in main DB
		}
		if stateSlot != stateMain {
			log.Debug("IsSlotDBReadsValid addrState read invalid(true: exist, false: not exist)",
				"addr", addr, "stateSlot", stateSlot, "stateMain", stateMain,
				"txIndex", slotDB.txIndex, "baseTxIndex", slotDB.parallel.baseTxIndex)
			return false
		}
	}
	return true
}

// For most of the transactions, systemAddressOpsCount should be 2:
//  one for SetBalance(0) on NewSlotDB()
//  the other is for AddBalance(GasFee) at the end.
// (systemAddressOpsCount > 2) means the transaction tries to access systemAddress, in
// this case, we should redo and keep its balance on NewSlotDB()
func (s *StateDB) SystemAddressRedo() bool {
	return s.parallel.systemAddressOpsCount > 2
}

// NeedsRedo returns true if there is any clear reason that we need to redo this transaction
func (s *StateDB) NeedsRedo() bool {
	return s.parallel.needsRedo
}

func (s *StateDB) GetCode(addr common.Address) []byte {
	if s.parallel.isSlotDB {
		// 1.Try to get from dirty
		if _, ok := s.parallel.codeChangesInSlot[addr]; ok {
			obj := s.parallel.dirtiedStateObjectsInSlot[addr] // addr must exist in dirtiedStateObjectsInSlot
			code := obj.Code(s.db)
			return code
		}
		// 2.Try to get from uncomfirmed DB or main DB
		// 2.1 Already read before
		if code, ok := s.parallel.codeReadsInSlot[addr]; ok {
			return code
		}
		// 2.2 Try to get from unconfirmed DB if exist
		if code, ok := s.getCodeFromUnconfirmedDB(addr); ok {
			s.parallel.codeReadsInSlot[addr] = code
			return code
		}
	}

	// 3. Try to get from main StateObejct
	stateObject := s.getStateObjectNoSlot(addr)
	var code []byte
	if stateObject != nil {
		code = stateObject.Code(s.db)
	}
	if s.parallel.isSlotDB {
		s.parallel.codeReadsInSlot[addr] = code
	}
	return code
}

func (s *StateDB) GetCodeSize(addr common.Address) int {
	if s.parallel.isSlotDB {
		// 1.Try to get from dirty
		if _, ok := s.parallel.codeChangesInSlot[addr]; ok {
			obj := s.parallel.dirtiedStateObjectsInSlot[addr] // addr must exist in dirtiedStateObjectsInSlot
			return obj.CodeSize(s.db)
		}
		// 2.Try to get from uncomfirmed DB or main DB
		// 2.1 Already read before
		if code, ok := s.parallel.codeReadsInSlot[addr]; ok {
			return len(code) // len(nil) is 0 too
		}
		// 2.2 Try to get from unconfirmed DB if exist
		if code, ok := s.getCodeFromUnconfirmedDB(addr); ok {
			s.parallel.codeReadsInSlot[addr] = code
			return len(code) // len(nil) is 0 too
		}
	}

	// 3. Try to get from main StateObejct
	var codeSize int = 0
	var code []byte
	stateObject := s.getStateObjectNoSlot(addr)

	if stateObject != nil {
		code = stateObject.Code(s.db)
		codeSize = stateObject.CodeSize(s.db)
	}
	if s.parallel.isSlotDB {
		s.parallel.codeReadsInSlot[addr] = code
	}
	return codeSize
}

// return value of GetCodeHash:
//  - common.Hash{}: the address does not exist
//  - emptyCodeHash: the address exist, but code is empty
//  - others:        the address exist, and code is not empty
func (s *StateDB) GetCodeHash(addr common.Address) common.Hash {
	if s.parallel.isSlotDB {
		// 1.Try to get from dirty
		if _, ok := s.parallel.codeChangesInSlot[addr]; ok {
			obj := s.parallel.dirtiedStateObjectsInSlot[addr] // addr must exist in dirtiedStateObjectsInSlot
			return common.BytesToHash(obj.CodeHash())
		}
		// 2.Try to get from uncomfirmed DB or main DB
		// 2.1 Already read before
		if codeHash, ok := s.parallel.codeHashReadsInSlot[addr]; ok {
			return codeHash
		}
		// 2.2 Try to get from unconfirmed DB if exist
		if codeHash, ok := s.getCodeHashFromUnconfirmedDB(addr); ok {
			s.parallel.codeHashReadsInSlot[addr] = codeHash
			return codeHash
		}
	}

	// 3. Try to get from main StateObejct
	stateObject := s.getStateObjectNoSlot(addr)
	codeHash := common.Hash{}
	if stateObject != nil {
		codeHash = common.BytesToHash(stateObject.CodeHash())
	}
	if s.parallel.isSlotDB {
		s.parallel.codeHashReadsInSlot[addr] = codeHash
	}
	return codeHash
}

// GetState retrieves a value from the given account's storage trie.
// For parallel mode wih, get from the state in order:
//   -> self dirty, both Slot & MainProcessor
//   -> pending of self: Slot on merge
//   -> pending of unconfirmed DB
//   -> pending of main StateDB
//   -> origin
func (s *StateDB) GetState(addr common.Address, hash common.Hash) common.Hash {
	if s.parallel.isSlotDB {

		// 1.Try to get from dirty
		if exist, ok := s.parallel.addrStateChangesInSlot[addr]; ok {
			if !exist {
				return common.Hash{}
			}
			obj := s.parallel.dirtiedStateObjectsInSlot[addr] // addr must exist in dirtiedStateObjectsInSlot
			return obj.GetState(s.db, hash)
		}
		if keys, ok := s.parallel.kvChangesInSlot[addr]; ok {
			if _, ok := keys[hash]; ok {
				obj := s.parallel.dirtiedStateObjectsInSlot[addr] // addr must exist in dirtiedStateObjectsInSlot
				return obj.GetState(s.db, hash)
			}
		}
		// 2.Try to get from uncomfirmed DB or main DB
		// 2.1 Already read before
		if storage, ok := s.parallel.kvReadsInSlot[addr]; ok {
			if val, ok := storage.GetValue(hash); ok {
				return val
			}
		}
		// 2.2 Try to get from unconfirmed DB if exist
		if val, ok := s.getKVFromUnconfirmedDB(addr, hash); ok {
			if s.parallel.kvReadsInSlot[addr] == nil {
				s.parallel.kvReadsInSlot[addr] = newStorage(false)
			}
			s.parallel.kvReadsInSlot[addr].StoreValue(hash, val) // update cache
			return val
		}
	}

	// 3.Get from main StateDB
	stateObject := s.getStateObjectNoSlot(addr)
	val := common.Hash{}
	if stateObject != nil {
		val = stateObject.GetState(s.db, hash)
	}
	if s.parallel.isSlotDB {
		if s.parallel.kvReadsInSlot[addr] == nil {
			s.parallel.kvReadsInSlot[addr] = newStorage(false)
		}
		s.parallel.kvReadsInSlot[addr].StoreValue(hash, val) // update cache
	}
	return val
}

// GetProof returns the Merkle proof for a given account.
func (s *StateDB) GetProof(addr common.Address) ([][]byte, error) {
	return s.GetProofByHash(crypto.Keccak256Hash(addr.Bytes()))
}

// GetProofByHash returns the Merkle proof for a given account.
func (s *StateDB) GetProofByHash(addrHash common.Hash) ([][]byte, error) {
	var proof proofList
	if _, err := s.Trie(); err != nil {
		return nil, err
	}
	err := s.trie.Prove(addrHash[:], 0, &proof)
	return proof, err
}

// GetStorageProof returns the Merkle proof for given storage slot.
func (s *StateDB) GetStorageProof(a common.Address, key common.Hash) ([][]byte, error) {
	var proof proofList
	trie := s.StorageTrie(a)
	if trie == nil {
		return proof, errors.New("storage trie for requested address does not exist")
	}
	err := trie.Prove(crypto.Keccak256(key.Bytes()), 0, &proof)
	return proof, err
}

// GetStorageProofByHash returns the Merkle proof for given storage slot.
func (s *StateDB) GetStorageProofByHash(a common.Address, key common.Hash) ([][]byte, error) {
	var proof proofList
	trie := s.StorageTrie(a)
	if trie == nil {
		return proof, errors.New("storage trie for requested address does not exist")
	}
	err := trie.Prove(crypto.Keccak256(key.Bytes()), 0, &proof)
	return proof, err
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
func (s *StateDB) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	if s.parallel.isSlotDB {
		// 1.No need to get from pending of itself even on merge, since stateobject in SlotDB won't do finalise
		// 2.Try to get from uncomfirmed DB or main DB
		//   KVs in unconfirmed DB can be seen as pending storage
		//   KVs in main DB are merged from SlotDB and has done finalise() on merge, can be seen as pending storage too.
		// 2.1 Already read before
		if storage, ok := s.parallel.kvReadsInSlot[addr]; ok {
			if val, ok := storage.GetValue(hash); ok {
				return val
			}
		}
		// 2.2 Try to get from unconfirmed DB if exist
		if val, ok := s.getKVFromUnconfirmedDB(addr, hash); ok {
			if s.parallel.kvReadsInSlot[addr] == nil {
				s.parallel.kvReadsInSlot[addr] = newStorage(false)
			}
			s.parallel.kvReadsInSlot[addr].StoreValue(hash, val) // update cache
			return val
		}
	}
	// 3. Try to get from main DB
	stateObject := s.getStateObjectNoSlot(addr)
	val := common.Hash{}
	if stateObject != nil {
		val = stateObject.GetCommittedState(s.db, hash)
	}
	if s.parallel.isSlotDB {
		if s.parallel.kvReadsInSlot[addr] == nil {
			s.parallel.kvReadsInSlot[addr] = newStorage(false)
		}
		s.parallel.kvReadsInSlot[addr].StoreValue(hash, val) // update cache
	}
	return val
}

// Database retrieves the low level database supporting the lower level trie ops.
func (s *StateDB) Database() Database {
	return s.db
}

// StorageTrie returns the storage trie of an account.
// The return value is a copy and is nil for non-existent accounts.
func (s *StateDB) StorageTrie(addr common.Address) Trie {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return nil
	}
	cpy := stateObject.deepCopy(s)
	cpy.updateTrie(s.db)
	return cpy.getTrie(s.db)
}

func (s *StateDB) HasSuicided(addr common.Address) bool {
	if s.parallel.isSlotDB {
		// 1.Try to get from dirty
		if obj, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
			return obj.suicided
		}
		// 2.Try to get from uncomfirmed
		if deleted, ok := s.getAddrStateFromUnconfirmedDB(addr); ok {
			if deleted {
				return false
			}
			return false
		}
	}
	stateObject := s.getStateObjectNoSlot(addr)
	if stateObject != nil {
		return stateObject.suicided
	}
	return false
}

/*
 * SETTERS
 */

// the source mainObj should be got from the main StateDB
// we have to update its nonce, balance, code if they have updated in the unconfirmed DBs
/*
func (s *StateDB) unconfirmedLightCopy(mainObj *StateObject) *StateObject {
	newObj := mainObj.lightCopy(s) // copied nonce, balance, code from base DB

	// do balance fixup only when it exist in unconfirmed DB
	if nonce, ok := s.getNonceFromUnconfirmedDB(mainObj.address); ok {
		// code got from unconfirmed DB
		newObj.setNonce(nonce)
	}

	// do balance fixup
	if balance := s.getBalanceFromUnconfirmedDB(mainObj.address); balance != nil {
		// balance got from unconfirmed DB
		newObj.setBalance(balance)
	}
	// do code fixup
	if codeObj, ok := s.getCodeFromUnconfirmedDB(mainObj.address); ok {
		newObj.setCode(crypto.Keccak256Hash(codeObj), codeObj) // fixme: to confirm if we should use "codeObj.Code(db)"
		newObj.dirtyCode = false                               // copy does not make the code dirty,
	}
	return newObj
}
*/

// AddBalance adds amount to the account associated with addr.
func (s *StateDB) AddBalance(addr common.Address, amount *big.Int) {
	// if s.parallel.isSlotDB {
	// add balance will perform a read operation first
	// s.parallel.balanceReadsInSlot[addr] = struct{}{} // fixme: to make the the balance valid, since unconfirmed would refer it.
	// if amount.Sign() == 0 {
	// if amount == 0, no balance change, but there is still an empty check.
	// take this empty check as addr state read(create, suicide, empty delete)
	// s.parallel.addrStateReadsInSlot[addr] = struct{}{}
	// }
	// }

	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		if s.parallel.isSlotDB {
			if addr == s.parallel.systemAddress {
				s.parallel.systemAddressOpsCount++
			}
			if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
				newStateObject := stateObject.lightCopy(s) // light copy from main DB
				// do balance fixup from the confirmed DB, it could be more reliable than main DB
				if balance := s.getBalanceFromUnconfirmedDB(addr); balance != nil {
					newStateObject.setBalance(balance)
				}
				s.parallel.balanceReadsInSlot[addr] = newStateObject.Balance() // could read from main DB or unconfirmed DB

				newStateObject.AddBalance(amount)
				s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
				// if amount.Sign() != 0 { // todo: to reenable it
				s.parallel.balanceChangesInSlot[addr] = struct{}{}
				return
			}
		}
		stateObject.AddBalance(amount)
	}
}

// SubBalance subtracts amount from the account associated with addr.
func (s *StateDB) SubBalance(addr common.Address, amount *big.Int) {
	// if s.parallel.isSlotDB {
	// if amount.Sign() != 0 {
	// unlike add, sub 0 balance will not touch empty object
	// s.parallel.balanceReadsInSlot[addr] = struct{}{}
	// }
	// }
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		if s.parallel.isSlotDB {
			if addr == s.parallel.systemAddress {
				s.parallel.systemAddressOpsCount++
			}
			if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
				newStateObject := stateObject.lightCopy(s) // light copy from main DB
				// do balance fixup from the confirmed DB, it could be more reliable than main DB
				if balance := s.getBalanceFromUnconfirmedDB(addr); balance != nil {
					newStateObject.setBalance(balance)
				}
				s.parallel.balanceReadsInSlot[addr] = newStateObject.Balance()
				newStateObject.SubBalance(amount)
				s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject

				// if amount.Sign() != 0 { // todo: to reenable it
				s.parallel.balanceChangesInSlot[addr] = struct{}{}
				return
			}
		}
		stateObject.SubBalance(amount)
	}
}

func (s *StateDB) SetBalance(addr common.Address, amount *big.Int) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		if s.parallel.isSlotDB {
			if addr == s.parallel.systemAddress {
				s.parallel.systemAddressOpsCount++
			}
			s.parallel.balanceChangesInSlot[addr] = struct{}{}
			if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
				newStateObject := stateObject.lightCopy(s)
				newStateObject.SetBalance(amount)
				s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
				return
			}
		}
		stateObject.SetBalance(amount)
	}
}

func (s *StateDB) SetNonce(addr common.Address, nonce uint64) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		if s.parallel.isSlotDB {
			s.parallel.nonceChangesInSlot[addr] = struct{}{}
			if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
				newStateObject := stateObject.lightCopy(s)
				newStateObject.SetNonce(nonce)
				s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
				return
			}
		}
		stateObject.SetNonce(nonce)
	}
}

func (s *StateDB) SetCode(addr common.Address, code []byte) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		codeHash := crypto.Keccak256Hash(code)
		if s.parallel.isSlotDB {
			s.parallel.codeChangesInSlot[addr] = struct{}{}
			if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
				newStateObject := stateObject.lightCopy(s)
				newStateObject.SetCode(codeHash, code)
				s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
				return
			}
		}
		stateObject.SetCode(codeHash, code)
	}
}

func (s *StateDB) SetState(addr common.Address, key, value common.Hash) {
	stateObject := s.GetOrNewStateObject(addr) // attention: if StateObject's lightCopy, its storage is only a part of the full storage,
	if stateObject != nil {
		if s.parallel.isSlotDB {
			if s.parallel.baseTxIndex+1 == s.txIndex {
				// we check if state is unchanged
				// only when current transaction is the next transaction to be committed
				// fixme: there is a bug, block: 14,962,284,
				//        stateObject is in dirty (light copy), but the key is in mainStateDB
				//        stateObject dirty -> committed, will skip mainStateDB dirty
				if s.GetState(addr, key) == value {
					log.Debug("Skip set same state", "baseTxIndex", s.parallel.baseTxIndex,
						"txIndex", s.txIndex, "addr", addr,
						"key", key, "value", value)
					return
				}
			}

			if s.parallel.kvChangesInSlot[addr] == nil {
				s.parallel.kvChangesInSlot[addr] = make(StateKeys) // make(Storage, defaultNumOfSlots)
			}

			if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
				newStateObject := stateObject.lightCopy(s)
				newStateObject.SetState(s.db, key, value)
				s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
				return
			}
		}
		stateObject.SetState(s.db, key, value)
	}
}

// SetStorage replaces the entire storage for the specified account with given
// storage. This function should only be used for debugging.
func (s *StateDB) SetStorage(addr common.Address, storage map[common.Hash]common.Hash) {
	stateObject := s.GetOrNewStateObject(addr) // fixme: parallel mode?
	if stateObject != nil {
		stateObject.SetStorage(storage)
	}
}

// Suicide marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (s *StateDB) Suicide(addr common.Address) bool {
	var stateObject *StateObject
	if s.parallel.isSlotDB {
		// 1.Try to get from dirty, it could be suicided inside of contract call
		stateObject = s.parallel.dirtiedStateObjectsInSlot[addr]
		// 2.Try to get from uncomfirmed, if deleted return false, since the address does not exist
		if stateObject == nil {
			if deleted, ok := s.getAddrStateFromUnconfirmedDB(addr); ok {
				if deleted {
					return false
				}
			}
		}
	}
	// 3.Try to get from main StateDB
	if stateObject == nil {
		stateObject = s.getStateObjectNoSlot(addr)
	}
	if stateObject == nil {
		return false
	}

	s.journal.append(suicideChange{
		account:     &addr,
		prev:        stateObject.suicided, // todo: must be false?
		prevbalance: new(big.Int).Set(stateObject.Balance()),
	})

	if s.parallel.isSlotDB {
		if _, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; !ok {
			// do copy-on-write for suicide "write"
			newStateObject := stateObject.lightCopy(s)
			newStateObject.markSuicided()
			newStateObject.data.Balance = new(big.Int)
			s.parallel.dirtiedStateObjectsInSlot[addr] = newStateObject
			s.parallel.addrStateChangesInSlot[addr] = false // false: the address does not exist any more,
			s.parallel.nonceChangesInSlot[addr] = struct{}{}
			s.parallel.balanceChangesInSlot[addr] = struct{}{}
			s.parallel.codeChangesInSlot[addr] = struct{}{}
			// s.parallel.kvChangesInSlot[addr] = make(StateKeys) // all key changes are discarded
			return true
		}
	}

	stateObject.markSuicided()
	stateObject.data.Balance = new(big.Int)
	return true
}

//
// Setting, updating & deleting state object methods.
//

// updateStateObject writes the given object to the trie.
func (s *StateDB) updateStateObject(obj *StateObject) {
	// Track the amount of time wasted on updating the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Encode the account and update the account trie
	addr := obj.Address()
	data := obj.encodeData
	var err error
	if data == nil {
		data, err = rlp.EncodeToBytes(obj)
		if err != nil {
			panic(fmt.Errorf("can't encode object at %x: %v", addr[:], err))
		}
	}
	if err = s.trie.TryUpdate(addr[:], data); err != nil {
		s.setError(fmt.Errorf("updateStateObject (%x) error: %v", addr[:], err))
	}
}

// deleteStateObject removes the given object from the state trie.
func (s *StateDB) deleteStateObject(obj *StateObject) {
	// Track the amount of time wasted on deleting the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Delete the account from the trie
	addr := obj.Address()
	if err := s.trie.TryDelete(addr[:]); err != nil {
		s.setError(fmt.Errorf("deleteStateObject (%x) error: %v", addr[:], err))
	}
}

// getStateObject retrieves a state object given by the address, returning nil if
// the object is not found or was deleted in this execution context. If you need
// to differentiate between non-existent/just-deleted, use getDeletedStateObject.
// fixme: avoid getStateObjectNoSlot, may be we define a new struct SlotDB which inherit StateDB
func (s *StateDB) getStateObjectNoSlot(addr common.Address) *StateObject {
	if obj := s.getDeletedStateObject(addr); obj != nil && !obj.deleted {
		return obj
	}
	return nil
}

// for parallel execution mode, try to get dirty StateObject in slot first.
func (s *StateDB) getStateObject(addr common.Address) *StateObject {
	if s.parallel.isSlotDB {
		if obj, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
			return obj
		}
	}

	if obj := s.getDeletedStateObject(addr); obj != nil && !obj.deleted {
		return obj
	}
	return nil
}

// getDeletedStateObject is similar to getStateObject, but instead of returning
// nil for a deleted state object, it returns the actual object with the deleted
// flag set. This is needed by the state journal to revert to the correct s-
// destructed object instead of wiping all knowledge about the state object.
func (s *StateDB) getDeletedStateObject(addr common.Address) *StateObject {
	// Prefer live objects if any is available
	if obj, _ := s.getStateObjectFromStateObjects(addr); obj != nil {
		return obj
	}
	// If no live objects are available, attempt to use snapshots
	var (
		data *Account
		err  error
	)
	if s.snap != nil {
		if metrics.EnabledExpensive {
			defer func(start time.Time) { s.SnapshotAccountReads += time.Since(start) }(time.Now())
		}
		var acc *snapshot.Account
		if acc, err = s.snap.Account(crypto.HashData(s.hasher, addr.Bytes())); err == nil {
			if acc == nil {
				return nil
			}
			data = &Account{
				Nonce:    acc.Nonce,
				Balance:  acc.Balance,
				CodeHash: acc.CodeHash,
				Root:     common.BytesToHash(acc.Root),
			}
			if len(data.CodeHash) == 0 {
				data.CodeHash = emptyCodeHash
			}
			if data.Root == (common.Hash{}) {
				data.Root = emptyRoot
			}
		}
	}
	// If snapshot unavailable or reading from it failed, load from the database
	if s.snap == nil || err != nil {
		if s.trie == nil {
			tr, err := s.db.OpenTrie(s.originalRoot)
			if err != nil {
				s.setError(fmt.Errorf("failed to open trie tree"))
				return nil
			}
			s.trie = tr
		}
		if metrics.EnabledExpensive {
			defer func(start time.Time) { s.AccountReads += time.Since(start) }(time.Now())
		}
		enc, err := s.trie.TryGet(addr.Bytes())
		if err != nil {
			s.setError(fmt.Errorf("getDeleteStateObject (%x) error: %v", addr.Bytes(), err))
			return nil
		}
		if len(enc) == 0 {
			return nil
		}
		data = new(Account)
		if err := rlp.DecodeBytes(enc, data); err != nil {
			log.Error("Failed to decode state object", "addr", addr, "err", err)
			return nil
		}
	}
	// Insert into the live set
	obj := newObject(s, s.isParallel, addr, *data)
	s.SetStateObject(obj)
	return obj
}

func (s *StateDB) SetStateObject(object *StateObject) {
	s.storeStateObj(object.Address(), object)
}

// GetOrNewStateObject retrieves a state object or create a new state object if nil.
// dirtyInSlot -> Unconfirmed DB -> main DB -> snapshot, no? create one
func (s *StateDB) GetOrNewStateObject(addr common.Address) *StateObject {
	var stateObject *StateObject = nil
	exist := true
	if s.parallel.isSlotDB {
		if stateObject, ok := s.parallel.dirtiedStateObjectsInSlot[addr]; ok {
			return stateObject
		}
		stateObject, _ = s.getStateObjectFromUnconfirmedDB(addr)
	}

	if stateObject == nil {
		stateObject = s.getStateObjectNoSlot(addr)
	}
	if stateObject == nil || stateObject.deleted {
		stateObject = s.createObject(addr)
		exist = false
	}

	if s.parallel.isSlotDB {
		s.parallel.addrStateReadsInSlot[addr] = exist // true: exist, false: not exist
	}
	return stateObject
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten and returned as the second return value.

// prev is used for CreateAccount to get its balance
// Parallel mode:
// if prev in dirty:  revert is ok
// if prev in unconfirmed DB:  addr state read record, revert should not put it back
// if prev in main DB:  addr state read record, revert should not put it back
// if pre no exist:  addr state read record,

// `prev` is used to handle revert, to recover with the `prev` object
// In Parallel mode, we only need to recover to `prev` in SlotDB,
//  a.if it is not in SlotDB, `revert` will remove it from the SlotDB
//  b.if it is exist in SlotDB, `revert` will recover to the `prev` in SlotDB
//  c.as `snapDestructs` it is the same
func (s *StateDB) createObject(addr common.Address) (newobj *StateObject) {
	var prev *StateObject = nil
	if s.parallel.isSlotDB {
		// do not get from unconfirmed DB, since it will has problem on revert
		prev = s.parallel.dirtiedStateObjectsInSlot[addr]
	} else {
		prev = s.getDeletedStateObject(addr) // Note, prev might have been deleted, we need that!
	}

	var prevdestruct bool

	if s.snap != nil && prev != nil {
		_, prevdestruct = s.snapDestructs[prev.address]
		if !prevdestruct {
			// To destroy the previous trie node first and update the trie tree
			// with the new object on block commit.
			s.snapDestructs[prev.address] = struct{}{}
		}
	}
	newobj = newObject(s, s.isParallel, addr, Account{})
	newobj.setNonce(0) // sets the object to dirty
	if prev == nil {
		s.journal.append(createObjectChange{account: &addr})
	} else {
		s.journal.append(resetObjectChange{prev: prev, prevdestruct: prevdestruct})
	}

	if s.parallel.isSlotDB {
		s.parallel.dirtiedStateObjectsInSlot[addr] = newobj
		s.parallel.addrStateChangesInSlot[addr] = true // the object is created
		s.parallel.nonceChangesInSlot[addr] = struct{}{}
		s.parallel.balanceChangesInSlot[addr] = struct{}{}
		s.parallel.codeChangesInSlot[addr] = struct{}{}
		// notice: all the KVs are cleared if any
		s.parallel.kvChangesInSlot[addr] = make(StateKeys)
	} else {
		s.SetStateObject(newobj)
	}
	return newobj
}

// CreateAccount explicitly creates a state object. If a state object with the address
// already exists the balance is carried over to the new account.
//
// CreateAccount is called during the EVM CREATE operation. The situation might arise that
// a contract does the following:
//
//   1. sends funds to sha(account ++ (nonce + 1))
//   2. tx_create(sha(account ++ nonce)) (note that this gets the address of 1)
//
// Carrying over the balance ensures that Ether doesn't disappear.
func (s *StateDB) CreateAccount(addr common.Address) {
	// no matter it is got from dirty, unconfirmed or main DB
	// if addr not exist, preBalance will be common.Big0, it is same as new(big.Int) which
	// is the value newObject(),
	preBalance := s.GetBalance(addr) // parallel balance read will be recorded inside of GetBalance
	newObj := s.createObject(addr)
	newObj.setBalance(new(big.Int).Set(preBalance)) // new big.Int for newObj
}

func (s *StateDB) ForEachStorage(addr common.Address, cb func(key, value common.Hash) bool) error {
	so := s.getStateObject(addr)
	if so == nil {
		return nil
	}
	it := trie.NewIterator(so.getTrie(s.db).NodeIterator(nil))

	for it.Next() {
		key := common.BytesToHash(s.trie.GetKey(it.Key))
		if value, dirty := so.dirtyStorage.GetValue(key); dirty {
			if !cb(key, value) {
				return nil
			}
			continue
		}

		if len(it.Value) > 0 {
			_, content, _, err := rlp.Split(it.Value)
			if err != nil {
				return err
			}
			if !cb(key, common.BytesToHash(content)) {
				return nil
			}
		}
	}
	return nil
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (s *StateDB) Copy() *StateDB {
	// Copy all the basic fields, initialize the memory ones
	state := &StateDB{
		db:                  s.db,
		trie:                s.db.CopyTrie(s.trie),
		stateObjects:        make(map[common.Address]*StateObject, len(s.journal.dirties)),
		stateObjectsPending: make(map[common.Address]struct{}, len(s.stateObjectsPending)),
		stateObjectsDirty:   make(map[common.Address]struct{}, len(s.journal.dirties)),
		storagePool:         s.storagePool,
		refund:              s.refund,
		logs:                make(map[common.Hash][]*types.Log, len(s.logs)),
		logSize:             s.logSize,
		preimages:           make(map[common.Hash][]byte, len(s.preimages)),
		journal:             newJournal(),
		hasher:              crypto.NewKeccakState(),
		parallel:            ParallelState{},
	}
	// Copy the dirty states, logs, and preimages
	for addr := range s.journal.dirties {
		// As documented [here](https://github.com/ethereum/go-ethereum/pull/16485#issuecomment-380438527),
		// and in the Finalise-method, there is a case where an object is in the journal but not
		// in the stateObjects: OOG after touch on ripeMD prior to Byzantium. Thus, we need to check for
		// nil
		if object, exist := s.getStateObjectFromStateObjects(addr); exist {
			// Even though the original object is dirty, we are not copying the journal,
			// so we need to make sure that anyside effect the journal would have caused
			// during a commit (or similar op) is already applied to the copy.
			state.storeStateObj(addr, object.deepCopy(state))

			state.stateObjectsDirty[addr] = struct{}{}   // Mark the copy dirty to force internal (code/state) commits
			state.stateObjectsPending[addr] = struct{}{} // Mark the copy pending to force external (account) commits
		}
	}
	// Above, we don't copy the actual journal. This means that if the copy is copied, the
	// loop above will be a no-op, since the copy's journal is empty.
	// Thus, here we iterate over stateObjects, to enable copies of copies
	for addr := range s.stateObjectsPending {
		if _, exist := state.getStateObjectFromStateObjects(addr); !exist {
			object, _ := s.getStateObjectFromStateObjects(addr)
			state.storeStateObj(addr, object.deepCopy(state))
		}
		state.stateObjectsPending[addr] = struct{}{}
	}
	for addr := range s.stateObjectsDirty {
		if _, exist := state.getStateObjectFromStateObjects(addr); !exist {
			object, _ := s.getStateObjectFromStateObjects(addr)
			state.storeStateObj(addr, object.deepCopy(state))
		}
		state.stateObjectsDirty[addr] = struct{}{}
	}
	for hash, logs := range s.logs {
		cpy := make([]*types.Log, len(logs))
		for i, l := range logs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		state.logs[hash] = cpy
	}
	for hash, preimage := range s.preimages {
		state.preimages[hash] = preimage
	}
	// Do we need to copy the access list? In practice: No. At the start of a
	// transaction, the access list is empty. In practice, we only ever copy state
	// _between_ transactions/blocks, never in the middle of a transaction.
	// However, it doesn't cost us much to copy an empty list, so we do it anyway
	// to not blow up if we ever decide copy it in the middle of a transaction
	if s.accessList != nil {
		state.accessList = s.accessList.Copy()
	}

	// If there's a prefetcher running, make an inactive copy of it that can
	// only access data but does not actively preload (since the user will not
	// know that they need to explicitly terminate an active copy).
	if s.prefetcher != nil {
		state.prefetcher = s.prefetcher.copy()
	}
	if s.snaps != nil {
		// In order for the miner to be able to use and make additions
		// to the snapshot tree, we need to copy that aswell.
		// Otherwise, any block mined by ourselves will cause gaps in the tree,
		// and force the miner to operate trie-backed only
		state.snaps = s.snaps
		state.snap = s.snap
		// deep copy needed
		state.snapDestructs = make(map[common.Address]struct{})
		for k, v := range s.snapDestructs {
			state.snapDestructs[k] = v
		}
		state.snapAccounts = make(map[common.Address][]byte)
		for k, v := range s.snapAccounts {
			state.snapAccounts[k] = v
		}
		state.snapStorage = make(map[common.Address]map[string][]byte)
		for k, v := range s.snapStorage {
			temp := make(map[string][]byte)
			for kk, vv := range v {
				temp[kk] = vv
			}
			state.snapStorage[k] = temp
		}
	}
	return state
}

var addressStructPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]struct{}, defaultNumOfSlots) },
}

var journalPool = sync.Pool{
	New: func() interface{} {
		return &journal{
			dirties: make(map[common.Address]int, defaultNumOfSlots),
			entries: make([]journalEntry, 0, defaultNumOfSlots),
		}
	},
}

var stateKeysPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]StateKeys, defaultNumOfSlots) },
}

var stateObjectsPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]*StateObject, defaultNumOfSlots) },
}

var balancePool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]*big.Int, defaultNumOfSlots) },
}

var snapAccountPool = sync.Pool{
	New: func() interface{} { return make(map[common.Address][]byte, defaultNumOfSlots) },
}

var snapStoragePool = sync.Pool{
	New: func() interface{} { return make(map[common.Address]map[string][]byte, defaultNumOfSlots) },
}

var snapStorageValuePool = sync.Pool{
	New: func() interface{} { return make(map[string][]byte, defaultNumOfSlots) },
}

var logsPool = sync.Pool{
	New: func() interface{} { return make(map[common.Hash][]*types.Log, defaultNumOfSlots) },
}

func (s *StateDB) SlotDBPutSyncPool() {
	for key := range s.parallel.codeReadsInSlot {
		delete(s.parallel.codeReadsInSlot, key)
	}
	addressStructPool.Put(s.parallel.codeReadsInSlot)

	// for key := range s.parallel.codeChangesInSlot {
	//	delete(s.parallel.codeChangesInSlot, key)
	// }
	// addressStructPool.Put(s.parallel.codeChangesInSlot)

	for key := range s.parallel.balanceChangesInSlot {
		delete(s.parallel.balanceChangesInSlot, key)
	}
	balancePool.Put(s.parallel.balanceChangesInSlot)

	for key := range s.parallel.balanceReadsInSlot {
		delete(s.parallel.balanceReadsInSlot, key)
	}
	addressStructPool.Put(s.parallel.balanceReadsInSlot)

	for key := range s.parallel.addrStateReadsInSlot {
		delete(s.parallel.addrStateReadsInSlot, key)
	}
	addressStructPool.Put(s.parallel.addrStateReadsInSlot)

	for key := range s.parallel.nonceChangesInSlot {
		delete(s.parallel.nonceChangesInSlot, key)
	}
	addressStructPool.Put(s.parallel.nonceChangesInSlot)

	for key := range s.stateObjectsPending {
		delete(s.stateObjectsPending, key)
	}
	addressStructPool.Put(s.stateObjectsPending)

	for key := range s.stateObjectsDirty {
		delete(s.stateObjectsDirty, key)
	}
	addressStructPool.Put(s.stateObjectsDirty)

	for key := range s.journal.dirties {
		delete(s.journal.dirties, key)
	}
	s.journal.entries = s.journal.entries[:0]
	journalPool.Put(s.journal)

	// for key := range s.parallel.kvChangesInSlot {
	//	delete(s.parallel.kvChangesInSlot, key)
	//}
	//stateKeysPool.Put(s.parallel.kvChangesInSlot)

	for key := range s.parallel.kvReadsInSlot {
		delete(s.parallel.kvReadsInSlot, key)
	}
	stateKeysPool.Put(s.parallel.kvReadsInSlot)

	for key := range s.parallel.dirtiedStateObjectsInSlot {
		delete(s.parallel.dirtiedStateObjectsInSlot, key)
	}
	stateObjectsPool.Put(s.parallel.dirtiedStateObjectsInSlot)

	for key := range s.snapDestructs {
		delete(s.snapDestructs, key)
	}
	addressStructPool.Put(s.snapDestructs)

	for key := range s.snapAccounts {
		delete(s.snapAccounts, key)
	}
	snapAccountPool.Put(s.snapAccounts)

	for key, storage := range s.snapStorage {
		for key := range storage {
			delete(storage, key)
		}
		snapStorageValuePool.Put(storage)
		delete(s.snapStorage, key)
	}
	snapStoragePool.Put(s.snapStorage)

	for key := range s.logs {
		delete(s.logs, key)
	}
	logsPool.Put(s.logs)
}

// CopyForSlot copy all the basic fields, initialize the memory ones
func (s *StateDB) CopyForSlot() *StateDB {
	parallel := ParallelState{
		// use base(dispatcher) slot db's stateObjects.
		// It is a SyncMap, only readable to slot, not writable
		stateObjects:        s.parallel.stateObjects,
		unconfirmedDBInShot: make(map[int]*StateDB, 100),

		codeReadsInSlot:        make(map[common.Address][]byte, 10), // addressStructPool.Get().(map[common.Address]struct{}),
		codeHashReadsInSlot:    make(map[common.Address]common.Hash),
		codeChangesInSlot:      make(map[common.Address]struct{}),
		kvChangesInSlot:        make(map[common.Address]StateKeys),    // stateKeysPool.Get().(map[common.Address]StateKeys),
		kvReadsInSlot:          make(map[common.Address]Storage, 100), // stateKeysPool.Get().(map[common.Address]Storage),
		balanceChangesInSlot:   make(map[common.Address]struct{}, 10), // balancePool.Get().(map[common.Address]struct{}, 10),
		balanceReadsInSlot:     make(map[common.Address]*big.Int),     // addressStructPool.Get().(map[common.Address]struct{}),
		addrStateReadsInSlot:   make(map[common.Address]bool),         // addressStructPool.Get().(map[common.Address]struct{}),
		addrStateChangesInSlot: make(map[common.Address]bool),         // addressStructPool.Get().(map[common.Address]struct{}),
		nonceChangesInSlot:     make(map[common.Address]struct{}),     // addressStructPool.Get().(map[common.Address]struct{}),
		nonceReadsInSlot:       make(map[common.Address]uint64),

		isSlotDB:                  true,
		dirtiedStateObjectsInSlot: stateObjectsPool.Get().(map[common.Address]*StateObject),
	}
	state := &StateDB{
		db:                  s.db,
		trie:                s.db.CopyTrie(s.trie),
		stateObjects:        make(map[common.Address]*StateObject), // replaced by parallel.stateObjects in parallel mode
		stateObjectsPending: make(map[common.Address]struct{}),     // addressStructPool.Get().(map[common.Address]struct{}),
		stateObjectsDirty:   make(map[common.Address]struct{}),     // addressStructPool.Get().(map[common.Address]struct{}),
		refund:              s.refund,                              // should be 0
		logs:                logsPool.Get().(map[common.Hash][]*types.Log),
		logSize:             0,
		preimages:           make(map[common.Hash][]byte, len(s.preimages)),
		journal:             journalPool.Get().(*journal),
		hasher:              crypto.NewKeccakState(),
		isParallel:          true,
		parallel:            parallel,
	}

	for hash, preimage := range s.preimages {
		state.preimages[hash] = preimage
	}

	if s.snaps != nil {
		// In order for the miner to be able to use and make additions
		// to the snapshot tree, we need to copy that aswell.
		// Otherwise, any block mined by ourselves will cause gaps in the tree,
		// and force the miner to operate trie-backed only
		state.snaps = s.snaps
		state.snap = s.snap
		// deep copy needed
		state.snapDestructs = make(map[common.Address]struct{}) // addressStructPool.Get().(map[common.Address]struct{})
		s.snapParallelLock.RLock()
		for k, v := range s.snapDestructs {
			state.snapDestructs[k] = v
		}
		s.snapParallelLock.RUnlock()
		//
		state.snapAccounts = snapAccountPool.Get().(map[common.Address][]byte)
		for k, v := range s.snapAccounts {
			state.snapAccounts[k] = v
		}
		state.snapStorage = snapStoragePool.Get().(map[common.Address]map[string][]byte)
		for k, v := range s.snapStorage {
			temp := snapStorageValuePool.Get().(map[string][]byte)
			for kk, vv := range v {
				temp[kk] = vv
			}
			state.snapStorage[k] = temp
		}
		// trie prefetch should be done by dispacther on StateObject Merge,
		// disable it in parallel slot
		// state.prefetcher = s.prefetcher
	}
	return state
}

// Snapshot returns an identifier for the current revision of the state.
func (s *StateDB) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (s *StateDB) RevertToSnapshot(revid int) {
	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(s.validRevisions), func(i int) bool {
		return s.validRevisions[i].id >= revid
	})
	if idx == len(s.validRevisions) || s.validRevisions[idx].id != revid {
		panic(fmt.Errorf("revision id %v cannot be reverted", revid))
	}
	snapshot := s.validRevisions[idx].journalIndex

	// Replay the journal to undo changes and remove invalidated snapshots
	s.journal.revert(s, snapshot)
	s.validRevisions = s.validRevisions[:idx]
}

// GetRefund returns the current value of the refund counter.
func (s *StateDB) GetRefund() uint64 {
	return s.refund
}

// GetRefund returns the current value of the refund counter.
func (s *StateDB) WaitPipeVerification() error {
	// We need wait for the parent trie to commit
	if s.snap != nil {
		if valid := s.snap.WaitAndGetVerifyRes(); !valid {
			return fmt.Errorf("verification on parent snap failed")
		}
	}
	return nil
}

// Finalise finalises the state by removing the s destructed objects and clears
// the journal as well as the refunds. Finalise, however, will not push any updates
// into the tries just yet. Only IntermediateRoot or Commit will do that.
func (s *StateDB) Finalise(deleteEmptyObjects bool) { // fixme: concurrent safe...
	addressesToPrefetch := make([][]byte, 0, len(s.journal.dirties))
	for addr := range s.journal.dirties {
		var obj *StateObject
		var exist bool
		if s.parallel.isSlotDB {
			obj = s.parallel.dirtiedStateObjectsInSlot[addr]
			if obj != nil {
				exist = true
			} else {
				log.Error("StateDB Finalise dirty addr not in dirtiedStateObjectsInSlot",
					"addr", addr)
			}
		} else {
			obj, exist = s.getStateObjectFromStateObjects(addr)
		}
		if !exist {
			// ripeMD is 'touched' at block 1714175, in tx 0x1237f737031e40bcde4a8b7e717b2d15e3ecadfe49bb1bbc71ee9deb09c6fcf2
			// That tx goes out of gas, and although the notion of 'touched' does not exist there, the
			// touch-event will still be recorded in the journal. Since ripeMD is a special snowflake,
			// it will persist in the journal even though the journal is reverted. In this special circumstance,
			// it may exist in `s.journal.dirties` but not in `s.stateObjects`.
			// Thus, we can safely ignore it here
			continue
		}
		if obj.suicided || (deleteEmptyObjects && obj.empty()) {
			if s.parallel.isSlotDB {
				s.parallel.addrStateChangesInSlot[addr] = false // false: deleted
			}
			obj.deleted = true

			// If state snapshotting is active, also mark the destruction there.
			// Note, we can't do this only at the end of a block because multiple
			// transactions within the same block might self destruct and then
			// ressurrect an account; but the snapshotter needs both events.
			if s.snap != nil {
				s.snapDestructs[obj.address] = struct{}{} // We need to maintain account deletions explicitly (will remain set indefinitely)
				delete(s.snapAccounts, obj.address)       // Clear out any previously updated account data (may be recreated via a ressurrect)
				delete(s.snapStorage, obj.address)        // Clear out any previously updated storage data (may be recreated via a ressurrect)
			}
		} else {
			// 1.none parallel mode, we do obj.finalise(true) as normal
			// 2.with parallel mode, we do obj.finalise(true) on dispatcher, not on slot routine
			//   obj.finalise(true) will clear its dirtyStorage, will make prefetch broken.
			if !s.isParallel || !s.parallel.isSlotDB {
				obj.finalise(true) // Prefetch slots in the background
			}
		}
		if _, exist := s.stateObjectsPending[addr]; !exist {
			s.stateObjectsPending[addr] = struct{}{}
		}
		if _, exist := s.stateObjectsDirty[addr]; !exist {
			s.stateObjectsDirty[addr] = struct{}{}
			// At this point, also ship the address off to the precacher. The precacher
			// will start loading tries, and when the change is eventually committed,
			// the commit-phase will be a lot faster
			addressesToPrefetch = append(addressesToPrefetch, common.CopyBytes(addr[:])) // Copy needed for closure
		}
	}
	if s.prefetcher != nil && len(addressesToPrefetch) > 0 {
		s.prefetcher.prefetch(s.originalRoot, addressesToPrefetch, emptyAddr)
	}
	// Invalidate journal because reverting across transactions is not allowed.
	s.clearJournalAndRefund()
}

// IntermediateRoot computes the current root hash of the state trie.
// It is called in between transactions to get the root hash that
// goes into transaction receipts.
func (s *StateDB) IntermediateRoot(deleteEmptyObjects bool) common.Hash {
	if s.lightProcessed {
		s.StopPrefetcher()
		return s.trie.Hash()
	}
	// Finalise all the dirty storage states and write them into the tries
	s.Finalise(deleteEmptyObjects)
	s.AccountsIntermediateRoot()
	return s.StateIntermediateRoot()
}

func (s *StateDB) AccountsIntermediateRoot() {
	tasks := make(chan func())
	finishCh := make(chan struct{})
	defer close(finishCh)
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				select {
				case task := <-tasks:
					task()
				case <-finishCh:
					return
				}
			}
		}()
	}

	// Although naively it makes sense to retrieve the account trie and then do
	// the contract storage and account updates sequentially, that short circuits
	// the account prefetcher. Instead, let's process all the storage updates
	// first, giving the account prefeches just a few more milliseconds of time
	// to pull useful data from disk.
	for addr := range s.stateObjectsPending {
		if obj, _ := s.getStateObjectFromStateObjects(addr); !obj.deleted {
			wg.Add(1)
			tasks <- func() {
				obj.updateRoot(s.db)
				// If state snapshotting is active, cache the data til commit. Note, this
				// update mechanism is not symmetric to the deletion, because whereas it is
				// enough to track account updates at commit time, deletions need tracking
				// at transaction boundary level to ensure we capture state clearing.
				if s.snap != nil && !obj.deleted {
					s.snapMux.Lock()
					// It is possible to add unnecessary change, but it is fine.
					s.snapAccounts[obj.address] = snapshot.SlimAccountRLP(obj.data.Nonce, obj.data.Balance, obj.data.Root, obj.data.CodeHash)
					s.snapMux.Unlock()
				}
				data, err := rlp.EncodeToBytes(obj)
				if err != nil {
					panic(fmt.Errorf("can't encode object at %x: %v", addr[:], err))
				}
				obj.encodeData = data
				wg.Done()
			}
		}
	}
	wg.Wait()
}

func (s *StateDB) StateIntermediateRoot() common.Hash {
	// If there was a trie prefetcher operating, it gets aborted and irrevocably
	// modified after we start retrieving tries. Remove it from the statedb after
	// this round of use.
	//
	// This is weird pre-byzantium since the first tx runs with a prefetcher and
	// the remainder without, but pre-byzantium even the initial prefetcher is
	// useless, so no sleep lost.
	prefetcher := s.prefetcher
	defer func() {
		s.prefetcherLock.Lock()
		if s.prefetcher != nil {
			s.prefetcher.close()
			s.prefetcher = nil
		}
		// try not use defer inside defer
		s.prefetcherLock.Unlock()
	}()

	// Now we're about to start to write changes to the trie. The trie is so far
	// _untouched_. We can check with the prefetcher, if it can give us a trie
	// which has the same root, but also has some content loaded into it.
	if prefetcher != nil {
		if trie := prefetcher.trie(s.originalRoot); trie != nil {
			s.trie = trie
		}
	}
	if s.trie == nil {
		tr, err := s.db.OpenTrie(s.originalRoot)
		if err != nil {
			panic(fmt.Sprintf("Failed to open trie tree %s", s.originalRoot))
		}
		s.trie = tr
	}
	usedAddrs := make([][]byte, 0, len(s.stateObjectsPending))
	for addr := range s.stateObjectsPending {
		if obj, _ := s.getStateObjectFromStateObjects(addr); obj.deleted {
			s.deleteStateObject(obj)
		} else {
			s.updateStateObject(obj)
		}
		usedAddrs = append(usedAddrs, common.CopyBytes(addr[:])) // Copy needed for closure
	}
	if prefetcher != nil {
		prefetcher.used(s.originalRoot, usedAddrs)
	}
	if len(s.stateObjectsPending) > 0 {
		s.stateObjectsPending = make(map[common.Address]struct{})
	}
	// Track the amount of time wasted on hashing the account trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountHashes += time.Since(start) }(time.Now())
	}
	root := s.trie.Hash()
	return root
}

// Prepare sets the current transaction hash and index and block hash which is
// used when the EVM emits new state logs.
func (s *StateDB) Prepare(thash, bhash common.Hash, ti int) {
	s.thash = thash
	s.bhash = bhash
	s.txIndex = ti
	s.accessList = nil
}

func (s *StateDB) clearJournalAndRefund() {
	if len(s.journal.entries) > 0 {
		s.journal = newJournal()
		s.refund = 0
	}
	s.validRevisions = s.validRevisions[:0] // Snapshots can be created without journal entires
}

func (s *StateDB) LightCommit() (common.Hash, *types.DiffLayer, error) {
	codeWriter := s.db.TrieDB().DiskDB().NewBatch()

	// light process already verified it, expectedRoot is trustworthy.
	root := s.expectedRoot

	commitFuncs := []func() error{
		func() error {
			for codeHash, code := range s.diffCode {
				rawdb.WriteCode(codeWriter, codeHash, code)
				if codeWriter.ValueSize() >= ethdb.IdealBatchSize {
					if err := codeWriter.Write(); err != nil {
						return err
					}
					codeWriter.Reset()
				}
			}
			if codeWriter.ValueSize() > 0 {
				if err := codeWriter.Write(); err != nil {
					return err
				}
			}
			return nil
		},
		func() error {
			tasks := make(chan func())
			taskResults := make(chan error, len(s.diffTries))
			tasksNum := 0
			finishCh := make(chan struct{})
			defer close(finishCh)
			threads := gopool.Threads(len(s.diffTries))

			for i := 0; i < threads; i++ {
				go func() {
					for {
						select {
						case task := <-tasks:
							task()
						case <-finishCh:
							return
						}
					}
				}()
			}

			for account, diff := range s.diffTries {
				tmpAccount := account
				tmpDiff := diff
				tasks <- func() {
					root, err := tmpDiff.Commit(nil)
					if err != nil {
						taskResults <- err
						return
					}
					s.db.CacheStorage(crypto.Keccak256Hash(tmpAccount[:]), root, tmpDiff)
					taskResults <- nil
				}
				tasksNum++
			}

			for i := 0; i < tasksNum; i++ {
				err := <-taskResults
				if err != nil {
					return err
				}
			}

			// commit account trie
			var account Account
			root, err := s.trie.Commit(func(_ [][]byte, _ []byte, leaf []byte, parent common.Hash) error {
				if err := rlp.DecodeBytes(leaf, &account); err != nil {
					return nil
				}
				if account.Root != emptyRoot {
					s.db.TrieDB().Reference(account.Root, parent)
				}
				return nil
			})
			if err != nil {
				return err
			}
			if root != emptyRoot {
				s.db.CacheAccount(root, s.trie)
			}
			return nil
		},
		func() error {
			if s.snap != nil {
				if metrics.EnabledExpensive {
					defer func(start time.Time) { s.SnapshotCommits += time.Since(start) }(time.Now())
				}
				// Only update if there's a state transition (skip empty Clique blocks)
				if parent := s.snap.Root(); parent != root {
					// for light commit, always do sync commit
					if err := s.snaps.Update(root, parent, s.snapDestructs, s.snapAccounts, s.snapStorage, nil); err != nil {
						log.Warn("Failed to update snapshot tree", "from", parent, "to", root, "err", err)
					}
					// Keep n diff layers in the memory
					// - head layer is paired with HEAD state
					// - head-1 layer is paired with HEAD-1 state
					// - head-(n-1) layer(bottom-most diff layer) is paired with HEAD-(n-1)state
					if err := s.snaps.Cap(root, s.snaps.CapLimit()); err != nil {
						log.Warn("Failed to cap snapshot tree", "root", root, "layers", s.snaps.CapLimit(), "err", err)
					}
				}
			}
			return nil
		},
	}
	commitRes := make(chan error, len(commitFuncs))
	for _, f := range commitFuncs {
		tmpFunc := f
		go func() {
			commitRes <- tmpFunc()
		}()
	}
	for i := 0; i < len(commitFuncs); i++ {
		r := <-commitRes
		if r != nil {
			return common.Hash{}, nil, r
		}
	}
	s.snap, s.snapDestructs, s.snapAccounts, s.snapStorage = nil, nil, nil, nil
	s.diffTries, s.diffCode = nil, nil
	return root, s.diffLayer, nil
}

// Commit writes the state to the underlying in-memory trie database.
func (s *StateDB) Commit(failPostCommitFunc func(), postCommitFuncs ...func() error) (common.Hash, *types.DiffLayer, error) {
	if s.dbErr != nil {
		return common.Hash{}, nil, fmt.Errorf("commit aborted due to earlier error: %v", s.dbErr)
	}
	// Finalize any pending changes and merge everything into the tries
	if s.lightProcessed {
		root, diff, err := s.LightCommit()
		if err != nil {
			return root, diff, err
		}
		for _, postFunc := range postCommitFuncs {
			err = postFunc()
			if err != nil {
				return root, diff, err
			}
		}
		return root, diff, nil
	}
	var diffLayer *types.DiffLayer
	var verified chan struct{}
	var snapUpdated chan struct{}
	if s.snap != nil {
		diffLayer = &types.DiffLayer{}
	}
	if s.pipeCommit {
		// async commit the MPT
		verified = make(chan struct{})
		snapUpdated = make(chan struct{})
	}

	commmitTrie := func() error {
		commitErr := func() error {
			if s.stateRoot = s.StateIntermediateRoot(); s.fullProcessed && s.expectedRoot != s.stateRoot {
				return fmt.Errorf("invalid merkle root (remote: %x local: %x)", s.expectedRoot, s.stateRoot)
			}
			tasks := make(chan func())
			taskResults := make(chan error, len(s.stateObjectsDirty))
			tasksNum := 0
			finishCh := make(chan struct{})

			threads := gopool.Threads(len(s.stateObjectsDirty))
			wg := sync.WaitGroup{}
			for i := 0; i < threads; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case task := <-tasks:
							task()
						case <-finishCh:
							return
						}
					}
				}()
			}

			if s.snap != nil {
				for addr := range s.stateObjectsDirty {
					if obj, _ := s.getStateObjectFromStateObjects(addr); !obj.deleted {
						if obj.code != nil && obj.dirtyCode {
							diffLayer.Codes = append(diffLayer.Codes, types.DiffCode{
								Hash: common.BytesToHash(obj.CodeHash()),
								Code: obj.code,
							})
						}
					}
				}
			}

			for addr := range s.stateObjectsDirty {
				if obj, _ := s.getStateObjectFromStateObjects(addr); !obj.deleted {
					// Write any contract code associated with the state object
					tasks <- func() {
						// Write any storage changes in the state object to its storage trie
						if err := obj.CommitTrie(s.db); err != nil {
							taskResults <- err
						}
						taskResults <- nil
					}
					tasksNum++
				}
			}

			for i := 0; i < tasksNum; i++ {
				err := <-taskResults
				if err != nil {
					close(finishCh)
					return err
				}
			}
			close(finishCh)

			// The onleaf func is called _serially_, so we can reuse the same account
			// for unmarshalling every time.
			var account Account
			root, err := s.trie.Commit(func(_ [][]byte, _ []byte, leaf []byte, parent common.Hash) error {
				if err := rlp.DecodeBytes(leaf, &account); err != nil {
					return nil
				}
				if account.Root != emptyRoot {
					s.db.TrieDB().Reference(account.Root, parent)
				}
				return nil
			})
			if err != nil {
				return err
			}
			if root != emptyRoot {
				s.db.CacheAccount(root, s.trie)
			}
			for _, postFunc := range postCommitFuncs {
				err = postFunc()
				if err != nil {
					return err
				}
			}
			wg.Wait()
			return nil
		}()

		if s.pipeCommit {
			if commitErr == nil {
				<-snapUpdated
				s.snaps.Snapshot(s.stateRoot).MarkValid()
			} else {
				// The blockchain will do the further rewind if write block not finish yet
				if failPostCommitFunc != nil {
					<-snapUpdated
					failPostCommitFunc()
				}
				log.Error("state verification failed", "err", commitErr)
			}
			close(verified)
		}
		return commitErr
	}

	commitFuncs := []func() error{
		func() error {
			codeWriter := s.db.TrieDB().DiskDB().NewBatch()
			for addr := range s.stateObjectsDirty {
				if obj, _ := s.getStateObjectFromStateObjects(addr); !obj.deleted {
					if obj.code != nil && obj.dirtyCode {
						rawdb.WriteCode(codeWriter, common.BytesToHash(obj.CodeHash()), obj.code)
						obj.dirtyCode = false
						if codeWriter.ValueSize() > ethdb.IdealBatchSize {
							if err := codeWriter.Write(); err != nil {
								return err
							}
							codeWriter.Reset()
						}
					}
				}
			}
			if codeWriter.ValueSize() > 0 {
				if err := codeWriter.Write(); err != nil {
					log.Crit("Failed to commit dirty codes", "error", err)
					return err
				}
			}
			return nil
		},
		func() error {
			// If snapshotting is enabled, update the snapshot tree with this new version
			if s.snap != nil {
				if metrics.EnabledExpensive {
					defer func(start time.Time) { s.SnapshotCommits += time.Since(start) }(time.Now())
				}
				if s.pipeCommit {
					defer close(snapUpdated)
				}
				// Only update if there's a state transition (skip empty Clique blocks)
				if parent := s.snap.Root(); parent != s.expectedRoot {
					if err := s.snaps.Update(s.expectedRoot, parent, s.snapDestructs, s.snapAccounts, s.snapStorage, verified); err != nil {
						log.Warn("Failed to update snapshot tree", "from", parent, "to", s.expectedRoot, "err", err)
					}
					// Keep n diff layers in the memory
					// - head layer is paired with HEAD state
					// - head-1 layer is paired with HEAD-1 state
					// - head-(n-1) layer(bottom-most diff layer) is paired with HEAD-(n-1)state
					go func() {
						if err := s.snaps.Cap(s.expectedRoot, s.snaps.CapLimit()); err != nil {
							log.Warn("Failed to cap snapshot tree", "root", s.expectedRoot, "layers", s.snaps.CapLimit(), "err", err)
						}
					}()
				}
			}
			return nil
		},
		func() error {
			if s.snap != nil {
				diffLayer.Destructs, diffLayer.Accounts, diffLayer.Storages = s.SnapToDiffLayer()
			}
			return nil
		},
	}
	if s.pipeCommit {
		go commmitTrie()
	} else {
		commitFuncs = append(commitFuncs, commmitTrie)
	}
	commitRes := make(chan error, len(commitFuncs))
	for _, f := range commitFuncs {
		tmpFunc := f
		go func() {
			commitRes <- tmpFunc()
		}()
	}
	for i := 0; i < len(commitFuncs); i++ {
		r := <-commitRes
		if r != nil {
			return common.Hash{}, nil, r
		}
	}
	root := s.stateRoot
	if s.pipeCommit {
		root = s.expectedRoot
	}

	return root, diffLayer, nil
}

func (s *StateDB) DiffLayerToSnap(diffLayer *types.DiffLayer) (map[common.Address]struct{}, map[common.Address][]byte, map[common.Address]map[string][]byte, error) {
	snapDestructs := make(map[common.Address]struct{})
	snapAccounts := make(map[common.Address][]byte)
	snapStorage := make(map[common.Address]map[string][]byte)

	for _, des := range diffLayer.Destructs {
		snapDestructs[des] = struct{}{}
	}
	for _, account := range diffLayer.Accounts {
		snapAccounts[account.Account] = account.Blob
	}
	for _, storage := range diffLayer.Storages {
		// should never happen
		if len(storage.Keys) != len(storage.Vals) {
			return nil, nil, nil, errors.New("invalid diffLayer: length of keys and values mismatch")
		}
		snapStorage[storage.Account] = make(map[string][]byte, len(storage.Keys))
		n := len(storage.Keys)
		for i := 0; i < n; i++ {
			snapStorage[storage.Account][storage.Keys[i]] = storage.Vals[i]
		}
	}
	return snapDestructs, snapAccounts, snapStorage, nil
}

func (s *StateDB) SnapToDiffLayer() ([]common.Address, []types.DiffAccount, []types.DiffStorage) {
	destructs := make([]common.Address, 0, len(s.snapDestructs))
	for account := range s.snapDestructs {
		destructs = append(destructs, account)
	}
	accounts := make([]types.DiffAccount, 0, len(s.snapAccounts))
	for accountHash, account := range s.snapAccounts {
		accounts = append(accounts, types.DiffAccount{
			Account: accountHash,
			Blob:    account,
		})
	}
	storages := make([]types.DiffStorage, 0, len(s.snapStorage))
	for accountHash, storage := range s.snapStorage {
		keys := make([]string, 0, len(storage))
		values := make([][]byte, 0, len(storage))
		for k, v := range storage {
			keys = append(keys, k)
			values = append(values, v)
		}
		storages = append(storages, types.DiffStorage{
			Account: accountHash,
			Keys:    keys,
			Vals:    values,
		})
	}
	return destructs, accounts, storages
}

// PrepareAccessList handles the preparatory steps for executing a state transition with
// regards to both EIP-2929 and EIP-2930:
//
// - Add sender to access list (2929)
// - Add destination to access list (2929)
// - Add precompiles to access list (2929)
// - Add the contents of the optional tx access list (2930)
//
// This method should only be called if Yolov3/Berlin/2929+2930 is applicable at the current number.
func (s *StateDB) PrepareAccessList(sender common.Address, dst *common.Address, precompiles []common.Address, list types.AccessList) {
	s.AddAddressToAccessList(sender)
	if dst != nil {
		s.AddAddressToAccessList(*dst)
		// If it's a create-tx, the destination will be added inside evm.create
	}
	for _, addr := range precompiles {
		s.AddAddressToAccessList(addr)
	}
	for _, el := range list {
		s.AddAddressToAccessList(el.Address)
		for _, key := range el.StorageKeys {
			s.AddSlotToAccessList(el.Address, key)
		}
	}
}

// AddAddressToAccessList adds the given address to the access list
func (s *StateDB) AddAddressToAccessList(addr common.Address) {
	if s.accessList == nil {
		s.accessList = newAccessList()
	}
	if s.accessList.AddAddress(addr) {
		s.journal.append(accessListAddAccountChange{&addr})
	}
}

// AddSlotToAccessList adds the given (address, slot)-tuple to the access list
func (s *StateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	if s.accessList == nil {
		s.accessList = newAccessList()
	}
	addrMod, slotMod := s.accessList.AddSlot(addr, slot)
	if addrMod {
		// In practice, this should not happen, since there is no way to enter the
		// scope of 'address' without having the 'address' become already added
		// to the access list (via call-variant, create, etc).
		// Better safe than sorry, though
		s.journal.append(accessListAddAccountChange{&addr})
	}
	if slotMod {
		s.journal.append(accessListAddSlotChange{
			address: &addr,
			slot:    &slot,
		})
	}
}

// AddressInAccessList returns true if the given address is in the access list.
func (s *StateDB) AddressInAccessList(addr common.Address) bool {
	if s.accessList == nil {
		return false
	}
	return s.accessList.ContainsAddress(addr)
}

// SlotInAccessList returns true if the given (address, slot)-tuple is in the access list.
func (s *StateDB) SlotInAccessList(addr common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	if s.accessList == nil {
		return false, false
	}
	return s.accessList.Contains(addr, slot)
}

func (s *StateDB) GetDirtyAccounts() []common.Address {
	accounts := make([]common.Address, 0, len(s.stateObjectsDirty))
	for account := range s.stateObjectsDirty {
		accounts = append(accounts, account)
	}
	return accounts
}

func (s *StateDB) GetStorage(address common.Address) *sync.Map {
	return s.storagePool.getStorage(address)
}
