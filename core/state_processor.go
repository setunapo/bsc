// Copyright 2015 The go-ethereum Authors
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

package core

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/gopool"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/systemcontracts"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/internal/debug"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	fullProcessCheck       = 21 // On diff sync mode, will do full process every fullProcessCheck randomly
	recentTime             = 1024 * 3
	recentDiffLayerTimeout = 5
	farDiffLayerTimeout    = 2
	maxUnitSize            = 10
	dispatchPolicyStatic   = 1
	dispatchPolicyDynamic  = 2  // not supported
	maxRedoCounterInstage1 = 4  // try 2, 4, 10, or no limit? not needed
	stage2CheckNumber      = 20 // not fixed, use decrease?
	stage2RedoNumber       = 3
	stage2ReservedNum      = 3 // ?
)

var dispatchPolicy = dispatchPolicyStatic

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	return &StateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// add for parallel executions
type ParallelStateProcessor struct {
	StateProcessor
	parallelNum               int // leave a CPU to dispatcher
	queueSize                 int // parallel slot's maximum number of pending Txs
	pendingConfirmChan        chan *ParallelTxResult
	pendingConfirmResultsLock sync.RWMutex                // fixme: this if for Stage2, to remove this lock
	pendingConfirmResults     map[int][]*ParallelTxResult // tx could be executed several times, with several result to check
	txResultChan              chan *ParallelTxResult      // to notify dispatcher that a tx is done
	// txReqAccountSorted   map[common.Address][]*ParallelTxRequest // fixme: *ParallelTxRequest => ParallelTxRequest?
	slotState             []*SlotState // idle, or pending messages
	mergedTxIndex         int          // the latest finalized tx index, fixme: use Atomic
	slotDBsToRelease      []*state.ParallelStateDB
	debugConflictRedoNum  int
	unconfirmedStateDBs   *sync.Map     // [int]*state.StateDB // fixme: concurrent safe, not use sync.Map?
	stopSlotChan          chan int      // fixme: use struct{}{}, to make sure all slot are idle
	stopConfirmChan       chan struct{} // fixme: use struct{}{}, to make sure all slot are idle
	confirmStage2Chan     chan int
	stopConfirmStage2Chan chan struct{}
	allTxReqs             []*ParallelTxRequest
	txReqExecuteRecord    map[int]int      // for each the execute count of each Tx
	txReqPrefetchRecord   map[int]struct{} // for each the execute count of each Tx
	txReqExecuteCount     int
	inConfirmStage2       bool
}

func NewParallelStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine, parallelNum int, queueSize int) *ParallelStateProcessor {
	processor := &ParallelStateProcessor{
		StateProcessor: *NewStateProcessor(config, bc, engine),
		parallelNum:    parallelNum,
		queueSize:      queueSize,
	}
	processor.init()
	return processor
}

type LightStateProcessor struct {
	check int64
	StateProcessor
}

func NewLightStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *LightStateProcessor {
	randomGenerator := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	check := randomGenerator.Int63n(fullProcessCheck)
	return &LightStateProcessor{
		check:          check,
		StateProcessor: *NewStateProcessor(config, bc, engine),
	}
}

func (p *LightStateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (*state.StateDB, types.Receipts, []*types.Log, uint64, error) {
	allowLightProcess := true
	if posa, ok := p.engine.(consensus.PoSA); ok {
		allowLightProcess = posa.AllowLightProcess(p.bc, block.Header())
	}
	// random fallback to full process
	if allowLightProcess && block.NumberU64()%fullProcessCheck != uint64(p.check) && len(block.Transactions()) != 0 {
		var pid string
		if peer, ok := block.ReceivedFrom.(PeerIDer); ok {
			pid = peer.ID()
		}
		var diffLayer *types.DiffLayer
		var diffLayerTimeout = recentDiffLayerTimeout
		if time.Now().Unix()-int64(block.Time()) > recentTime {
			diffLayerTimeout = farDiffLayerTimeout
		}
		for tried := 0; tried < diffLayerTimeout; tried++ {
			// wait a bit for the diff layer
			diffLayer = p.bc.GetUnTrustedDiffLayer(block.Hash(), pid)
			if diffLayer != nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if diffLayer != nil {
			if err := diffLayer.Receipts.DeriveFields(p.bc.chainConfig, block.Hash(), block.NumberU64(), block.Transactions()); err != nil {
				log.Error("Failed to derive block receipts fields", "hash", block.Hash(), "number", block.NumberU64(), "err", err)
				// fallback to full process
				return p.StateProcessor.Process(block, statedb, cfg)
			}

			receipts, logs, gasUsed, err := p.LightProcess(diffLayer, block, statedb)
			if err == nil {
				log.Info("do light process success at block", "num", block.NumberU64())
				return statedb, receipts, logs, gasUsed, nil
			}
			log.Error("do light process err at block", "num", block.NumberU64(), "err", err)
			p.bc.removeDiffLayers(diffLayer.DiffHash)
			// prepare new statedb
			statedb.StopPrefetcher()
			parent := p.bc.GetHeader(block.ParentHash(), block.NumberU64()-1)
			statedb, err = state.New(parent.Root, p.bc.stateCache, p.bc.snaps)
			statedb.SetExpectedStateRoot(block.Root())
			if p.bc.pipeCommit {
				statedb.EnablePipeCommit()
			}
			if err != nil {
				return statedb, nil, nil, 0, err
			}
			// Enable prefetching to pull in trie node paths while processing transactions
			statedb.StartPrefetcher("chain")
		}
	}
	// fallback to full process
	return p.StateProcessor.Process(block, statedb, cfg)
}

func (p *LightStateProcessor) LightProcess(diffLayer *types.DiffLayer, block *types.Block, statedb *state.StateDB) (types.Receipts, []*types.Log, uint64, error) {
	statedb.MarkLightProcessed()
	fullDiffCode := make(map[common.Hash][]byte, len(diffLayer.Codes))
	diffTries := make(map[common.Address]state.Trie)
	diffCode := make(map[common.Hash][]byte)

	snapDestructs, snapAccounts, snapStorage, err := statedb.DiffLayerToSnap(diffLayer)
	if err != nil {
		return nil, nil, 0, err
	}

	for _, c := range diffLayer.Codes {
		fullDiffCode[c.Hash] = c.Code
	}
	stateTrie, err := statedb.Trie()
	if err != nil {
		return nil, nil, 0, err
	}
	for des := range snapDestructs {
		stateTrie.TryDelete(des[:])
	}
	threads := gopool.Threads(len(snapAccounts))

	iteAccounts := make([]common.Address, 0, len(snapAccounts))
	for diffAccount := range snapAccounts {
		iteAccounts = append(iteAccounts, diffAccount)
	}

	errChan := make(chan error, threads)
	exitChan := make(chan struct{})
	var snapMux sync.RWMutex
	var stateMux, diffMux sync.Mutex
	for i := 0; i < threads; i++ {
		start := i * len(iteAccounts) / threads
		end := (i + 1) * len(iteAccounts) / threads
		if i+1 == threads {
			end = len(iteAccounts)
		}
		go func(start, end int) {
			for index := start; index < end; index++ {
				select {
				// fast fail
				case <-exitChan:
					return
				default:
				}
				diffAccount := iteAccounts[index]
				snapMux.RLock()
				blob := snapAccounts[diffAccount]
				snapMux.RUnlock()
				addrHash := crypto.Keccak256Hash(diffAccount[:])
				latestAccount, err := snapshot.FullAccount(blob)
				if err != nil {
					errChan <- err
					return
				}

				// fetch previous state
				var previousAccount state.Account
				stateMux.Lock()
				enc, err := stateTrie.TryGet(diffAccount[:])
				stateMux.Unlock()
				if err != nil {
					errChan <- err
					return
				}
				if len(enc) != 0 {
					if err := rlp.DecodeBytes(enc, &previousAccount); err != nil {
						errChan <- err
						return
					}
				}
				if latestAccount.Balance == nil {
					latestAccount.Balance = new(big.Int)
				}
				if previousAccount.Balance == nil {
					previousAccount.Balance = new(big.Int)
				}
				if previousAccount.Root == (common.Hash{}) {
					previousAccount.Root = types.EmptyRootHash
				}
				if len(previousAccount.CodeHash) == 0 {
					previousAccount.CodeHash = types.EmptyCodeHash
				}

				// skip no change account
				if previousAccount.Nonce == latestAccount.Nonce &&
					bytes.Equal(previousAccount.CodeHash, latestAccount.CodeHash) &&
					previousAccount.Balance.Cmp(latestAccount.Balance) == 0 &&
					previousAccount.Root == common.BytesToHash(latestAccount.Root) {
					// It is normal to receive redundant message since the collected message is redundant.
					log.Debug("receive redundant account change in diff layer", "account", diffAccount, "num", block.NumberU64())
					snapMux.Lock()
					delete(snapAccounts, diffAccount)
					delete(snapStorage, diffAccount)
					snapMux.Unlock()
					continue
				}

				// update code
				codeHash := common.BytesToHash(latestAccount.CodeHash)
				if !bytes.Equal(latestAccount.CodeHash, previousAccount.CodeHash) &&
					!bytes.Equal(latestAccount.CodeHash, types.EmptyCodeHash) {
					if code, exist := fullDiffCode[codeHash]; exist {
						if crypto.Keccak256Hash(code) != codeHash {
							errChan <- fmt.Errorf("code and code hash mismatch, account %s", diffAccount.String())
							return
						}
						diffMux.Lock()
						diffCode[codeHash] = code
						diffMux.Unlock()
					} else {
						rawCode := rawdb.ReadCode(p.bc.db, codeHash)
						if len(rawCode) == 0 {
							errChan <- fmt.Errorf("missing code, account %s", diffAccount.String())
							return
						}
					}
				}

				//update storage
				latestRoot := common.BytesToHash(latestAccount.Root)
				if latestRoot != previousAccount.Root {
					accountTrie, err := statedb.Database().OpenStorageTrie(addrHash, previousAccount.Root)
					if err != nil {
						errChan <- err
						return
					}
					snapMux.RLock()
					storageChange, exist := snapStorage[diffAccount]
					snapMux.RUnlock()

					if !exist {
						errChan <- errors.New("missing storage change in difflayer")
						return
					}
					for k, v := range storageChange {
						if len(v) != 0 {
							accountTrie.TryUpdate([]byte(k), v)
						} else {
							accountTrie.TryDelete([]byte(k))
						}
					}

					// check storage root
					accountRootHash := accountTrie.Hash()
					if latestRoot != accountRootHash {
						errChan <- errors.New("account storage root mismatch")
						return
					}
					diffMux.Lock()
					diffTries[diffAccount] = accountTrie
					diffMux.Unlock()
				} else {
					snapMux.Lock()
					delete(snapStorage, diffAccount)
					snapMux.Unlock()
				}

				// can't trust the blob, need encode by our-self.
				latestStateAccount := state.Account{
					Nonce:    latestAccount.Nonce,
					Balance:  latestAccount.Balance,
					Root:     common.BytesToHash(latestAccount.Root),
					CodeHash: latestAccount.CodeHash,
				}
				bz, err := rlp.EncodeToBytes(&latestStateAccount)
				if err != nil {
					errChan <- err
					return
				}
				stateMux.Lock()
				err = stateTrie.TryUpdate(diffAccount[:], bz)
				stateMux.Unlock()
				if err != nil {
					errChan <- err
					return
				}
			}
			errChan <- nil
		}(start, end)
	}

	for i := 0; i < threads; i++ {
		err := <-errChan
		if err != nil {
			close(exitChan)
			return nil, nil, 0, err
		}
	}

	var allLogs []*types.Log
	var gasUsed uint64
	for _, receipt := range diffLayer.Receipts {
		allLogs = append(allLogs, receipt.Logs...)
		gasUsed += receipt.GasUsed
	}

	// Do validate in advance so that we can fall back to full process
	if err := p.bc.validator.ValidateState(block, statedb, diffLayer.Receipts, gasUsed, false); err != nil {
		log.Error("validate state failed during diff sync", "error", err)
		return nil, nil, 0, err
	}

	// remove redundant storage change
	for account := range snapStorage {
		if _, exist := snapAccounts[account]; !exist {
			log.Warn("receive redundant storage change in diff layer")
			delete(snapStorage, account)
		}
	}

	// remove redundant code
	if len(fullDiffCode) != len(diffLayer.Codes) {
		diffLayer.Codes = make([]types.DiffCode, 0, len(diffCode))
		for hash, code := range diffCode {
			diffLayer.Codes = append(diffLayer.Codes, types.DiffCode{
				Hash: hash,
				Code: code,
			})
		}
	}

	statedb.SetSnapData(snapDestructs, snapAccounts, snapStorage)
	if len(snapAccounts) != len(diffLayer.Accounts) || len(snapStorage) != len(diffLayer.Storages) {
		diffLayer.Destructs, diffLayer.Accounts, diffLayer.Storages = statedb.SnapToDiffLayer()
	}
	statedb.SetDiff(diffLayer, diffTries, diffCode)

	return diffLayer.Receipts, allLogs, gasUsed, nil
}

type SlotState struct {
	pendingTxReqChan       chan *ParallelTxRequest
	pendingTxReqShadowChan chan *ParallelTxRequest
	pendingTxReqList       []*ParallelTxRequest        // maintained by dispatcher for dispatch policy
	slotdbChan             chan *state.ParallelStateDB // dispatch will create and send this slotDB to slot
	activatedId            int32                       // 0: normal slot, 1: shadow slot
	// idle                   bool
	// shadowIdle             bool
	stopChan       chan struct{}
	stopShadowChan chan struct{}
	// txReqUnits         []*ParallelDispatchUnit // only dispatch can accesssd
}

type ParallelTxResult struct {
	prefetchAddr bool  // for redo and pending tx quest, slot needs new slotDB,
	updateSlotDB bool  // for redo and pending tx quest, slot needs new slotDB,
	keepSystem   bool  // for redo, should keep system address's balance
	slotIndex    int   // slot index
	err          error // to describe error message?
	txReq        *ParallelTxRequest
	receipt      *types.Receipt
	slotDB       *state.ParallelStateDB // if updated, it is not equal to txReq.slotDB
	gpSlot       *GasPool
	evm          *vm.EVM
	result       *ExecutionResult
}

type ParallelTxRequest struct {
	txIndex         int
	staticSlotIndex int // static dispatched id
	tx              *types.Transaction
	// slotDB         *state.ParallelStateDB
	gasLimit       uint64
	msg            types.Message
	block          *types.Block
	vmConfig       vm.Config
	bloomProcessor *AsyncReceiptBloomGenerator
	usedGas        *uint64
	curTxChan      chan int
	systemAddrRedo bool
	runnable       int32 // we only run a Tx once or if it needs redo
}

// to create and start the execution slot goroutines
func (p *ParallelStateProcessor) init() {
	log.Info("Parallel execution mode is enabled", "Parallel Num", p.parallelNum,
		"CPUNum", runtime.NumCPU(),
		"QueueSize", p.queueSize)
	p.txResultChan = make(chan *ParallelTxResult, p.parallelNum)
	p.stopSlotChan = make(chan int, 1)
	p.stopConfirmChan = make(chan struct{}, 1)
	p.stopConfirmStage2Chan = make(chan struct{}, 1)

	p.slotState = make([]*SlotState, p.parallelNum)
	for i := 0; i < p.parallelNum; i++ {
		p.slotState[i] = &SlotState{
			slotdbChan:             make(chan *state.ParallelStateDB, 1),
			pendingTxReqChan:       make(chan *ParallelTxRequest, 1),
			pendingTxReqShadowChan: make(chan *ParallelTxRequest, 1),
			stopChan:               make(chan struct{}, 1),
			stopShadowChan:         make(chan struct{}, 1),
		}
		// start the shadow slot first
		go func(slotIndex int) {
			p.runSlotLoop(slotIndex, 1) // this loop will be permanent live
		}(i)

		// start the slot's goroutine
		go func(slotIndex int) {
			p.runSlotLoop(slotIndex, 0) // this loop will be permanent live
		}(i)
	}

	p.pendingConfirmChan = make(chan *ParallelTxResult, 400)
	go func() {
		p.runConfirmLoop() // this loop will be permanent live
	}()
	p.confirmStage2Chan = make(chan int, 10)
	go func() {
		p.runConfirmStage2Loop() // this loop will be permanent live
	}()
}

/*
// for parallel execute, we put contracts of same address in a slot,
// since these txs probably would have conflicts
func (p *ParallelStateProcessor) queueSameToAddress(txReq *ParallelTxRequest) bool {
	txToAddr := txReq.tx.To()
	// To() == nil means contract creation, no same To address
	if txToAddr == nil {
		return false
	}
	for i, slot := range p.slotState {
		for _, pending := range slot.pendingTxReqList {
			// To() == nil means contract creation, skip it.
			if pending.tx.To() == nil {
				continue
			}
			// same to address, put it on slot's pending list.
			if *txToAddr == *pending.tx.To() {
				select {
				case slot.pendingTxReqChan <- txReq:
					slot.pendingTxReqList = append(slot.pendingTxReqList, txReq)
					log.Debug("queue same To address", "Slot", i, "txIndex", txReq.txIndex)
					return true
				default:
					log.Debug("queue same To address, but queue is full", "Slot", i, "txIndex", txReq.txIndex)
					break // try next slot
				}
			}
		}
	}
	return false
}

// for parallel execute, we put contracts of same address in a slot,
// since these txs probably would have conflicts

func (p *ParallelStateProcessor) queueSameFromAddress(txReq *ParallelTxRequest) bool {
	txFromAddr := txReq.msg.From()
	for i, slot := range p.slotState {
		for _, pending := range slot.pendingTxReqList {
			// same from address, put it on slot's pending list.
			if txFromAddr == pending.msg.From() {
				select {
				case slot.pendingTxReqChan <- txReq:
					slot.pendingTxReqList = append(slot.pendingTxReqList, txReq)
					log.Debug("queue same From address", "Slot", i, "txIndex", txReq.txIndex)
					return true
				default:
					log.Debug("queue same From address, but queue is full", "Slot", i, "txIndex", txReq.txIndex)
					break // try next slot
				}
			}
		}
	}
	return false
}

func (p *ParallelStateProcessor) dispatchToHungrySlot(statedb *state.StateDB, txReq *ParallelTxRequest) bool {
	var workload int = len(p.slotState[0].pendingTxReqList)
	var slotIndex int = 0
	for i, slot := range p.slotState { // can start from index 1
		if len(slot.pendingTxReqList) < workload {
			slotIndex = i
			workload = len(slot.pendingTxReqList)
		}
	}
	if workload >= p.queueSize {
		log.Debug("dispatch no Hungry Slot, all slots are full of task", "queueSize", p.queueSize)
		return false
	}

	log.Debug("dispatch To Hungry Slot", "slot", slotIndex, "workload", workload, "txIndex", txReq.txIndex)
	slot := p.slotState[slotIndex]
	select {
	case slot.pendingTxReqChan <- txReq:
		slot.pendingTxReqList = append(slot.pendingTxReqList, txReq)
		return true
	default:
		log.Error("dispatch To Hungry Slot, but chan <- txReq failed??", "Slot", slotIndex, "txIndex", txReq.txIndex)
		break
	}

	return false
}
*/
// 1.Sliding Window:

// txReqAccountSorted
// Unit: a slice of *TxReq, with len <= maxParallelUnitSize
// Units should be ordered by TxIndex
// TxReq's TxIndex of a Unit should be within a certain range: ParallelNum * maxParallelUnitSize?

// Dispatch an Unit once for each slot?
// Unit make policy:
//  1.From
//  2.To...

/*
type ParallelDispatchUnit struct {
	unitId int // unit with same id is likely has dependency, but to put them in same slot.
	// ** preemptible: true
	//    the unit can be preempted by dispatcher and be reallocated to other slot.
	//    unit with same Id will be reallocated together.
	// ** preemptible: false
	//    can not be preempted, maybe it is the first unconfirmed unit of the current slot.
	preemptible  bool
	startTxIndex int
	endTxIndex   int
	txsSize      int
	txReqs       []*ParallelTxRequest
}
*/
// Try best to make the unit full, it is full when:
//  ** maxUnitSize reached
//  ** tx index range reached
// Avoid to make it full immediately, swicth to next unit when:
//  ** full
//  ** not full, but the Tx of the same address has exhausted

// New Unit will be created by batch
//  ** first

// Benefit of StaticDispatch:
//  ** try best to make Txs with same From() in same slot
//  ** reduce IPC cost by dispatch in Unit

// 2022.03.25: too complicated, apply simple method first...
// ** make sure same From in same slot
// ** try to make it balanced, queue to the most hungry slot for new Address
func (p *ParallelStateProcessor) doStaticDispatch(mainStatedb *state.StateDB, txReqs []*ParallelTxRequest) {
	fromSlotMap := make(map[common.Address]int, 100)
	toSlotMap := make(map[common.Address]int, 100)
	for _, txReq := range txReqs {
		var slotIndex int = -1
		if i, ok := fromSlotMap[txReq.msg.From()]; ok {
			// first: same From are all in same slot
			slotIndex = i
		} else if txReq.msg.To() != nil {
			// To Address, with txIndex sorted, could be in different slot.
			// fixme: Create will move to hungry slot
			if i, ok := toSlotMap[*txReq.msg.To()]; ok {
				slotIndex = i
			}
		}

		// not found, dispatch to most hungry slot
		if slotIndex == -1 {
			var workload int = len(p.slotState[0].pendingTxReqList)
			slotIndex = 0
			for i, slot := range p.slotState { // can start from index 1
				if len(slot.pendingTxReqList) < workload {
					slotIndex = i
					workload = len(slot.pendingTxReqList)
				}
			}
		}
		// update
		fromSlotMap[txReq.msg.From()] = slotIndex
		if txReq.msg.To() != nil {
			toSlotMap[*txReq.msg.To()] = slotIndex
		}

		slot := p.slotState[slotIndex]
		txReq.staticSlotIndex = slotIndex // txreq is better to be executed in this slot
		slot.pendingTxReqList = append(slot.pendingTxReqList, txReq)
	}
}

// get the most hungry slot

/*
	//
	 unitsInBatch := make([]*ParallelDispatchUnit, p.parallelNum )

	slotIndex :=0
	for _, txReqs := range p.txReqAccountSorted {
		currentUnit := unitsInBatch[slotIndex]
		slotIndex := (slotIndex+1) % p.parallelNum
		if currentUnit.txsSize >= maxUnitSize {
			// current slot's unit is full, try next slot's unit
			continue
		}
			var unit *ParallelDispatchUnit
			for _, txReq := range txReqs {
				numUnit := len(p.slotState[slotIndex].txReqUnits)
				// create a unit for the first one
				if numUnit == 0 {
					unit = &ParallelDispatchUnit{
						startTxIndex: txReq.txIndex,
						endTxIndex:   txReq.txIndex + txIndexSize,
						txsSize:      0,
					}
					unit.txReqs = append(unit.txReqs, txReq)
					continue
				}
				//
				unit = p.slotState[slotIndex].txReqUnits[numUnit-1]
				// unit is already full
				if unit.txsSize >= maxParallelUnitSize {

				}
			}
		}
		// first: move From() to unit

		allUnit = append(allUnit)
	}
*/

// wait until the next Tx is executed and its result is merged to the main stateDB
func (p *ParallelStateProcessor) waitUntilNextTxDone(statedb *state.StateDB, gp *GasPool) *ParallelTxResult {
	traceMsg := "waitUntilNextTxDone"
	defer debug.Handler.StartRegionAuto(traceMsg)()
	var result *ParallelTxResult
	for {
		result = <-p.txResultChan
		// slot may request new slotDB, if a TxReq do not have valid parallel state db
		if result.updateSlotDB {
			// the target slot is waiting for new slotDB
			slotState := p.slotState[result.slotIndex]
			slotDB := state.NewSlotDB(statedb, consensus.SystemAddress, result.txReq.txIndex,
				p.mergedTxIndex, result.keepSystem, p.unconfirmedStateDBs)
			slotDB.SetSlotIndex(result.slotIndex)
			p.slotDBsToRelease = append(p.slotDBsToRelease, slotDB)
			slotState.slotdbChan <- slotDB
			continue
		}
		if result.prefetchAddr {
			statedb.AddrPrefetch(result.slotDB)
			continue
		}
		// ok, the tx result is valid and can be merged
		break
	}
	log.Debug("waitUntilNextTxDone receive a result", "result.slotIndex", result.slotIndex,
		"TxIndex", result.txReq.txIndex, "result.receipt.GasUsed", result.receipt.GasUsed)
	if err := gp.SubGas(result.receipt.GasUsed); err != nil {
		log.Error("gas limit reached", "block", result.txReq.block.Number(),
			"txIndex", result.txReq.txIndex, "GasUsed", result.receipt.GasUsed, "gp.Gas", gp.Gas())
	}

	resultTxIndex := result.txReq.txIndex
	// no need to delete in static dispatch
	// if dispatchPolicy == dispatchPolicyDynamic {
	// resultSlotIndex := result.slotIndex
	// resultSlotState := p.slotState[resultSlotIndex]
	// resultSlotState.pendingTxReqList = resultSlotState.pendingTxReqList[1:]
	// }
	statedb.MergeSlotDB(result.slotDB, result.receipt, resultTxIndex)

	if resultTxIndex != p.mergedTxIndex+1 {
		log.Error("ProcessParallel tx result out of order", "resultTxIndex", resultTxIndex,
			"p.mergedTxIndex", p.mergedTxIndex)
	}
	p.mergedTxIndex = resultTxIndex
	// log.Debug("waitUntilNextTxDone result is merged", "result.slotIndex", result.slotIndex,
	//	"TxIndex", result.txReq.txIndex, "p.mergedTxIndex", p.mergedTxIndex)

	// notify the following Tx, it is merged,
	// todo(optimize): if next tx is in same slot, it do not need to wait; save this channel cost.
	result.txReq.curTxChan <- p.mergedTxIndex
	// 	close(result.txReq.curTxChan)
	return result
}

func (p *ParallelStateProcessor) executeInSlot(slotIndex int, txReq *ParallelTxRequest, slotDB *state.ParallelStateDB) *ParallelTxResult {
	traceMsg := "executeInSlot:" + strconv.Itoa(slotIndex)
	defer debug.Handler.StartRegionAuto(traceMsg)()

	slotDB.Prepare(txReq.tx.Hash(), txReq.block.Hash(), txReq.txIndex)
	blockContext := NewEVMBlockContext(txReq.block.Header(), p.bc, nil) // can share blockContext within a block for efficiency
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, slotDB, p.config, txReq.vmConfig)
	// gasLimit not accurate, but it is ok for block import.
	// each slot would use its own gas pool, and will do gaslimit check later
	gpSlot := new(GasPool).AddGas(txReq.gasLimit) // block.GasLimit()

	evm, result, err := applyTransactionStageExecution(txReq.msg, gpSlot, slotDB, vmenv)
	if err != nil {
		// txReq.runnable must be flase, switch it to true
		atomic.CompareAndSwapInt32(&txReq.runnable, 0, 1)

		// the error could be caused by unconfirmed balance reference,
		// the balance could insufficient to pay its gas limit, which cause it preCheck.buyGas() failed
		// redo could solve it.
		log.Warn("In slot execution error", "error", err,
			"slotIndex", slotIndex, "txIndex", txReq.txIndex)
		return &ParallelTxResult{
			updateSlotDB: false,
			slotIndex:    slotIndex,
			txReq:        txReq,
			receipt:      nil, // receipt is generated in finalize stage
			slotDB:       slotDB,
			err:          err,
			gpSlot:       gpSlot,
			evm:          evm,
			result:       result,
		}
	}
	if result.Failed() {
		// if Tx is reverted, all its state change will be discarded
		slotDB.RevertSlotDB(txReq.msg.From())
	}
	slotDB.Finalise(true) // Finalise could write s.parallel.addrStateChangesInSlot[addr], keep Read and Write in same routine to avoid crash
	return &ParallelTxResult{
		updateSlotDB: false,
		slotIndex:    slotIndex,
		txReq:        txReq,
		receipt:      nil, // receipt is generated in finalize stage
		slotDB:       slotDB,
		err:          nil,
		gpSlot:       gpSlot,
		evm:          evm,
		result:       result,
	}
}

func (p *ParallelStateProcessor) runConfirmLoop() {
	for {
		// ParallelTxResult is not confirmed yet
		var unconfirmedResult *ParallelTxResult
		select {
		case <-p.stopConfirmChan:
			for len(p.pendingConfirmChan) > 0 {
				<-p.pendingConfirmChan
			}
			p.stopSlotChan <- -1
			continue
		case unconfirmedResult = <-p.pendingConfirmChan:
		}
		txIndex := unconfirmedResult.txReq.txIndex
		if txIndex <= p.mergedTxIndex {
			log.Warn("runConfirmLoop drop merged txReq", "txIndex", txIndex, "p.mergedTxIndex", p.mergedTxIndex)
			continue
		}

		if unconfirmedResult.err != nil {
			if _, ok := p.txReqExecuteRecord[txIndex]; !ok {
				p.txReqExecuteRecord[txIndex] = 0
				p.txReqExecuteCount++
			}
			p.txReqExecuteRecord[txIndex]++
		}

		region := debug.Handler.StartTrace("runConfirmLoop")
		log.Debug("runConfirmLoop receive", "txIndex", txIndex, "p.mergedTxIndex", p.mergedTxIndex)
		p.pendingConfirmResultsLock.Lock()
		p.pendingConfirmResults[txIndex] = append(p.pendingConfirmResults[txIndex], unconfirmedResult)
		p.pendingConfirmResultsLock.Unlock()
		newTxMerged := false
		for {
			targetTxIndex := p.mergedTxIndex + 1
			if delivered := p.toConfirmTxIndex(targetTxIndex, false); !delivered {
				break
			}
			newTxMerged = true
		}
		//  schedule prefetch once, and only when  unconfirmedResult is valid and is not merged
		if unconfirmedResult.err != nil && txIndex > p.mergedTxIndex {
			if _, ok := p.txReqPrefetchRecord[txIndex]; !ok {
				p.txReqPrefetchRecord[txIndex] = struct{}{}
				// do prefetch in advance.
				unconfirmedResult.prefetchAddr = true
				p.txResultChan <- unconfirmedResult
			}
		}

		txSize := len(p.allTxReqs)
		// usually, the the last Tx could be the bottleneck it could be very slow,
		// so it is better for us to enter stage 2 a bit earlier
		targetStage2Count := txSize
		if txSize > 50 {
			targetStage2Count = txSize - stage2ReservedNum
		}
		if !p.inConfirmStage2 && p.txReqExecuteCount == targetStage2Count {
			p.inConfirmStage2 = true
			for i := 0; i < txSize; i++ {
				p.txReqExecuteRecord[txIndex] = 0 // clear it when enter stage2, for redo limit
			}
		}
		// if no Tx is merged, we will skip the stage 2 check
		if !newTxMerged {
			debug.Handler.EndTrace(region)
			continue
		}

		// stage 2,if all tx have been executed at least once, and its result has been recevied.
		// in Stage 2, we will run check when merge is advanced.
		if p.inConfirmStage2 {
			// more aggressive tx result confirm, even for these Txs not in turn
			// now we will be more aggressive:
			//   do conflcit check , as long as tx result is generated,
			//   if lucky, it is the Tx's turn, we will do conflict check with WBNB makeup
			//   otherwise, do conflict check without WBNB makeup, but we will ignor WBNB's balance conflict.
			// throw these likely conflicted tx back to re-execute
			/*
				startTxIndex := p.mergedTxIndex + 2 // stage 2's will start from the next target merge index
				endTxIndex := startTxIndex + stage2CheckNumber
				if endTxIndex > (txSize - 1) {
					endTxIndex = txSize - 1
				}
				conflictNumMark := p.debugConflictRedoNum
				for txIndex := startTxIndex; txIndex < endTxIndex; txIndex++ {
					p.toConfirmTxIndex(txIndex, true)
					newConflictNum := p.debugConflictRedoNum - conflictNumMark
					// if many redo is scheduled, stop now
					if newConflictNum >= stage2RedoNumber {
						break
					}
				}
			*/
			p.confirmStage2Chan <- p.mergedTxIndex
		}
		debug.Handler.EndTrace(region)
	}
}

func (p *ParallelStateProcessor) runConfirmStage2Loop() {
	for {
		var mergedTxIndex int
		select {
		case <-p.stopConfirmStage2Chan:
			for len(p.confirmStage2Chan) > 0 {
				<-p.confirmStage2Chan
			}
			p.stopSlotChan <- -1
			continue
		case mergedTxIndex = <-p.confirmStage2Chan:
			for len(p.confirmStage2Chan) > 0 {
				mergedTxIndex = <-p.confirmStage2Chan // drain the chan to get the latest merged txIndex
			}
		}
		// stage 2,if all tx have been executed at least once, and its result has been recevied.
		// in Stage 2, we will run check when merge is advanced.
		// more aggressive tx result confirm, even for these Txs not in turn
		// now we will be more aggressive:
		//   do conflcit check , as long as tx result is generated,
		//   if lucky, it is the Tx's turn, we will do conflict check with WBNB makeup
		//   otherwise, do conflict check without WBNB makeup, but we will ignor WBNB's balance conflict.
		// throw these likely conflicted tx back to re-execute
		region := debug.Handler.StartTrace("runConfirmLoop")
		startTxIndex := mergedTxIndex + 2 // stage 2's will start from the next target merge index
		endTxIndex := startTxIndex + stage2CheckNumber
		txSize := len(p.allTxReqs)
		if endTxIndex > (txSize - 1) {
			endTxIndex = txSize - 1
		}
		conflictNumMark := p.debugConflictRedoNum
		for txIndex := startTxIndex; txIndex < endTxIndex; txIndex++ {
			p.toConfirmTxIndex(txIndex, true)
			newConflictNum := p.debugConflictRedoNum - conflictNumMark
			// if many redo is scheduled, stop now
			if newConflictNum >= stage2RedoNumber {
				break
			}
		}
		debug.Handler.EndTrace(region)
	}

}

// do conflict detect
func (p *ParallelStateProcessor) hasConflict(txResult *ParallelTxResult, isStage2 bool) bool {
	slotDB := txResult.slotDB

	if txResult.err != nil {
		return true
	} else if slotDB.SystemAddressRedo() {
		if !isStage2 {
			// for system addr redo, it has to wait until it's turn to keep the system address balance
			txResult.txReq.systemAddrRedo = true
		}
		return true
	} else if slotDB.NeedsRedo() {
		// if this is any reason that indicates this transaction needs to redo, skip the conflict check
		return true
	} else {
		// to check if what the slot db read is correct.
		// refDetail := slotDB.UnconfirmedRefList()
		if !slotDB.IsParallelReadsValid(isStage2, p.mergedTxIndex, p.unconfirmedStateDBs) {
			return true
		}
	}
	return false
}

func (p *ParallelStateProcessor) switchSlot(slot *SlotState, slotIndex int) {
	if atomic.CompareAndSwapInt32(&slot.activatedId, 0, 1) {
		// switch from normal to shadow slot
		if len(slot.pendingTxReqShadowChan) == 0 {
			slot.pendingTxReqShadowChan <- nil // only notify when target once
		}
	} else if atomic.CompareAndSwapInt32(&slot.activatedId, 1, 0) {
		// switch from shadow to normal slot
		if len(slot.pendingTxReqChan) == 0 {
			slot.pendingTxReqChan <- nil // only notify when target once
		}
	}
}

// to confirm a serial TxResults with same txIndex
func (p *ParallelStateProcessor) toConfirmTxIndex(targetTxIndex int, isStage2 bool) bool {
	defer debug.Handler.StartRegionAuto("toConfirmTxIndex")()
	// var targetTxIndex int
	if targetTxIndex <= p.mergedTxIndex {
		log.Warn("toConfirmTxIndex in stage 2, invalid txIndex",
			"targetTxIndex", targetTxIndex, "isStage2", isStage2)
		return false
	}
	if targetTxIndex == p.mergedTxIndex+1 && isStage2 {
		// this is the one that can been merged,
		// others are for likely conflict check, since it is not their tuen.
		log.Warn("toConfirmTxIndex in stage 2, invalid txIndex",
			"targetTxIndex", targetTxIndex, "isStage2", isStage2)
		return false
	}

	for {
		// handle a targetTxIndex in a loop
		// targetTxIndex = p.mergedTxIndex + 1
		// select a unconfirmedResult to check
		p.pendingConfirmResultsLock.RLock()
		results := p.pendingConfirmResults[targetTxIndex]
		resultsLen := len(results)
		if resultsLen == 0 { // no pending result can be verified, break and wait for incoming results
			p.pendingConfirmResultsLock.RUnlock()
			return false
		}
		lastResult := results[len(results)-1] // last is the most fresh, stack based priority
		p.pendingConfirmResultsLock.RUnlock()

		if !isStage2 {
			// not remove the confirm result in Stage2, since the conflict check is guranteed.
			p.pendingConfirmResultsLock.Lock()
			p.pendingConfirmResults[targetTxIndex] = p.pendingConfirmResults[targetTxIndex][:resultsLen-1] // remove from the queue
			p.pendingConfirmResultsLock.Unlock()
			if atomic.CompareAndSwapInt32(&lastResult.txReq.runnable, 1, 1) { // if runnable, no conflict detect
				return false
			}
		}

		valid := p.toConfirmTxIndexResult(lastResult, isStage2)
		staticSlotIndex := lastResult.txReq.staticSlotIndex
		if !valid {
			if resultsLen == 1 || isStage2 { // for Stage 2, we only check its latest result.
				if !isStage2 || p.txReqExecuteRecord[lastResult.txReq.txIndex] < maxRedoCounterInstage1 {
					lastResult.txReq.runnable = 1 // needs redo
					p.debugConflictRedoNum++
				}
				slot := p.slotState[staticSlotIndex]
				// interrupt its current routine, and switch to the other routine
				p.switchSlot(slot, staticSlotIndex)
				return false
			}
			continue
		}
		if isStage2 {
			// likely valid, but not sure, can not deliver
			// fixme: need to handle txResult repeatedly check?
			return false
		}
		log.Debug("runConfirmLoop result to deliver",
			"staticSlotIndex", staticSlotIndex,
			"executedSlotIndex", lastResult.slotIndex,
			"txIndex", lastResult.txReq.txIndex, "mergedTxIndex", p.mergedTxIndex)
		region2 := debug.Handler.StartTrace("valid, deliver to process")
		// result is valid, deliver it to main processor
		lastResult.prefetchAddr = false
		p.txResultChan <- lastResult
		// wait until merged TxIndex is updated
		<-lastResult.txReq.curTxChan
		// 	close(result.txReq.curTxChan) // fixme: to close
		debug.Handler.EndTrace(region2)

		if p.mergedTxIndex != (targetTxIndex) {
			log.Warn("runConfirmLoop result delivered, but unexpected mergedTxIndex",
				"mergedTxIndex", p.mergedTxIndex, "targetTxIndex", targetTxIndex)
		}
		// p.mergedTxIndex = targetTxIndex // fixme: cpu execute disorder,
		return true // try validate next txIndex
	}
}

// to confirm one txResult
func (p *ParallelStateProcessor) toConfirmTxIndexResult(txResult *ParallelTxResult, isStage2 bool) bool {
	defer debug.Handler.StartRegionAuto("toConfirmTxIndexResult")()
	txReq := txResult.txReq
	// txIndex := txReq.txIndex
	// slotDB := txResult.slotDB
	if p.hasConflict(txResult, isStage2) {
		return false
	}
	if isStage2 { // not its turn
		return true // likely valid, not sure, no finalized right now.
	}

	// goroutine unsafe operation will be handled from here for safety
	gasConsumed := txReq.gasLimit - txResult.gpSlot.Gas()
	if gasConsumed != txResult.result.UsedGas {
		log.Error("gasConsumed != result.UsedGas mismatch",
			"gasConsumed", gasConsumed, "result.UsedGas", txResult.result.UsedGas)
	}

	// ok, time to do finalize, stage2 should not be parallel
	header := txReq.block.Header()
	txResult.receipt, txResult.err = applyTransactionStageFinalization(txResult.evm, txResult.result,
		txReq.msg, p.config, txResult.slotDB, header,
		txReq.tx, txReq.usedGas, txReq.bloomProcessor)
	txResult.updateSlotDB = false
	return true
}

func (p *ParallelStateProcessor) runSlotLoop(slotIndex int, slotType int32) {
	curSlot := p.slotState[slotIndex]
	startTxIndex := 0
	var wakeupChan chan *ParallelTxRequest
	var stopChan chan struct{}

	if slotType == 0 { // 0: normal, 1: shadow
		wakeupChan = curSlot.pendingTxReqChan
		stopChan = curSlot.stopChan
	} else {
		wakeupChan = curSlot.pendingTxReqShadowChan
		stopChan = curSlot.stopShadowChan
	}
	for {
		// wait for new TxReq
		// txReq := <-curSlot.pendingTxReqChan
		select {
		case <-stopChan:
			p.stopSlotChan <- slotIndex
			continue
		case <-wakeupChan:
		}

		traceMsg := "runSlotLoop " + strconv.Itoa(slotIndex)
		if slotType == 1 {
			traceMsg = traceMsg + " shadow"
		}
		region1 := debug.Handler.StartTrace(traceMsg)
		startTxIndex = p.mergedTxIndex + 1
		if dispatchPolicy == dispatchPolicyStatic {
			interrupted := false
			for _, txReq := range curSlot.pendingTxReqList {
				if txReq.txIndex < startTxIndex {
					continue
				}
				// if interrupted,
				if curSlot.activatedId != slotType {
					interrupted = true
					break
				}

				if !atomic.CompareAndSwapInt32(&txReq.runnable, 1, 0) {
					// not swapped: txReq.runnable == 0
					continue
				}
				// if txReq.slotDB == nil {  // must update slot DB
				resultUpdateDB := &ParallelTxResult{
					updateSlotDB: true,
					slotIndex:    slotIndex,
					err:          nil,
					txReq:        txReq,
					keepSystem:   txReq.systemAddrRedo,
				}
				p.txResultChan <- resultUpdateDB
				slotDB := <-curSlot.slotdbChan
				if slotDB == nil { // block is processed, fixme: no need to steal
					break
				}
				result := p.executeInSlot(slotIndex, txReq, slotDB)
				p.unconfirmedStateDBs.Store(txReq.txIndex, slotDB)
				p.pendingConfirmChan <- result
			}
			// switched to the other slot.
			if interrupted || p.inConfirmStage2 {
				debug.Handler.EndTrace(region1)
				continue
			}
			// txReq in this Slot have all been executed, try steal one from other slot.
			// as long as the TxReq is runable, we steal it, mark it as stolen
			// steal one by one

			for _, stealTxReq := range p.allTxReqs {
				if stealTxReq.txIndex < startTxIndex {
					continue
				}
				if !atomic.CompareAndSwapInt32(&stealTxReq.runnable, 1, 0) {
					// not swapped: txReq.runnable == 0
					continue
				}
				region2 := debug.Handler.StartTrace("for a stolen TxReq")
				resultUpdateDB := &ParallelTxResult{
					updateSlotDB: true,
					slotIndex:    slotIndex,
					err:          nil,
					txReq:        stealTxReq,
					keepSystem:   stealTxReq.systemAddrRedo,
				}
				p.txResultChan <- resultUpdateDB
				slotDB := <-curSlot.slotdbChan
				if slotDB == nil { // block is processed
					debug.Handler.EndTrace(region2)
					break
				}

				result := p.executeInSlot(slotIndex, stealTxReq, slotDB)
				p.unconfirmedStateDBs.Store(stealTxReq.txIndex, slotDB)
				p.pendingConfirmChan <- result
				debug.Handler.EndTrace(region2)
			}

		}
		debug.Handler.EndTrace(region1)

		/*
			// disable dynamic right now.
			else if dispatchPolicy == dispatchPolicyDynamic {
				if txReq.slotDB == nil {
					result := &ParallelTxResult{
						updateSlotDB: true,
						slotIndex:    slotIndex,
						err:          nil,
						txReq:        txReq,
					}
					p.txResultChan <- result
					txReq.slotDB = <-curSlot.slotdbChan
				}
				result := p.executeInSlot(slotIndex, txReq)
				p.unconfirmedStateDBs.Store(txReq.txIndex, txReq.slotDB)
				p.pendingConfirmChan <- result
			}
		*/
	}
}

// clear slot state for each block.
func (p *ParallelStateProcessor) resetState(txNum int, statedb *state.StateDB) {
	if txNum == 0 {
		return
	}
	p.mergedTxIndex = -1
	p.debugConflictRedoNum = 0
	p.inConfirmStage2 = false
	// p.txReqAccountSorted = make(map[common.Address][]*ParallelTxRequest) // fixme: to be reused?

	statedb.PrepareForParallel()
	p.allTxReqs = make([]*ParallelTxRequest, 0)
	p.slotDBsToRelease = make([]*state.ParallelStateDB, 0, txNum)

	/*
		stateDBsToRelease := p.slotDBsToRelease
			go func() {
				for _, slotDB := range stateDBsToRelease {
					slotDB.SlotDBPutSyncPool()
				}
			}()
	*/
	for _, slot := range p.slotState {
		slot.pendingTxReqList = make([]*ParallelTxRequest, 0)
		slot.activatedId = 0
	}
	p.unconfirmedStateDBs = new(sync.Map) // make(map[int]*state.ParallelStateDB)
	p.pendingConfirmResults = make(map[int][]*ParallelTxResult, 200)
	p.txReqExecuteRecord = make(map[int]int, 200)
	p.txReqPrefetchRecord = make(map[int]struct{}, 200)
	p.txReqExecuteCount = 0
}

// Implement BEP-130: Parallel Transaction Execution.
func (p *ParallelStateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (*state.StateDB, types.Receipts, []*types.Log, uint64, error) {
	debug.Handler.EnableTraceCapture(block.Header().Number.Uint64())
	traceMsg := "ProcessParallel " + block.Header().Number.String()
	defer debug.Handler.StartRegionAuto(traceMsg)()

	var (
		usedGas = new(uint64)
		header  = block.Header()
		gp      = new(GasPool).AddGas(block.GasLimit())
	)
	var receipts = make([]*types.Receipt, 0)
	txNum := len(block.Transactions())
	p.resetState(txNum, statedb)
	if txNum > 0 {
		log.Info("ProcessParallel", "block", header.Number, "txNum", txNum)
	}

	// Iterate over and process the individual transactions
	posa, isPoSA := p.engine.(consensus.PoSA)
	commonTxs := make([]*types.Transaction, 0, txNum)
	// usually do have two tx, one for validator set contract, another for system reward contract.
	systemTxs := make([]*types.Transaction, 0, 2)

	signer, _, bloomProcessor := p.preExecute(block, statedb, cfg, true)
	// var txReqs []*ParallelTxRequest
	for i, tx := range block.Transactions() {
		if isPoSA {
			if isSystemTx, err := posa.IsSystemTransaction(tx, block.Header()); err != nil {
				return statedb, nil, nil, 0, err
			} else if isSystemTx {
				systemTxs = append(systemTxs, tx)
				continue
			}
		}

		// can be moved it into slot for efficiency, but signer is not concurrent safe
		// Parallel Execution 1.0&2.0 is for full sync mode, Nonce PreCheck is not necessary
		// And since we will do out-of-order execution, the Nonce PreCheck could fail.
		// We will disable it and leave it to Parallel 3.0 which is for validator mode
		msg, err := tx.AsMessageNoNonceCheck(signer)
		if err != nil {
			return statedb, nil, nil, 0, err
		}

		// parallel start, wrap an exec message, which will be dispatched to a slot
		txReq := &ParallelTxRequest{
			txIndex:         i,
			staticSlotIndex: -1,
			tx:              tx,
			gasLimit:        block.GasLimit(), // gp.Gas().
			msg:             msg,
			block:           block,
			vmConfig:        cfg,
			bloomProcessor:  bloomProcessor,
			usedGas:         usedGas,
			curTxChan:       make(chan int, 1),
			systemAddrRedo:  false, // set to true, when systemAddr access is detected.
			runnable:        1,     // 0: not runnable, 1: runnable
		}
		p.allTxReqs = append(p.allTxReqs, txReq)
	}

	if dispatchPolicy == dispatchPolicyStatic {
		p.doStaticDispatch(statedb, p.allTxReqs) // todo: put txReqs in unit?
		// after static dispatch, we notify the slot to work.
		for _, slot := range p.slotState {
			slot.pendingTxReqChan <- nil
		}
		// wait until all Txs have processed.
		for {
			if len(commonTxs)+len(systemTxs) == txNum {
				break
			}
			result := p.waitUntilNextTxDone(statedb, gp)
			// update tx result
			if result.err != nil {
				log.Error("ProcessParallel a failed tx", "resultSlotIndex", result.slotIndex,
					"resultTxIndex", result.txReq.txIndex, "result.err", result.err)
				return statedb, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", result.txReq.txIndex, result.txReq.tx.Hash().Hex(), result.err)
			}
			commonTxs = append(commonTxs, result.txReq.tx)
			receipts = append(receipts, result.receipt)
		}
		// wait unitl all slot are stopped
		region := debug.Handler.StartTrace("waitSlotIidle")
		for i, slot := range p.slotState {
			log.Debug("ProcessParallel to stop", "slotIndex", i)
			slot.stopChan <- struct{}{}
			slot.stopShadowChan <- struct{}{}
			stopCount := 0
			for {
				select {
				case updateDB := <-p.txResultChan: // in case a slot is requesting a new DB...
					if updateDB.updateSlotDB {
						slotState := p.slotState[updateDB.slotIndex]
						log.Debug("ProcessParallel try to update slot db", "slotIndex", updateDB.slotIndex)
						slotState.slotdbChan <- nil
						continue
					}
				case <-p.stopSlotChan:
					stopCount++
				}
				if stopCount == 2 {
					break
				}
			}
			log.Debug("ProcessParallel shadow slot stopped", "slotIndex", i)
		}
		for {
			if len(p.txResultChan) > 0 { // drop prefetch addr?
				<-p.txResultChan
			}
		}
		// wait until the confirm routine is stopped
		p.stopConfirmChan <- struct{}{}
		<-p.stopSlotChan
		log.Debug("ProcessParallel stopped confirm routine")
		debug.Handler.EndTrace(region)
		p.stopConfirmStage2Chan <- struct{}{}
		<-p.stopSlotChan
	}
	/*
		else if dispatchPolicy == dispatchPolicyDynamic {
			for _, txReq := range txReqs {
				// to optimize the for { for {} } loop code style? it is ok right now.
				for {
					if p.queueSameFromAddress(txReq) {
						break
					}
					if p.queueSameToAddress(txReq) {
						break
					}
					// if idle slot available, just dispatch and process next tx.
					if p.dispatchToHungrySlot(statedb, txReq) {
						break
					}
					log.Debug("ProcessParallel no slot available, wait", "txIndex", txReq.txIndex)
					// no idle slot, wait until a tx is executed and merged.
					result := p.waitUntilNextTxDone(statedb, gp)

					// update tx result
					if result.err != nil {
						log.Warn("ProcessParallel a failed tx", "resultSlotIndex", result.slotIndex,
							"resultTxIndex", result.txReq.txIndex, "result.err", result.err)
						return statedb, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", result.txReq.txIndex, result.txReq.tx.Hash().Hex(), result.err)
					}
					commonTxs = append(commonTxs, result.txReq.tx)
					receipts = append(receipts, result.receipt)
				}
			}
			// wait until all tx request are done
			for len(commonTxs)+len(systemTxs) < txNum {
				result := p.waitUntilNextTxDone(statedb, gp)

				// update tx result
				if result.err != nil {
					log.Error("ProcessParallel a failed tx", "resultSlotIndex", result.slotIndex,
						"resultTxIndex", result.txReq.txIndex, "result.err", result.err)
					return statedb, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", result.txReq.txIndex, result.txReq.tx.Hash().Hex(), result.err)
				}

				commonTxs = append(commonTxs, result.txReq.tx)
				receipts = append(receipts, result.receipt)
			}
		}
	*/

	// len(commonTxs) could be 0, such as: https://bscscan.com/block/14580486
	if len(commonTxs) > 0 {
		log.Info("ProcessParallel tx all done", "block", header.Number, "usedGas", *usedGas,
			"txNum", txNum,
			"len(commonTxs)", len(commonTxs),
			"conflictNum", p.debugConflictRedoNum,
			"redoRate(%)", 100*(p.debugConflictRedoNum)/len(commonTxs))
	}
	allLogs, err := p.postExecute(block, statedb, &commonTxs, &receipts, &systemTxs, usedGas, bloomProcessor)
	return statedb, receipts, allLogs, *usedGas, err
}

// Before transactions are executed, do shared preparation for Process() & ProcessParallel()
func (p *StateProcessor) preExecute(block *types.Block, statedb *state.StateDB, cfg vm.Config, parallel bool) (types.Signer, *vm.EVM, *AsyncReceiptBloomGenerator) {
	signer := types.MakeSigner(p.bc.chainConfig, block.Number())
	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	// Handle upgrade build-in system contract code
	systemcontracts.UpgradeBuildInSystemContract(p.config, block.Number(), statedb)

	// with parallel mode, vmenv will be created inside of slot
	var vmenv *vm.EVM
	if !parallel {
		blockContext := NewEVMBlockContext(block.Header(), p.bc, nil)
		vmenv = vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
	}

	// initialise bloom processors
	bloomProcessor := NewAsyncReceiptBloomGenerator(len(block.Transactions()))
	statedb.MarkFullProcessed()

	return signer, vmenv, bloomProcessor
}

func (p *StateProcessor) postExecute(block *types.Block, statedb *state.StateDB, commonTxs *[]*types.Transaction,
	receipts *[]*types.Receipt, systemTxs *[]*types.Transaction, usedGas *uint64, bloomProcessor *AsyncReceiptBloomGenerator) ([]*types.Log, error) {
	allLogs := make([]*types.Log, 0, len(*receipts))

	bloomProcessor.Close()

	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	err := p.engine.Finalize(p.bc, block.Header(), statedb, commonTxs, block.Uncles(), receipts, systemTxs, usedGas)
	if err != nil {
		return allLogs, err
	}
	for _, receipt := range *receipts {
		allLogs = append(allLogs, receipt.Logs...)
	}
	return allLogs, nil
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (*state.StateDB, types.Receipts, []*types.Log, uint64, error) {
	debug.Handler.EnableTraceCapture(block.Header().Number.Uint64())
	traceMsg := "Process " + block.Header().Number.String()
	defer debug.Handler.StartRegionAuto(traceMsg)()

	var (
		usedGas = new(uint64)
		header  = block.Header()
		gp      = new(GasPool).AddGas(block.GasLimit())
	)
	var receipts = make([]*types.Receipt, 0)
	txNum := len(block.Transactions())
	if txNum > 0 {
		log.Info("Process", "block", header.Number, "txNum", txNum)
	}
	commonTxs := make([]*types.Transaction, 0, txNum)
	// Iterate over and process the individual transactions
	posa, isPoSA := p.engine.(consensus.PoSA)
	// usually do have two tx, one for validator set contract, another for system reward contract.
	systemTxs := make([]*types.Transaction, 0, 2)

	signer, vmenv, bloomProcessor := p.preExecute(block, statedb, cfg, false)
	for i, tx := range block.Transactions() {
		if isPoSA {
			if isSystemTx, err := posa.IsSystemTransaction(tx, block.Header()); err != nil {
				return statedb, nil, nil, 0, err
			} else if isSystemTx {
				systemTxs = append(systemTxs, tx)
				continue
			}
		}

		msg, err := tx.AsMessage(signer)
		if err != nil {
			return statedb, nil, nil, 0, err
		}
		statedb.Prepare(tx.Hash(), block.Hash(), i)
		receipt, err := applyTransaction(msg, p.config, p.bc, nil, gp, statedb, header, tx, usedGas, vmenv, bloomProcessor)
		if err != nil {
			return statedb, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		commonTxs = append(commonTxs, tx)
		receipts = append(receipts, receipt)
	}

	allLogs, err := p.postExecute(block, statedb, &commonTxs, &receipts, &systemTxs, usedGas, bloomProcessor)
	return statedb, receipts, allLogs, *usedGas, err
}

func applyTransaction(msg types.Message, config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, evm *vm.EVM, receiptProcessors ...ReceiptProcessor) (*types.Receipt, error) {
	traceMsg := "applyTransaction"
	defer debug.Handler.StartRegionAuto(traceMsg)()

	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	result, err := ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, err
	}

	// Update the state with pending changes.
	var root []byte
	if config.IsByzantium(header.Number) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
	}
	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas
	// If the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = statedb.GetLogs(tx.Hash())
	receipt.BlockHash = statedb.BlockHash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(statedb.TxIndex())
	for _, receiptProcessor := range receiptProcessors {
		receiptProcessor.Apply(receipt)
	}
	return receipt, err
}

func applyTransactionStageExecution(msg types.Message, gp *GasPool, statedb *state.ParallelStateDB, evm *vm.EVM) (*vm.EVM, *ExecutionResult, error) {
	defer debug.Handler.StartRegionAuto("applyTransactionStageExecution")()
	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	result, err := ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, nil, err
	}

	return evm, result, err
}

func applyTransactionStageFinalization(evm *vm.EVM, result *ExecutionResult, msg types.Message, config *params.ChainConfig, statedb *state.ParallelStateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, receiptProcessors ...ReceiptProcessor) (*types.Receipt, error) {
	defer debug.Handler.StartRegionAuto("applyTransactionStageFinalization")()
	// Update the state with pending changes.
	var root []byte
	if config.IsByzantium(header.Number) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
	}
	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = statedb.GetLogs(tx.Hash())
	receipt.BlockHash = statedb.BlockHash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(statedb.TxIndex())
	for _, receiptProcessor := range receiptProcessors {
		receiptProcessor.Apply(receipt)
	}
	return receipt, nil
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config, receiptProcessors ...ReceiptProcessor) (*types.Receipt, error) {
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err
	}
	// Create a new context to be used in the EVM environment
	blockContext := NewEVMBlockContext(header, bc, author)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, config, cfg)
	defer func() {
		ite := vmenv.Interpreter()
		vm.EVMInterpreterPool.Put(ite)
		vm.EvmPool.Put(vmenv)
	}()
	return applyTransaction(msg, config, bc, author, gp, statedb, header, tx, usedGas, vmenv, receiptProcessors...)
}
