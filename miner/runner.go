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

package miner

import (
	"bytes"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AresDream/bsc/common"
	"github.com/AresDream/bsc/consensus"
	"github.com/AresDream/bsc/core"
	"github.com/AresDream/bsc/core/state"
	"github.com/AresDream/bsc/core/types"
	"github.com/AresDream/bsc/event"
	"github.com/AresDream/bsc/log"
	"github.com/AresDream/bsc/params"
	mapset "github.com/deckarep/golang-set"
)

// runner is the main object which takes care of submitting new work to consensus engine
// and gathering the sealing result.
type runner struct {
	config      *Config
	chainConfig *params.ChainConfig
	engine      consensus.Engine
	eth         Backend
	chain       *core.BlockChain

	// Feeds
	pendingLogsFeed    event.Feed
	pendingLogRunsFeed event.Feed

	// Subscriptions
	mux          *event.TypeMux
	txsCh        chan core.NewTxsEvent
	txsSub       event.Subscription
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription
	chainSideCh  chan core.ChainSideEvent
	chainSideSub event.Subscription

	// Channels
	newWorkCh          chan *newWorkReq
	taskCh             chan *task
	resultCh           chan *types.Block
	startCh            chan struct{}
	exitCh             chan struct{}
	resubmitIntervalCh chan time.Duration
	resubmitAdjustCh   chan *intervalAdjust

	current      *environment                 // An environment for current running cycle.
	localUncles  map[common.Hash]*types.Block // A set of side blocks generated locally as the possible uncle blocks.
	remoteUncles map[common.Hash]*types.Block // A set of side blocks as the possible uncle blocks.

	mu       sync.RWMutex // The lock used to protect the coinbase and extra fields
	coinbase common.Address
	extra    []byte

	pendingMu    sync.RWMutex
	pendingTasks map[common.Hash]*task

	snapshotMu    sync.RWMutex // The lock used to protect the block snapshot and state snapshot
	snapshotBlock *types.Block
	snapshotState *state.StateDB

	// atomic status counters
	running int32 // The indicator whether the consensus engine is running or not.
	newTxs  int32 // New arrival transaction count since last sealing work submitting.

	// noempty is the flag used to control whether the feature of pre-seal empty
	// block is enabled. The default value is false(pre-seal is enabled by default).
	// But in some special scenario the consensus engine will seal blocks instantaneously,
	// in this case this feature will add all empty blocks into canonical chain
	// non-stop and no real transaction will be included.
	noempty uint32

	// External functions
	isLocalBlock func(block *types.Block) bool // Function used to determine whether the specified block is mined by local miner.

	// Test hooks
	newTaskHook  func(*task)                        // Method to call upon receiving a new sealing task.
	skipSealHook func(*task) bool                   // Method to decide whether skipping the sealing.
	fullTaskHook func()                             // Method to call before pushing the full sealing task.
	resubmitHook func(time.Duration, time.Duration) // Method to call upon updating resubmitting interval.
}

func newRunner(config *Config, chainConfig *params.ChainConfig, engine consensus.Engine, eth Backend, mux *event.TypeMux, isLocalBlock func(*types.Block) bool, init bool) *runner {
	runner := &runner{
		config:             config,
		chainConfig:        chainConfig,
		engine:             engine,
		eth:                eth,
		mux:                mux,
		chain:              eth.BlockChain(),
		isLocalBlock:       isLocalBlock,
		localUncles:        make(map[common.Hash]*types.Block),
		remoteUncles:       make(map[common.Hash]*types.Block),
		pendingTasks:       make(map[common.Hash]*task),
		txsCh:              make(chan core.NewTxsEvent, txChanSize),
		chainHeadCh:        make(chan core.ChainHeadEvent, chainHeadChanSize),
		chainSideCh:        make(chan core.ChainSideEvent, chainSideChanSize),
		newWorkCh:          make(chan *newWorkReq),
		taskCh:             make(chan *task),
		resultCh:           make(chan *types.Block, resultQueueSize),
		exitCh:             make(chan struct{}),
		startCh:            make(chan struct{}, 1),
		resubmitIntervalCh: make(chan time.Duration),
		resubmitAdjustCh:   make(chan *intervalAdjust, resubmitAdjustChanSize),
	}
	// Subscribe NewTxsEvent for tx pool
	runner.txsSub = eth.TxPool().SubscribeNewTxsEvent(runner.txsCh)
	// Subscribe events for blockchain
	runner.chainHeadSub = eth.BlockChain().SubscribeChainHeadEvent(runner.chainHeadCh)
	runner.chainSideSub = eth.BlockChain().SubscribeChainSideEvent(runner.chainSideCh)

	// Sanitize recommit interval if the user-specified one is too short.
	recommit := runner.config.Recommit
	if recommit < minRecommitInterval {
		log.Warn("Sanitizing miner recommit interval", "provided", recommit, "updated", minRecommitInterval)
		recommit = minRecommitInterval
	}

	go runner.mainLoop()
	go runner.newWorkLoop(recommit)

	// Submit first work to initialize pending state.
	if init {
		runner.startCh <- struct{}{}
	}
	return runner
}

// setEtherbase sets the etherbase used to initialize the block coinbase field.
func (r *runner) setEtherbase(addr common.Address) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.coinbase = addr
}

// setExtra sets the content used to initialize the block extra field.
func (r *runner) setExtra(extra []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extra = extra
}

// pending returns the pending state and corresponding block.
func (r *runner) pending() (*types.Block, *state.StateDB) {
	// return a snapshot to avoid contention on currentMu mutex
	r.snapshotMu.RLock()
	defer r.snapshotMu.RUnlock()
	if r.snapshotState == nil {
		return nil, nil
	}
	return r.snapshotBlock, r.snapshotState.Copy()
}

// pendingBlock returns pending block.
func (r *runner) pendingBlock() *types.Block {
	// return a snapshot to avoid contention on currentMu mutex
	r.snapshotMu.RLock()
	defer r.snapshotMu.RUnlock()
	return r.snapshotBlock
}

// start sets the running status as 1 and triggers new work submitting.
func (r *runner) start() {
	atomic.StoreInt32(&r.running, 1)
	r.startCh <- struct{}{}
}

// stop sets the running status as 0.
func (r *runner) stop() {
	atomic.StoreInt32(&r.running, 0)
}

// isRunning returns an indicator whether runner is running or not.
func (r *runner) isRunning() bool {
	return atomic.LoadInt32(&r.running) == 1
}

// close terminates all background threads maintained by the runner.
// Note the runner does not support being closed multiple times.
func (r *runner) close() {
	atomic.StoreInt32(&r.running, 0)
	close(r.exitCh)
}

// mainLoop is a standalone goroutine to regenerate the sealing task based on the received event.
func (r *runner) mainLoop() {
	defer r.txsSub.Unsubscribe()
	defer r.chainHeadSub.Unsubscribe()
	defer r.chainSideSub.Unsubscribe()

	for {
		select {
		case req := <-r.newWorkCh:
			r.commitNewWork(req.interrupt, req.noempty, req.timestamp)
		case ev := <-r.txsCh:
			// Apply transactions to the pending state
			if r.current != nil {
				r.mu.RLock()
				coinbase := r.coinbase
				r.mu.RUnlock()

				txs := make(map[common.Address]types.Transactions)
				for _, tx := range ev.Txs {
					log.Info("[Runner] tx:", tx.Hash().String())
					acc, _ := types.Sender(r.current.signer, tx)
					txs[acc] = append(txs[acc], tx)
				}
				txset := types.NewTransactionsByPriceAndNonce(r.current.signer, txs)

				r.commitTransactions(txset, coinbase, nil)

			} else {
				// Special case, if the consensus engine is 0 period clique(dev mode),
				// submit mining work here since all empty submission will be rejected
				// by clique. Of course the advance sealing(empty submission) is disabled.
				if r.chainConfig.Clique != nil && r.chainConfig.Clique.Period == 0 {
					r.commitNewWork(nil, true, time.Now().Unix())
				}
			}
		// System stopped
		case <-r.exitCh:
			return
		case <-r.txsSub.Err():
			return
		case <-r.chainHeadSub.Err():
			return
		case <-r.chainSideSub.Err():
			return
		}
	}
}

func (r *runner) commitTransaction(tx *types.Transaction, coinbase common.Address) ([]*types.Log, error) {
	snap := r.current.state.Snapshot()

	receipt, err := core.ApplyTransaction(r.chainConfig, r.chain, &coinbase, r.current.gasPool, r.current.state, r.current.header, tx, &r.current.header.GasUsed, *r.chain.GetVMConfig())
	if err != nil {
		r.current.state.RevertToSnapshot(snap)
		return nil, err
	}

	//r.current.txs = append(r.current.txs, tx)
	//r.current.receipts = append(r.current.receipts, receipt)

	return receipt.Logs, nil
}

func (r *runner) commitTransactions(txs *types.TransactionsByPriceAndNonce, coinbase common.Address, interrupt *int32) bool {
	// Short circuit if current is nil
	if r.current == nil {
		return true
	}
	if r.current.gasPool == nil {
		r.current.gasPool = new(core.GasPool).AddGas(r.current.header.GasLimit)
	}

	var coalescedLogs []*types.Log
	gasPrice := make(map[common.Hash]*big.Int)
	for {
		// If we don't have enough gas for any further transactions then we're done
		//if r.current.gasPool.Gas() < params.TxGas {
		//	log.Trace("Not enough gas for further transactions", "have", r.current.gasPool, "want", params.TxGas)
		//	break
		//}
		// Retrieve the next transaction and abort if all done
		tx := txs.Peek()
		//log.Info("tx:",tx)
		if tx == nil {
			break
		}

		// Start executing the transaction
		r.current.state.Prepare(tx.Hash(), common.Hash{}, r.current.tcount)
		logs, err := r.commitTransaction(tx, coinbase)
		switch {
		//case errors.Is(err, core.ErrGasLimitReached):
		// Pop the current out-of-gas transaction without shifting in the next from the account
		//	log.Trace("Gas limit exceeded for current block", "sender", from)
		//	txs.Pop()

		//case errors.Is(err, core.ErrNonceTooLow):
		// New head notification data race between the transaction pool and miner, shift
		//	log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
		//	txs.Shift()

		//case errors.Is(err, core.ErrNonceTooHigh):
		// Reorg notification data race between the transaction pool and miner, skip account =
		//	log.Trace("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
		//	txs.Pop()

		case errors.Is(err, nil):
			// Everything ok, collect the logs and shift in the next transaction from the same account
			coalescedLogs = append(coalescedLogs, logs...)
			r.current.tcount++
			gasPrice[tx.Hash()] = tx.GasPrice()
			txs.Shift()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			//coalescedLogs = append(coalescedLogs, logs...)
			//r.current.tcount++
			log.Debug("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)
			//gasPrice[tx.Hash()] = tx.GasPrice()
			txs.Shift()
		}
	}

	if len(coalescedLogs) > 0 {
		// We don't push the pendingLogsEvent while we are mining. The reason is that
		// when we are mining, the runner will regenerate a mining block every 3 seconds.
		// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.

		// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
		// logs by filling in the block hash when the block was mined by the local miner. This can
		// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
		cpyrun := make([]*types.LogRun, len(coalescedLogs))
		for i, l := range coalescedLogs {
			cpyrun[i] = new(types.LogRun)
			cpyrun[i].Address = l.Address
			cpyrun[i].BlockHash = l.BlockHash
			cpyrun[i].BlockNumber = l.BlockNumber
			cpyrun[i].Data = append([]byte(nil), l.Data...)
			cpyrun[i].Index = l.Index
			cpyrun[i].Removed = l.Removed
			cpyrun[i].Topics = l.Topics
			cpyrun[i].TxHash = l.TxHash
			cpyrun[i].TxIndex = l.TxIndex
			cpyrun[i].GasPrice = gasPrice[l.TxHash]
		}
		r.pendingLogRunsFeed.Send(cpyrun)
	}
	return false
}

// makeCurrent creates a new environment for the current cycle.
func (r *runner) makeCurrent(parent *types.Block, header *types.Header) error {
	state, err := r.chain.StateAt(parent.Root())
	if err != nil {
		return err
	}
	env := &environment{
		signer:    types.NewEIP155Signer(r.chainConfig.ChainID),
		state:     state,
		ancestors: mapset.NewSet(),
		family:    mapset.NewSet(),
		uncles:    mapset.NewSet(),
		header:    header,
	}

	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range r.chain.GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			env.family.Add(uncle.Hash())
		}
		env.family.Add(ancestor.Hash())
		env.ancestors.Add(ancestor.Hash())
	}

	// Keep track of transactions which return errors so they can be removed
	env.tcount = 0
	r.current = env
	return nil
}

// commitNewWork generates several new sealing tasks based on the parent block.
func (r *runner) commitNewWork(interrupt *int32, noempty bool, timestamp int64) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	parent := r.chain.CurrentBlock()

	if parent.Time() >= uint64(timestamp) {
		timestamp = int64(parent.Time() + 1)
	}
	// this will ensure we're not going off too far in the future
	if now := time.Now().Unix(); timestamp > now+1 {
		wait := time.Duration(timestamp-now) * time.Second
		log.Info("Mining too far in the future", "wait", common.PrettyDuration(wait))
		time.Sleep(wait)
	}

	num := parent.Number()
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		GasLimit:   core.CalcGasLimit(parent, r.config.GasFloor, r.config.GasCeil),
		Extra:      r.extra,
		Time:       uint64(timestamp),
	}
	// Only set the coinbase if our consensus engine is running (avoid spurious block rewards)
	if r.isRunning() {
		if r.coinbase == (common.Address{}) {
			log.Error("Refusing to mine without etherbase")
			return
		}
		header.Coinbase = r.coinbase
	}
	if err := r.engine.Prepare(r.chain, header); err != nil {
		log.Error("Failed to prepare header for mining", "err", err)
		return
	}
	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := r.chainConfig.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if r.chainConfig.DAOForkSupport {
				header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			} else if bytes.Equal(header.Extra, params.DAOForkBlockExtra) {
				header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
	}
	// Could potentially happen if starting to mine in an odd state.
	err := r.makeCurrent(parent, header)
	if err != nil {
		log.Error("Failed to create mining context", "err", err)
		return
	}
	// Create the current work task and check any fork transitions needed
}

// newWorkLoop is a standalone goroutine to submit new mining work upon received events.
func (r *runner) newWorkLoop(recommit time.Duration) {
	var (
		interrupt   *int32
		minRecommit = recommit // minimal resubmit interval specified by user.
		timestamp   int64      // timestamp for each round of mining.
	)

	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C // discard the initial tick

	// commit aborts in-flight transaction execution with given signal and resubmits a new one.
	commit := func(noempty bool, s int32) {
		if interrupt != nil {
			atomic.StoreInt32(interrupt, s)
		}
		interrupt = new(int32)
		r.newWorkCh <- &newWorkReq{interrupt: interrupt, noempty: noempty, timestamp: timestamp}
		timer.Reset(recommit)
		atomic.StoreInt32(&r.newTxs, 0)
	}
	// recalcRecommit recalculates the resubmitting interval upon feedback.
	recalcRecommit := func(target float64, inc bool) {
		var (
			prev = float64(recommit.Nanoseconds())
			next float64
		)
		if inc {
			next = prev*(1-intervalAdjustRatio) + intervalAdjustRatio*(target+intervalAdjustBias)
			// Recap if interval is larger than the maximum time interval
			if next > float64(maxRecommitInterval.Nanoseconds()) {
				next = float64(maxRecommitInterval.Nanoseconds())
			}
		} else {
			next = prev*(1-intervalAdjustRatio) + intervalAdjustRatio*(target-intervalAdjustBias)
			// Recap if interval is less than the user specified minimum
			if next < float64(minRecommit.Nanoseconds()) {
				next = float64(minRecommit.Nanoseconds())
			}
		}
		recommit = time.Duration(int64(next))
	}
	// clearPending cleans the stale pending tasks.
	clearPending := func(number uint64) {
		r.pendingMu.Lock()
		for h, t := range r.pendingTasks {
			if t.block.NumberU64()+staleThreshold <= number {
				delete(r.pendingTasks, h)
			}
		}
		r.pendingMu.Unlock()
	}

	for {
		select {
		case <-r.startCh:
			clearPending(r.chain.CurrentBlock().NumberU64())
			timestamp = time.Now().Unix()
			commit(false, commitInterruptNewHead)

		case head := <-r.chainHeadCh:
			if !r.isRunning() {
				continue
			}
			clearPending(head.Block.NumberU64())
			timestamp = time.Now().Unix()
			commit(false, commitInterruptNewHead)

		case <-timer.C:
			// If mining is running resubmit a new work cycle periodically to pull in
			// higher priced transactions. Disable this overhead for pending blocks.
			if r.isRunning() && ((r.chainConfig.Ethash != nil) || (r.chainConfig.Clique != nil &&
				r.chainConfig.Clique.Period > 0) || (r.chainConfig.Parlia != nil && r.chainConfig.Parlia.Period > 0)) {
				// Short circuit if no new transaction arrives.
				if atomic.LoadInt32(&r.newTxs) == 0 {
					timer.Reset(recommit)
					continue
				}
				commit(true, commitInterruptResubmit)
			}

		case interval := <-r.resubmitIntervalCh:
			// Adjust resubmit interval explicitly by user.
			if interval < minRecommitInterval {
				log.Warn("Sanitizing miner recommit interval", "provided", interval, "updated", minRecommitInterval)
				interval = minRecommitInterval
			}
			log.Info("Miner recommit interval update", "from", minRecommit, "to", interval)
			minRecommit, recommit = interval, interval

			if r.resubmitHook != nil {
				r.resubmitHook(minRecommit, recommit)
			}

		case adjust := <-r.resubmitAdjustCh:
			// Adjust resubmit interval by feedback.
			if adjust.inc {
				before := recommit
				recalcRecommit(float64(recommit.Nanoseconds())/adjust.ratio, true)
				log.Trace("Increase miner recommit interval", "from", before, "to", recommit)
			} else {
				before := recommit
				recalcRecommit(float64(minRecommit.Nanoseconds()), false)
				log.Trace("Decrease miner recommit interval", "from", before, "to", recommit)
			}

			if r.resubmitHook != nil {
				r.resubmitHook(minRecommit, recommit)
			}

		case <-r.exitCh:
			return
		}
	}
}
