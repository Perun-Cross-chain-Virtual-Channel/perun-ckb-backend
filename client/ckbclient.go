package client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/nervosnetwork/ckb-sdk-go/v2/address"
	"github.com/nervosnetwork/ckb-sdk-go/v2/collector"
	"github.com/nervosnetwork/ckb-sdk-go/v2/indexer"
	"github.com/nervosnetwork/ckb-sdk-go/v2/rpc"
	ckbtransaction "github.com/nervosnetwork/ckb-sdk-go/v2/transaction"
	"github.com/nervosnetwork/ckb-sdk-go/v2/types"
	"github.com/nervosnetwork/ckb-sdk-go/v2/types/molecule"
	"perun.network/go-perun/channel"
	"perun.network/go-perun/wallet"
	"perun.network/perun-ckb-backend/backend"
	ckbchannel "perun.network/perun-ckb-backend/channel"
	"perun.network/perun-ckb-backend/channel/asset"
	"perun.network/perun-ckb-backend/encoding"
	molecule2 "perun.network/perun-ckb-backend/encoding/molecule"
	"perun.network/perun-ckb-backend/transaction"
	ckbaddress "perun.network/perun-ckb-backend/wallet/address"
)

var ErrNoChannelLiveCell = errors.New("no channel live cell found")

const SearchIndexerLimit = 1199

// contentionRetries bounds how many times a state-mutating operation rebuilds
// and resubmits after losing a race for its input cells. Between attempts we
// re-read on-chain state, so a competing transaction that already achieved our
// goal (or merely consumed our fee cell) is handled on the next pass.
const contentionRetries = 6

// contentionRetryDelay is the pause between contention retries, chosen to let a
// competing tx get mined and the ckb-indexer catch up before we re-read.
const contentionRetryDelay = time.Second

// isCellContentionError reports whether err indicates the transaction lost a
// race for its input cells (CKB has no per-account nonce): an under-priced RBF
// rejection against a pending tx on the same cell, or an input already
// spent/unknown because the competitor was mined first.
func isCellContentionError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "PoolRejectedRBF") ||
		strings.Contains(msg, "TransactionFailedToResolve") ||
		strings.Contains(msg, "Dead(OutPoint") ||
		strings.Contains(msg, "Unknown(OutPoint")
}

type BlockNumber = uint64

type CKBClient interface {
	// Start starts a new channel on-chain with the given parameters and initial state.
	// It returns the resulting channel token or an error.
	// Start should block until the starting transaction is committed on-chain.
	// The implementation can assume that Start will only ever be performed by Party A.
	Start(ctx context.Context, params *channel.Params, state *channel.State) (*types.Script, error)

	// Abort aborts the channel with the given channel token.
	Abort(ctx context.Context, pcts *types.Script, params *channel.Params, state *channel.State) error

	// Fund funds the channel with the given channel token. The implementation can assume that Fund will only ever
	// be performed by Party B.
	Fund(ctx context.Context, pcts *types.Script, state *channel.State, params *channel.Params) error

	// Dispute registers a dispute for the channel with the given channel ID on chain.
	// It should register the given state with the given signatures as witness.
	// Note: The given signatures are padded (see encoding.NewMoleculeSignature).
	Dispute(ctx context.Context, id channel.ID, state *channel.State, sigs []wallet.Sig, params *channel.Params) error

	// DisputeVC registers a dispute for a channel and its virtual channel with the given channel ID on chain.
	// It should register the given state with the given signatures as witness.
	// Note: The given signatures are padded (see encoding.NewMoleculeSignature).
	DisputeVC(ctx context.Context, vcID, parentID channel.ID, vcState, parentState *channel.State, vcParams, parentParams *channel.Params, sigs, parentSigs []wallet.Sig, indexMap []channel.Index) error

	// Close closes the channel with the given channel ID on chain.
	// The implementation can assume that the given state is final.
	// Note: The given signatures are padded (see encoding.NewMoleculeSignature).
	Close(ctx context.Context, id channel.ID, state *channel.State, sigs []wallet.Sig, params *channel.Params) error

	// ForceClose closes the channel with the given channel ID on chain.
	// The implementation can assume that the channel has already been disputed and that the challenge duration
	// is expired in real-time, though it may be necessary to wait until a block is produced with a timestamp strictly
	// later than the expiration of the challenge duration.
	ForceClose(ctx context.Context, id channel.ID, state *channel.State, params *channel.Params) error

	// ForceCloseWithVC closes the channel with the given channel ID on chain given that the channel has a virtual channel as a child.
	// The implementation can assume that the channel has already been disputed and that the challenge duration
	// is expired in real-time, though it may be necessary to wait until a block is produced with a timestamp strictly
	// later than the expiration of the challenge duration.
	ForceCloseWithVC(ctx context.Context, id channel.ID, vcid channel.ID, state *channel.State, vcstate *channel.State, sigs []wallet.Sig, params *channel.Params, indexMap []channel.Index) error

	// Coordinate locks the channel with the given id to the canonical state by
	// submitting a `coordinate` action on-chain. The implementation can assume
	// the channel has been disputed and the challenge window has elapsed
	// (the contract verifies this; if it has not, the tx will revert).
	// canonicalState.Version must be >= the on-chain state's version.
	// sigs are the two participant signatures over canonicalState; coordSig is
	// the coordinator signature over canonicalState. Note: the given signatures
	// are padded (see encoding.NewMoleculeSignature).
	Coordinate(ctx context.Context, id channel.ID, canonicalState *channel.State, sigs []wallet.Sig, coordSig wallet.Sig, params *channel.Params) error

	// CoordinateVC locks both the parent ledger channel and its virtual channel
	// to their canonical states in a single transaction. Mirrors the eth-backend
	// recursive coordinate. parentSigs/vcSigs are participant signatures over
	// the respective canonical states; parentCoordSig/vcCoordSig are the
	// coordinator signatures.
	CoordinateVC(ctx context.Context, parentID, vcID channel.ID, parentState, vcState *channel.State, parentSigs, vcSigs []wallet.Sig, parentCoordSig, vcCoordSig wallet.Sig, parentParams, vcParams *channel.Params) error

	// GetChannelWithID returns an on-chain channel with the given channel ID.
	// Note: Only the channel ID field in the state must be checked, as the pcts verifies the integrity of said
	// field upon channel start (i.e. that it is equal to the hash of the channel parameters).
	// If there are multiple channels with the same ID, the implementation can return any of them, but the returned
	// constants and status must belong to the same channel.
	// Iff all RPC calls succeed but no live cell for the given channel ID is found, the returned error is
	// ErrNoChannelLiveCell.
	GetChannelWithID(ctx context.Context, id channel.ID) (BlockNumber, *types.Script, *molecule.ChannelConstants, *molecule.ChannelStatus, error)

	// GetChannelWithExactPCTS returns the on-chain channel status for the given type script.
	// Iff all RPC calls succeed but no live cell for the given channel ID is found, the returned error is
	// ErrNoChannelLiveCell.
	GetChannelWithExactPCTS(ctx context.Context, pcts *types.Script) (BlockNumber, *molecule.ChannelStatus, error)

	// GetBlockTime returns the timestamp of the block with the given block number.
	GetBlockTime(ctx context.Context, blockNumber BlockNumber) (time.Time, error)
}

func retryRPC[T any](ctx context.Context, n int, delay time.Duration, fn func() (T, error)) (T, error) {
	var zero T
	var err error
	for i := 0; i < n; i++ {
		var result T
		result, err = fn()
		if err == nil {
			return result, nil
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return zero, fmt.Errorf("RPC canceled or deadline exceeded: %w", err)
		}

		log.Printf("RPC attempt %d/%d failed: %v", i+1, n, err)
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(delay):
		}
	}
	return zero, fmt.Errorf("RPC failed after %d attempts: %w", n, err)
}

type Client struct {
	client rpc.Client

	signer     backend.Signer
	deployment backend.Deployment

	psh     *transaction.PerunScriptHandler
	cache   StableScriptCache
	vccache StableScriptCache

	// txMu serializes state-mutating operations on this client so two concurrent
	// operations (e.g. a watcher goroutine racing the caller) don't select and
	// spend the same fee/channel cells. Pointer so it stays shared across the
	// value-receiver copies of Client.
	txMu *sync.Mutex
}

func NewClient(rpcClient rpc.Client, signer backend.Signer, deployment backend.Deployment) (*Client, error) {
	psh := transaction.NewPerunScriptHandlerWithDeployment(deployment)
	return &Client{
		client:     rpcClient,
		signer:     signer,
		deployment: deployment,
		psh:        psh,
		cache:      NewStableScriptCache(),
		vccache:    NewStableScriptCache(),
		txMu:       &sync.Mutex{},
	}, nil
}

var _ CKBClient = (*Client)(nil)

func (c Client) Start(ctx context.Context, params *channel.Params, state *channel.State) (*types.Script, error) {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	// TODO: Override defaulthash logic.
	log.Println("Start called")
	iter, _, err := c.mkMyCKBCellIterator()
	if err != nil {
		return nil, fmt.Errorf("creating cell iterator: %w", err)
	}
	channelToken, err := c.createOrGetChannelToken(ctx, iter)
	if err != nil {
		return nil, fmt.Errorf("creating channel token: %w", err)
	}
	cid, _ := ckbchannel.Backend.CalcID(params)
	oi := transaction.NewOpenInfo(cid, channelToken, params, state)
	log.Printf("Open Information: token=%v address=%v balances=%v", oi.ChannelToken.Token, oi.State.Assets[0].Address(), oi.State.Balances)
	zeroHash := types.Hash{}
	builder, err := c.newPerunTransactionBuilder(map[types.Hash]collector.CellIterator{zeroHash: iter})
	if err != nil {
		return nil, fmt.Errorf("creating Perun transaction builder: %w", err)
	}
	log.Println("Start: created Perun transaction builder")
	if err := builder.Open(oi); err != nil {
		return nil, fmt.Errorf("creating open transaction: %w", err)
	}
	tx, err := builder.Build(c.signer.Contexts())
	if err != nil {
		log.Println("Start: error while building open Tx")
		return nil, fmt.Errorf("building open transaction: %w", err)
	}
	log.Println("Start: created open transaction")
	log.Println("Start: built open transaction")
	if err := c.submitTx(ctx, tx); err != nil {
		return nil, fmt.Errorf("submitting transaction: %w", err)
	}

	pcts, err := oi.GetPCTS()
	if err != nil {
		return nil, fmt.Errorf("getting PCTS: %w", err)
	}
	return pcts, nil
}

// newPerunScriptHandler creates a new PerunScriptHandler. The iterator used to
// fetch the live cells for the account associated with this client can be
// injected if it was necessary in the outer scope.
func (c Client) newPerunTransactionBuilder(withIterators map[types.Hash]collector.CellIterator) (*transaction.PerunTransactionBuilder, error) {
	iters, err := iteratorsForDeployment(c.client, c.deployment, c.signer.Address())
	if err != nil {
		return nil, fmt.Errorf("creating cell iterators: %w", err)
	}

	// Override custom specified iterators.
	for hash, iter := range withIterators {
		iters[hash] = iter
	}

	omni := c.signer.Address().Script.CodeHash != c.deployment.DefaultLockScript.CodeHash
	return transaction.NewPerunTransactionBuilderWithDeployment(c.client, c.deployment, iters, c.signer.Address(), omni)
}

func iteratorsForDeployment(cl rpc.Client, deployment backend.Deployment, sender address.Address) (map[types.Hash]collector.CellIterator, error) {
	zeroHash := types.Hash{}
	iters := make(map[types.Hash]collector.CellIterator)
	// Iterator for the lockscript:
	senderString, err := sender.Encode()
	if err != nil {
		return nil, fmt.Errorf("encoding sender address: %w", err)
	}
	iter, err := collector.NewLiveCellIteratorFromAddress(cl, senderString)
	if err != nil {
		return nil, fmt.Errorf("creating cell iterator for default lockscript: %w", err)
	}
	// NOTE: This is to gather CKBytes.
	iters[zeroHash] = NewCKBOnlyIterator(iter)

	// Iterator for udts:
	for _, udt := range deployment.SUDTs {
		searchKey := &indexer.SearchKey{
			Script:           &udt,
			ScriptType:       types.ScriptTypeType,
			ScriptSearchMode: types.ScriptSearchModePrefix,
			WithData:         true,
		}

		// Create the base UDT iterator (fetches all live cells with this type)
		baseIter := collector.NewLiveCellIterator(cl, searchKey)

		// Get sender lock script hash
		senderLockHash := sender.Script.Hash()

		// Filter UDT cells to only use sender's lock script
		filteredIter := NewFilteredCellIterator(baseIter, func(cell *types.CellOutput) bool {
			return cell.Lock.Hash() == senderLockHash
		})

		// Register filtered iterator
		iters[udt.Hash()] = filteredIter
	}
	return iters, nil
}

func (c Client) submitTx(ctx context.Context, tx *ckbtransaction.TransactionWithScriptGroups) error {
	sTx, err := c.signer.SignTransaction(tx)
	// TODO: Why this step?
	// *tx = *sTx
	if err != nil {
		return fmt.Errorf("signing transaction: %w", err)
	}
	return c.sendAndAwait(ctx, sTx)
}

// buildAndSubmitWithRetry rebuilds (re-selecting fresh fee cells) and resubmits
// a transaction on cell-contention errors. For paths where only the fee cell is
// contended; paths whose channel cell is contended re-read state per attempt
// instead (see Dispute/DisputeVC/ForceClose).
func (c Client) buildAndSubmitWithRetry(ctx context.Context, label string, build func() (*ckbtransaction.TransactionWithScriptGroups, error)) error {
	for attempt := 0; ; attempt++ {
		tx, err := build()
		if err != nil {
			return err
		}
		if err := c.submitTx(ctx, tx); err != nil {
			if isCellContentionError(err) && attempt < contentionRetries-1 {
				log.Printf("%s: cell contention (attempt %d), rebuilding: %v", label, attempt+1, err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(contentionRetryDelay):
				}
				continue
			}
			return err
		}
		return nil
	}
}

// submitTxWithArgument submits a transaction whose type is determined by the
// txTypeArgument.
//
//nolint:unused
func (c Client) submitTxWithArgument(ctx context.Context, txTypeArgument ...interface{}) error {
	b, err := c.newPerunTransactionBuilder(nil)
	if err != nil {
		return fmt.Errorf("creating Perun transaction builder: %w", err)
	}

	tx, err := b.Build(txTypeArgument)
	if err != nil {
		return fmt.Errorf("building transaction: %w", err)
	}
	return c.submitTx(ctx, tx)
}

// mkMyCKBCellIterator returns a celliterator together with the associated script
// hash it is meant to be used with.
func (c Client) mkMyCKBCellIterator() (collector.CellIterator, types.Hash, error) {
	defaultLockScript := c.signer.Address().Script
	key := &indexer.SearchKey{
		Script:     defaultLockScript,
		ScriptType: types.ScriptTypeLock,
		// Use `ScriptSearchModeExact` to make sure we only get cells that have no
		// type script set and could be SUDT cells.
		ScriptSearchMode: types.ScriptSearchModeExact,
		Filter:           nil,
		WithData:         true,
	}
	iter := collector.NewLiveCellIterator(c.client, key)
	return NewCKBOnlyIterator(iter), defaultLockScript.Hash(), nil
}

func (c Client) createOrGetChannelToken(ctx context.Context, iter collector.CellIterator) (backend.Token, error) {
	// TODO: This just takes the first available cell. This should be improved in
	// the next version, where we make sure to reuse the funding cells instead
	// of a dedicated cell for the channel token.
	if !iter.HasNext() {
		return backend.Token{}, errors.New("sending account has no funds available")
	}
	for iter.HasNext() {
		txInput := iter.Next()

		// Resolve the full cell to check the lock script
		liveCell, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.CellWithStatus, error) {
			return c.client.GetLiveCell(ctx, txInput.OutPoint, false)
		})

		if err != nil || liveCell == nil || liveCell.Cell == nil {
			log.Println("Skipping cell due to error or nil:", err, liveCell)
			continue
		}

		if liveCell.Cell.Output.Lock.Hash() != c.signer.Address().Script.Hash() {
			log.Println("Skipping cell due to lock hash mismatch:", liveCell.Cell.Output.Lock.Hash(), c.signer.Address().Script.Hash())
			continue
		}

		channelToken := molecule.
			NewChannelTokenBuilder().
			OutPoint(*txInput.OutPoint.Pack()).
			Build()
		return backend.Token{
			Idx:      txInput.OutPoint.Index,
			Outpoint: *txInput.OutPoint.Pack(),
			Token:    channelToken,
		}, nil
	}
	return backend.Token{}, errors.New("no suitable cell found for channel token")
}

func (c Client) Fund(ctx context.Context, pcts *types.Script, state *channel.State, params *channel.Params) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	log.Println("Fund(ckbclient) called")
	channelCell, err := c.getExactChannelLiveCell(ctx, pcts)
	if err != nil {
		return fmt.Errorf("getting channel live cell: %w", err)
	}
	header, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Header, error) {
		return c.client.GetTipHeader(ctx)
	})
	if err != nil {
		return fmt.Errorf("getting tip header: %w", err)
	}
	channelStatus, err := molecule.ChannelStatusFromSlice(channelCell.OutputData, false)
	if err != nil {
		return err
	}
	fi := transaction.NewFundInfo(*channelCell.OutPoint, params, state, pcts, *channelStatus, header.Hash)
	fi.InputChannelCapacity = channelCell.Output.Capacity
	builder, err := c.newPerunTransactionBuilder(nil)
	if err != nil {
		return fmt.Errorf("creating Perun transaction builder: %w", err)
	}
	if err = builder.Fund(fi); err != nil {
		return fmt.Errorf("creating fund transaction: %w", err)
	}
	tx, err := builder.Build(c.signer.Contexts())
	if err != nil {
		return fmt.Errorf("building fund transaction: %w", err)
	}
	return c.submitTx(ctx, tx)
}

func (c Client) Dispute(ctx context.Context, id channel.ID, state *channel.State, sigs []wallet.Sig, params *channel.Params) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	log.Println("Dispute called")

	if len(sigs) != 2 {
		return fmt.Errorf("expected 2 signatures, got %d", len(sigs))
	}

	sigA, err := encoding.NewMoleculeSignature(sigs[0])
	if err != nil {
		return fmt.Errorf("encoding signature A: %w", err)
	}

	sigB, err := encoding.NewMoleculeSignature(sigs[1])
	if err != nil {
		return fmt.Errorf("encoding signature B: %w", err)
	}

	// Retry on cell contention, re-reading state each attempt: once a competitor's
	// dispute lands checkVersion short-circuits to a no-op; a taken fee cell is
	// resolved by rebuilding.
	for attempt := 0; ; attempt++ {
		channelCell, status, err := c.getChannelLiveCellWithCache(ctx, id)
		if err != nil {
			return fmt.Errorf("getting channel live cell: %w", err)
		}

		if !checkVersion(state, status, nil, nil) {
			log.Println("Dispute not needed")
			return nil
		}

		header, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Header, error) {
			return c.client.GetTipHeader(ctx)
		})
		if err != nil {
			return fmt.Errorf("getting tip header: %w", err)
		}

		di := transaction.NewDisputeInfo(*channelCell.OutPoint, *status, state, params, header.Hash, channelCell.Output.Type, *sigA, *sigB)
		di.InputChannelCapacity = channelCell.Output.Capacity

		builder, err := c.newPerunTransactionBuilder(nil)
		if err != nil {
			return fmt.Errorf("creating Perun transaction builder: %w", err)
		}
		if err := builder.Dispute(di); err != nil {
			return fmt.Errorf("creating dispute transaction: %w", err)
		}
		tx, err := builder.Build(c.signer.Contexts())
		if err != nil {
			return fmt.Errorf("building dispute transaction: %w", err)
		}
		if err := c.submitTx(ctx, tx); err != nil {
			if isCellContentionError(err) && attempt < contentionRetries-1 {
				log.Printf("Dispute: cell contention (id=%x attempt %d), re-reading: %v", id[:4], attempt+1, err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(contentionRetryDelay):
				}
				continue
			}
			log.Printf("Dispute: submitTx failed (id=%x): %v", id[:4], err)
			return err
		}
		log.Printf("Dispute: submitted+committed (id=%x ver=%d)", id[:4], state.Version)
		return nil
	}
}

func (c Client) DisputeVC(ctx context.Context, vcID, parentID channel.ID, vcState, parentState *channel.State, vcParams, parentParams *channel.Params, vcSigs, parentSigs []wallet.Sig, indexMap []channel.Index) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	header, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Header, error) {
		return c.client.GetTipHeader(ctx)
	})
	if err != nil {
		return fmt.Errorf("getting tip header: %w", err)
	}

	if len(vcSigs) != 2 {
		return fmt.Errorf("expected 2 signatures, got %d", len(vcSigs))
	}
	sigA, err := encoding.NewMoleculeSignature(vcSigs[0])
	if err != nil {
		return fmt.Errorf("encoding signature A: %w", err)
	}

	sigB, err := encoding.NewMoleculeSignature(vcSigs[1])
	if err != nil {
		return fmt.Errorf("encoding signature B: %w", err)
	}

	if len(parentSigs) != 2 {
		return fmt.Errorf("expected 2 parent signatures, got %d", len(parentSigs))
	}
	parentSigA, err := encoding.NewMoleculeSignature(parentSigs[0])
	if err != nil {
		return fmt.Errorf("encoding signature A: %w", err)
	}

	parentSigB, err := encoding.NewMoleculeSignature(parentSigs[1])
	if err != nil {
		return fmt.Errorf("encoding signature B: %w", err)
	}

	vcDispute := encoding.PackVCDispute(sigA, sigB, parentSigA, parentSigB)

	// Get parents' hashes.
	var parentIDs [2]channel.ID
	for i := 0; i < 2; i++ {
		var hash [32]byte
		startIndex := i * 32
		endIndex := 32 * (i + 1)
		copy(hash[:], vcParams.Aux[startIndex:endIndex])
		parentIDs[i] = hash
	}

	if parentIDs[0] == parentIDs[1] {
		return fmt.Errorf("parent channel IDs are the same: %s", parentIDs[0])
	}

	_, parentHashA, _, _, err := c.GetChannelWithID(ctx, parentIDs[0])
	if err != nil {
		return fmt.Errorf("getting parent channel with ID: %w", err)
	}
	_, parentHashB, _, _, err := c.GetChannelWithID(ctx, parentIDs[1])
	if err != nil {
		return fmt.Errorf("getting parent channel with ID: %w", err)
	}

	var parentVec molecule.ParentsVec
	if parentIDs[0] == parentID {
		parentVecBuilder := molecule.NewParentsVecBuilder()
		parentVecBuilder.Push(molecule.NewParentDataBuilder().
			IdxMap(molecule.NewIndexMapBuilder().
				Nth0(*types.PackByte(byte(indexMap[0]))).
				Nth1(*types.PackByte(byte(indexMap[1]))).Build()).
			PctsHash(*molecule2.PackByte32(parentHashA.Hash())).
			Build())
		parentVecBuilder.Push(molecule.NewParentDataBuilder().
			IdxMap(molecule.NewIndexMapBuilder().
				Nth0(*types.PackByte(byte(indexMap[1]))).
				Nth1(*types.PackByte(byte(indexMap[0]))).Build()).
			PctsHash(*molecule2.PackByte32(parentHashB.Hash())).
			Build())
		parentVec = parentVecBuilder.Build()
	} else if parentIDs[1] == parentID {
		parentVecBuilder := molecule.NewParentsVecBuilder()
		parentVecBuilder.Push(molecule.NewParentDataBuilder().
			IdxMap(molecule.NewIndexMapBuilder().
				Nth0(*types.PackByte(byte(indexMap[1]))).
				Nth1(*types.PackByte(byte(indexMap[0]))).Build()).
			PctsHash(*molecule2.PackByte32(parentHashA.Hash())).
			Build())
		parentVecBuilder.Push(molecule.NewParentDataBuilder().
			IdxMap(molecule.NewIndexMapBuilder().
				Nth0(*types.PackByte(byte(indexMap[0]))).
				Nth1(*types.PackByte(byte(indexMap[1]))).Build()).
			PctsHash(*molecule2.PackByte32(parentHashB.Hash())).
			Build())
		parentVec = parentVecBuilder.Build()
	} else {
		return fmt.Errorf("parent channel ID %s not found in params", parentID)
	}

	// Retry on cell contention: the VC cell is shared by both parents, so a
	// concurrent dispute of either parent can consume it. Re-reading the VC cells
	// each attempt also re-selects the Start/merge/progress branch.
	for attempt := 0; ; attempt++ {
		virtualChannelCells, vcStatuses, err := c.getVirtualChannelLiveCellWithCache(ctx, vcID)
		if err != nil && err != ErrNoChannelLiveCell {
			return fmt.Errorf("looking up virtual channel live cell: %w", err)
		}

		if len(virtualChannelCells) > 1 {
			// Two VC cells (both parents disputed concurrently): merge them into
			// one first, then re-loop to dispute this parent on the merged cell.
			if err := c.mergeVirtualChannelCells(ctx, virtualChannelCells, vcStatuses, &vcDispute); err != nil {
				if isCellContentionError(err) && attempt < contentionRetries-1 {
					log.Printf("DisputeVC: merge contention (vc=%x attempt %d), re-reading: %v", vcID[:4], attempt+1, err)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(contentionRetryDelay):
					}
					continue
				}
				return err
			}
			continue
		}

		var di *transaction.VcDisputeInfo
		if virtualChannelCells == nil {
			// First VC dispute.
			parentCell, status, err := c.getChannelLiveCellWithCache(ctx, parentID)
			if err != nil {
				return fmt.Errorf("getting channel live cell: %w", err)
			}

			// Check parent
			if parentState.Version < molecule2.UnpackUint64(status.State().Version()) {
				return fmt.Errorf("parent state version is not up to date")
			}

			// Construct the VC owner participant using the signer's ACTUAL lock
			// script (not just the default sighash script derived from the pubkey).
			// This ensures VC rent payouts go to the right script for omni-lock /
			// EVMSigner participants, not to a default-sighash address they don't
			// control.
			signerPub := c.signer.PublicKey()
			signerAddr := c.signer.Address()
			signerParticipant := ckbaddress.NewParticipant(signerPub, signerAddr.Script, signerAddr.Script)

			di = transaction.NewVCDisputeInfo(
				parentCell.OutPoint,
				nil,
				status,
				nil,
				vcState,
				parentState,
				vcParams,
				header.Hash,
				parentCell.Output.Type,
				nil,
				*parentSigA, *parentSigB,
				&vcDispute,
				&parentVec,
				true,
				signerParticipant,
			)
			di.InputChannelCapacity = parentCell.Output.Capacity
		} else { // Dispute Progress or update
			parentCell, status, err := c.getChannelLiveCellWithCache(ctx, parentID)
			if err != nil {
				return fmt.Errorf("getting channel live cell: %w", err)
			}

			virtualChannelCell := virtualChannelCells[0]
			vcStatus := vcStatuses[0]

			// Check the states' version to determine if the dispute is needed.
			if !checkVersion(parentState, status, vcState, vcStatus) {
				log.Println("Dispute not needed")
				return nil
			}

			di = transaction.NewVCDisputeInfo(
				parentCell.OutPoint,
				virtualChannelCell.OutPoint,
				status,
				vcStatus,
				vcState,
				parentState,
				vcParams,
				header.Hash,
				parentCell.Output.Type,
				virtualChannelCell.Output.Type,
				*parentSigA, *parentSigB,
				&vcDispute,
				&parentVec,
				false,
				nil,
			)
			di.InputChannelCapacity = parentCell.Output.Capacity
		}

		builder, err := c.newPerunTransactionBuilder(nil)
		if err != nil {
			return fmt.Errorf("creating Perun transaction builder: %w", err)
		}
		if err := builder.DisputeVC(di); err != nil {
			return fmt.Errorf("creating dispute transaction: %w", err)
		}
		tx, err := builder.Build(c.signer.Contexts())
		if err != nil {
			return fmt.Errorf("building dispute transaction: %w", err)
		}
		if err := c.submitTx(ctx, tx); err != nil {
			if isCellContentionError(err) && attempt < contentionRetries-1 {
				log.Printf("DisputeVC: cell contention (vc=%x attempt %d), re-reading: %v", vcID[:4], attempt+1, err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(contentionRetryDelay):
				}
				continue
			}
			return err
		}
		return nil
	}
}

// mergeVirtualChannelCells consolidates the two VC cells that arise when a
// virtual channel's two parents are disputed concurrently into one. The
// contract detects the merge structurally (2 VC inputs, 1 output) and reads
// both cells' block headers (load_header(0/1, GroupInput)) to keep the
// lower-block cell, so both block hashes are supplied as header deps.
func (c Client) mergeVirtualChannelCells(ctx context.Context, vcCells []*indexer.LiveCell, vcStatuses []*molecule.VirtualChannelStatus, vcDispute *molecule.VCDispute) error {
	if len(vcCells) < 2 {
		return fmt.Errorf("mergeVirtualChannelCells requires 2 VC cells, got %d", len(vcCells))
	}

	occupiedCapacity0 := vcCells[0].Output.OccupiedCapacity(vcCells[0].OutputData)
	occupiedCapacity1 := vcCells[1].Output.OccupiedCapacity(vcCells[1].OutputData)

	// Resolve both VC owners' real payment scripts from their on-chain owner
	// records so the dropped cell's capacity is returned to the correct
	// (possibly omni-lock) address.
	omniCodeHash := c.deployment.OmniLockScript.CodeHash
	restoredOwnerScript0, err := ckbaddress.RecoverOnChainPaymentScript(vcStatuses[0].Owner(), omniCodeHash)
	if err != nil {
		return fmt.Errorf("recovering virtual channel 0 owner script: %w", err)
	}
	restoredOwnerScript1, err := ckbaddress.RecoverOnChainPaymentScript(vcStatuses[1].Owner(), omniCodeHash)
	if err != nil {
		return fmt.Errorf("recovering virtual channel 1 owner script: %w", err)
	}

	blockHash0, err := c.blockHashOfCell(ctx, vcCells[0])
	if err != nil {
		return fmt.Errorf("getting vc cell 0 block hash: %w", err)
	}
	blockHash1, err := c.blockHashOfCell(ctx, vcCells[1])
	if err != nil {
		return fmt.Errorf("getting vc cell 1 block hash: %w", err)
	}

	mergeVCInfo := transaction.NewVCMergeInfo(
		vcCells[0].OutPoint,
		vcCells[1].OutPoint,
		vcStatuses[0],
		vcStatuses[1],
		occupiedCapacity0,
		occupiedCapacity1,
		vcCells[0].BlockNumber,
		vcCells[1].BlockNumber,
		[]types.Hash{*blockHash0, *blockHash1},
		vcCells[0].Output.Type,
		vcDispute,
		restoredOwnerScript0,
		restoredOwnerScript1,
	)
	builder, err := c.newPerunTransactionBuilder(nil)
	if err != nil {
		return fmt.Errorf("creating Perun transaction builder: %w", err)
	}
	if err := builder.MergeVC(mergeVCInfo); err != nil {
		return fmt.Errorf("creating vc merge transaction: %w", err)
	}
	tx, err := builder.Build(c.signer.Contexts())
	if err != nil {
		return fmt.Errorf("building vc merge transaction: %w", err)
	}
	return c.submitTx(ctx, tx)
}

// blockHashOfCell returns the hash of the block in which the given live cell's
// producing transaction was included.
func (c Client) blockHashOfCell(ctx context.Context, cell *indexer.LiveCell) (*types.Hash, error) {
	tx, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.TransactionWithStatus, error) {
		return c.client.GetTransaction(ctx, cell.OutPoint.TxHash)
	})
	if err != nil {
		return nil, err
	}
	if tx.TxStatus.BlockHash == nil {
		return nil, fmt.Errorf("transaction %s has no block hash", cell.OutPoint.TxHash)
	}
	return tx.TxStatus.BlockHash, nil
}

// blockTimestamp returns the timestamp (CKB block time, in milliseconds) of the
// block identified by blockHash.
func (c Client) blockTimestamp(ctx context.Context, blockHash types.Hash) (uint64, error) {
	header, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Header, error) {
		return c.client.GetHeader(ctx, blockHash)
	})
	if err != nil {
		return 0, err
	}
	return header.Timestamp, nil
}

// waitForTimeLockExpired polls the chain tip until the newest block timestamp
// is at or past the largest supplied deadline, then returns that tip header.
// The contract's verify_time_lock_expired compares a dispute block's timestamp
// + challenge against the newest header dep, which lags wall-clock; without this
// the attached tip can be behind the deadline and trip TimeLockNotExpired (69).
func (c Client) waitForTimeLockExpired(ctx context.Context, deadlines ...uint64) (*types.Header, error) {
	var deadline uint64
	for _, d := range deadlines {
		if d > deadline {
			deadline = d
		}
	}
	for {
		header, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Header, error) {
			return c.client.GetTipHeader(ctx)
		})
		if err != nil {
			return nil, fmt.Errorf("getting tip header: %w", err)
		}
		if header.Timestamp >= deadline {
			return header, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for time-lock (deadline %d ms, tip %d ms): %w", deadline, header.Timestamp, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (c Client) Coordinate(ctx context.Context, id channel.ID, canonicalState *channel.State, sigs []wallet.Sig, coordSig wallet.Sig, params *channel.Params) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	log.Println("Coordinate called")
	if len(sigs) != 2 {
		return fmt.Errorf("expected 2 participant signatures, got %d", len(sigs))
	}
	if len(coordSig) == 0 {
		return fmt.Errorf("coordinator signature is empty")
	}

	channelCell, status, err := c.getChannelLiveCellWithCache(ctx, id)
	if err != nil {
		return fmt.Errorf("getting channel live cell: %w", err)
	}
	onChainVersion := molecule2.UnpackUint64(status.State().Version())
	if canonicalState.Version < onChainVersion {
		return fmt.Errorf("canonical state version %d is below on-chain version %d", canonicalState.Version, onChainVersion)
	}

	// The contract's verify_time_lock_expired loads the channel input cell's
	// block header via load_header(0, GroupInput), so that block hash must be a
	// header dep alongside the tip header (mirrors ForceClose).
	oldTx, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.TransactionWithStatus, error) {
		return c.client.GetTransaction(ctx, channelCell.OutPoint.TxHash)
	})
	if err != nil {
		return fmt.Errorf("getting channel cell transaction: %w", err)
	}
	blockHash := oldTx.TxStatus.BlockHash

	// Wait until the chain tip is past the dispute deadline before attaching it,
	// so verify_time_lock_expired sees current_time >= old_timestamp + challenge.
	disputeTime, err := c.blockTimestamp(ctx, *blockHash)
	if err != nil {
		return fmt.Errorf("getting dispute block timestamp: %w", err)
	}
	header, err := c.waitForTimeLockExpired(ctx, disputeTime+params.ChallengeDuration)
	if err != nil {
		return fmt.Errorf("waiting for channel time-lock to expire: %w", err)
	}

	ci := transaction.NewCoordinateInfo(
		*channelCell.OutPoint,
		*status,
		canonicalState,
		params,
		[]types.Hash{*blockHash, header.Hash},
		channelCell.Output.Type,
		sigs[0], sigs[1], coordSig,
		channelCell.Output.Capacity,
	)

	return c.buildAndSubmitWithRetry(ctx, "Coordinate", func() (*ckbtransaction.TransactionWithScriptGroups, error) {
		builder, err := c.newPerunTransactionBuilder(nil)
		if err != nil {
			return nil, fmt.Errorf("creating Perun transaction builder: %w", err)
		}
		if err := builder.Coordinate(ci); err != nil {
			return nil, fmt.Errorf("creating coordinate transaction: %w", err)
		}
		tx, err := builder.Build(c.signer.Contexts())
		if err != nil {
			return nil, fmt.Errorf("building coordinate transaction: %w", err)
		}
		return tx, nil
	})
}

func (c Client) CoordinateVC(ctx context.Context, parentID, vcID channel.ID, parentState, vcState *channel.State, parentSigs, vcSigs []wallet.Sig, parentCoordSig, vcCoordSig wallet.Sig, parentParams, vcParams *channel.Params) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	log.Println("CoordinateVC called")
	if len(parentSigs) != 2 || len(vcSigs) != 2 {
		return fmt.Errorf("expected 2 participant signatures per channel, got parent=%d vc=%d", len(parentSigs), len(vcSigs))
	}
	if len(parentCoordSig) == 0 || len(vcCoordSig) == 0 {
		return fmt.Errorf("coordinator signatures are empty (parent=%d vc=%d)", len(parentCoordSig), len(vcCoordSig))
	}

	parentCell, parentStatus, err := c.getChannelLiveCellWithCache(ctx, parentID)
	if err != nil {
		return fmt.Errorf("getting parent channel live cell: %w", err)
	}
	if onV := molecule2.UnpackUint64(parentStatus.State().Version()); parentState.Version < onV {
		return fmt.Errorf("canonical parent state version %d below on-chain %d", parentState.Version, onV)
	}

	vcCells, vcStatuses, err := c.getAllVirtualChannelLiveCellsForID(ctx, vcID)
	if err != nil {
		return fmt.Errorf("getting virtual channel live cell: %w", err)
	}
	if len(vcCells) > 1 {
		// The VC's two parents were disputed concurrently, leaving two cells
		// (same vcts, different blocks). Merge them into one BEFORE coordinating
		// either: once a cell is coordinated, the two statuses diverge and the
		// contract's verify_equal_vc_status would reject the merge. The merge
		// ignores the witness sigs but needs a structurally valid VCDispute.
		vcSigA, err := encodeOptionalSignature(vcSigs[0])
		if err != nil {
			return fmt.Errorf("encoding vc signature A for merge: %w", err)
		}
		vcSigB, err := encodeOptionalSignature(vcSigs[1])
		if err != nil {
			return fmt.Errorf("encoding vc signature B for merge: %w", err)
		}
		parentSigAEnc, err := encodeOptionalSignature(parentSigs[0])
		if err != nil {
			return fmt.Errorf("encoding parent signature A for merge: %w", err)
		}
		parentSigBEnc, err := encodeOptionalSignature(parentSigs[1])
		if err != nil {
			return fmt.Errorf("encoding parent signature B for merge: %w", err)
		}
		vcDispute := encoding.PackVCDispute(vcSigA, vcSigB, parentSigAEnc, parentSigBEnc)
		if err := c.mergeVirtualChannelCells(ctx, vcCells, vcStatuses, &vcDispute); err != nil {
			return fmt.Errorf("merging split virtual channel before coordinate: %w", err)
		}
		vcCells, vcStatuses, err = c.getAllVirtualChannelLiveCellsForID(ctx, vcID)
		if err != nil {
			return fmt.Errorf("getting virtual channel live cell after merge: %w", err)
		}
	}
	if len(vcCells) != 1 {
		return fmt.Errorf("CoordinateVC requires exactly one VC cell, got %d", len(vcCells))
	}
	vcCell := vcCells[0]
	vcStatus := vcStatuses[0]
	if onV := molecule2.UnpackUint64(vcStatus.Vcstate().Version()); vcState.Version < onV {
		return fmt.Errorf("canonical vc state version %d below on-chain %d", vcState.Version, onV)
	}

	// The contract's verify_time_lock_expired loads each group input's block
	// header via load_header(0, GroupInput): PCTS reads the parent channel cell,
	// VCTS reads the VC cell. Both block hashes must be header deps alongside the
	// tip header (find_closest_current_time). Mirrors Coordinate.
	parentTx, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.TransactionWithStatus, error) {
		return c.client.GetTransaction(ctx, parentCell.OutPoint.TxHash)
	})
	if err != nil {
		return fmt.Errorf("getting parent channel cell transaction: %w", err)
	}
	vcTx, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.TransactionWithStatus, error) {
		return c.client.GetTransaction(ctx, vcCell.OutPoint.TxHash)
	})
	if err != nil {
		return fmt.Errorf("getting vc cell transaction: %w", err)
	}

	// Wait past every enforced deadline: the parent's window always, plus the
	// VC's own window only on the first coordinate (the VCTS skips it once
	// old_vc_status.coordinated() is true, i.e. the second parent's coordinate).
	parentDisputeTime, err := c.blockTimestamp(ctx, *parentTx.TxStatus.BlockHash)
	if err != nil {
		return fmt.Errorf("getting parent dispute block timestamp: %w", err)
	}
	deadlines := []uint64{parentDisputeTime + parentParams.ChallengeDuration}
	if !encoding.ToBool(*vcStatus.Coordinated()) {
		vcDisputeTime, err := c.blockTimestamp(ctx, *vcTx.TxStatus.BlockHash)
		if err != nil {
			return fmt.Errorf("getting vc dispute block timestamp: %w", err)
		}
		deadlines = append(deadlines, vcDisputeTime+vcParams.ChallengeDuration)
	}
	header, err := c.waitForTimeLockExpired(ctx, deadlines...)
	if err != nil {
		return fmt.Errorf("waiting for vc/parent time-locks to expire: %w", err)
	}

	ci := &transaction.VCCoordinateInfo{
		ChannelCell:          *parentCell.OutPoint,
		LCStatus:             *parentStatus,
		LCState:              parentState,
		LCSigA:               parentSigs[0],
		LCSigB:               parentSigs[1],
		LCCoordSig:           parentCoordSig,
		VCCell:               *vcCell.OutPoint,
		VCStatus:             *vcStatus,
		VCState:              vcState,
		VCSigA:               vcSigs[0],
		VCSigB:               vcSigs[1],
		VCCoordSig:           vcCoordSig,
		Headers:              []types.Hash{*parentTx.TxStatus.BlockHash, *vcTx.TxStatus.BlockHash, header.Hash},
		PCTS:                 parentCell.Output.Type,
		VCTS:                 vcCell.Output.Type,
		InputChannelCapacity: parentCell.Output.Capacity,
		InputVCCapacity:      vcCell.Output.Capacity,
	}

	return c.buildAndSubmitWithRetry(ctx, "CoordinateVC", func() (*ckbtransaction.TransactionWithScriptGroups, error) {
		builder, err := c.newPerunTransactionBuilder(nil)
		if err != nil {
			return nil, fmt.Errorf("creating Perun transaction builder: %w", err)
		}
		if err := builder.CoordinateVC(ci); err != nil {
			return nil, fmt.Errorf("creating vc coordinate transaction: %w", err)
		}
		tx, err := builder.Build(c.signer.Contexts())
		if err != nil {
			return nil, fmt.Errorf("building vc coordinate transaction: %w", err)
		}
		return tx, nil
	})
}

func (c Client) Close(ctx context.Context, id channel.ID, state *channel.State, sigs []wallet.Sig, params *channel.Params) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	log.Println("Close called")
	channelCell, _, err := c.getChannelLiveCellWithCache(ctx, id)
	if err != nil {
		return fmt.Errorf("getting channel live cell: %w", err)
	}
	pcts := channelCell.Output.Type
	assets, err := c.getAssets(ctx, pcts)
	if err != nil {
		return fmt.Errorf("retrieving assets locked in channel: %w", err)
	}
	header, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Header, error) {
		return c.client.GetTipHeader(ctx)
	})
	if err != nil {
		return fmt.Errorf("getting tip header: %w", err)
	}
	channelCapacity := channelCell.Output.Capacity

	ci := transaction.NewCloseInfo(
		channelCapacity,
		types.CellInput{PreviousOutput: channelCell.OutPoint},
		mkCellInputs(assets),
		[]types.Hash{header.Hash},
		params,
		state,
		sigs,
	)

	builder, err := c.newPerunTransactionBuilder(nil)
	if err != nil {
		return fmt.Errorf("creating Perun transaction builder: %w", err)
	}
	if err := builder.Close(ci); err != nil {
		return fmt.Errorf("creating close transaction: %w", err)
	}
	tx, err := builder.Build(c.signer.Contexts())
	if err != nil {
		return fmt.Errorf("building close transaction: %w", err)
	}
	return c.submitTx(ctx, tx)
}

// Turns a list of live cells into a list of input cells.
func mkCellInputs(lcs *indexer.LiveCells) []types.CellInput {
	res := make([]types.CellInput, len(lcs.Objects))
	for idx, lc := range lcs.Objects {
		res[idx] = types.CellInput{
			Since:          0,
			PreviousOutput: lc.OutPoint,
		}
	}
	return res
}

// getAssets retrieves a list of all assets that are locked in the channel
// identified by the given PCTS.
func (c Client) getAssets(ctx context.Context, pcts *types.Script) (*indexer.LiveCells, error) {
	pctsScriptHash := pcts.Hash()
	pflsPrefix := &types.Script{
		CodeHash: c.deployment.PFLSCodeHash,
		HashType: c.deployment.PFLSHashType,
		Args:     pctsScriptHash[:],
	}
	searchKey := &indexer.SearchKey{
		Script:           pflsPrefix,
		ScriptType:       types.ScriptTypeLock,
		ScriptSearchMode: types.ScriptSearchModePrefix,
		Filter:           nil,
		WithData:         true,
	}
	cells, err := retryRPC(ctx, 3, 10*time.Second, func() (*indexer.LiveCells, error) {
		return c.client.GetCells(ctx, searchKey, indexer.SearchOrderDesc, SearchIndexerLimit, "")
	})
	return cells, err
}

func (c Client) ForceClose(ctx context.Context, id channel.ID, state *channel.State, params *channel.Params) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	log.Println("ForceClose called")
	// Retry on cell contention: both parties Settle the same channel, so re-read
	// each attempt — once the competitor's force close concludes it, the lookup
	// returns ErrNoChannelLiveCell, handled below as an idempotent success.
	for attempt := 0; ; attempt++ {
		channelCell, _, err := c.getChannelLiveCellWithCache(ctx, id)
		if errors.Is(err, ErrNoChannelLiveCell) {
			// Normal (non-VC) force close pays BOTH parties and consumes the channel
			// cell in a single transaction (see the contract's check_normal_force_close
			// -> verify_all_paid). When both parties Settle(secondary=false) — as the
			// multi-ledger harness does — the second force close legitimately finds no
			// channel cell: the channel is already concluded and both payouts landed.
			// Treat that as success (matching the eth-backend, whose Withdraw skips an
			// already-concluded channel). NOTE: this is the LEDGER-channel path; the
			// genuine first/second force-close distinction only applies to virtual
			// channels and is handled separately by ForceCloseWithVC.
			log.Printf("ForceClose: channel %x already concluded (both parties paid by the first force close)", id[:4])
			return nil
		}
		if err != nil {
			return fmt.Errorf("getting channel live cell: %w", err)
		}
		pcts := channelCell.Output.Type
		assets, err := c.getAssets(ctx, pcts)
		if err != nil {
			return fmt.Errorf("retrieving assets locked in channel: %w", err)
		}
		header, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Header, error) {
			return c.client.GetTipHeader(ctx)
		})
		if err != nil {
			return fmt.Errorf("getting tip header: %w", err)
		}

		oldTx, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.TransactionWithStatus, error) {
			return c.client.GetTransaction(ctx, channelCell.OutPoint.TxHash)
		})
		if err != nil {
			return fmt.Errorf("getting old transaction: %w", err)
		}
		blockHash := oldTx.TxStatus.BlockHash

		channelCapacity := channelCell.Output.Capacity
		fci := transaction.NewForceCloseInfo(
			types.CellInput{PreviousOutput: channelCell.OutPoint},
			mkCellInputs(assets),
			[]types.Hash{*blockHash, header.Hash},
			state,
			params,
			channelCapacity,
		)

		builder, err := c.newPerunTransactionBuilder(nil)
		if err != nil {
			return fmt.Errorf("creating Perun transaction builder: %w", err)
		}
		if err := builder.ForceClose(fci); err != nil {
			return fmt.Errorf("creating force close transaction: %w", err)
		}
		tx, err := builder.Build(c.signer.Contexts())
		if err != nil {
			return fmt.Errorf("building force close transaction: %w", err)
		}
		if err := c.submitTx(ctx, tx); err != nil {
			if isCellContentionError(err) && attempt < contentionRetries-1 {
				log.Printf("ForceClose: cell contention (id=%x attempt %d), re-reading: %v", id[:4], attempt+1, err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(contentionRetryDelay):
				}
				continue
			}
			return err
		}
		return nil
	}
}

// encodeOptionalSignature converts a participant signature to the molecule
// representation, returning an empty Bytes for a nil/empty signature. Used on
// witness positions the contract does not verify (e.g. the VC force-close
// path), where a coordinated transaction's reset sigs would otherwise abort the
// call before tx construction.
func encodeOptionalSignature(sig wallet.Sig) (*molecule.Bytes, error) {
	if len(sig) == 0 {
		empty := molecule.BytesDefault()
		return &empty, nil
	}
	return encoding.NewMoleculeSignature(sig)
}

func (c Client) ForceCloseWithVC(ctx context.Context, id channel.ID, vcid channel.ID, state *channel.State, vcstate *channel.State, sigs []wallet.Sig, params *channel.Params, indexMap []channel.Index) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	// Clone the caller's states: updateState (below, on the "old versions"
	// branch) reassigns balance rows in place, which would otherwise mutate the
	// channel-machine's state objects shared by reference with the test harness's
	// balance config and corrupt later assertions. A client must not mutate
	// caller-owned state.
	state = state.Clone()
	vcstate = vcstate.Clone()

	virtualChannelCells, vcStatuses, err := c.getVirtualChannelLiveCellWithCache(ctx, vcid)
	if err != nil {
		return fmt.Errorf("getting virtual channel live cell: %w", err)
	}

	if len(virtualChannelCells) > 1 {
		return fmt.Errorf("expected only one virtual channel, got %d", len(virtualChannelCells))
	}

	virtualChannelCell := virtualChannelCells[0]
	vcStatus := vcStatuses[0]
	occupiedVirtualChannelCapacity := virtualChannelCell.Output.Capacity

	firstForceClose := encoding.ToBool(*vcStatus.FirstForceClose())

	channelCell, status, err := c.getChannelLiveCellWithCache(ctx, id)
	if err != nil {
		return fmt.Errorf("getting channel live cell: %w", err)
	}

	// Party 0 reclaims the channel cell's ACTUAL capacity (which carries the conserved
	// sub-alloc reserve), matching the contract's load_cell_capacity check at force close.
	channelCapacity := channelCell.Output.Capacity

	pcts := channelCell.Output.Type
	vcts := virtualChannelCell.Output.Type
	assets, err := c.getAssets(ctx, pcts)
	if err != nil {
		return fmt.Errorf("retrieving assets locked in channel: %w", err)
	}

	// The participant signatures are vestigial on the VC force-close path: the
	// contract's check_vc_force_close performs no signature verification (the LC
	// is settled via its coordinated flag or its own time lock, and the witness
	// is the empty ForceClose default — see buildFirstForceCloseWithVCTransaction).
	// They are still required and verified on the dispute path that produced them.
	// After a Coordinate, however, the machine's coordinated transaction carries
	// nil sigs (machine.forceState resets them), so tolerate empty sigs here.
	sigA, err := encodeOptionalSignature(sigs[0])
	if err != nil {
		return fmt.Errorf("encoding signature A: %w", err)
	}

	sigB, err := encodeOptionalSignature(sigs[1])
	if err != nil {
		return fmt.Errorf("encoding signature B: %w", err)
	}

	oldTx, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.TransactionWithStatus, error) {
		return c.client.GetTransaction(ctx, channelCell.OutPoint.TxHash)
	})
	if err != nil {
		return fmt.Errorf("getting old transaction: %w", err)
	}
	blockHash := oldTx.TxStatus.BlockHash

	// Wait past both dispute deadlines (the parent and VC cells may sit in
	// different blocks). check_vc_force_close checks the parent's window with
	// params.challenge_duration and the VC's window with the VC's OWN
	// vcts_args.params().challenge_duration, which may differ.
	parentDisputeTime, err := c.blockTimestamp(ctx, *blockHash)
	if err != nil {
		return fmt.Errorf("getting parent dispute block timestamp: %w", err)
	}
	vcBlockHash, err := c.blockHashOfCell(ctx, virtualChannelCell)
	if err != nil {
		return fmt.Errorf("getting vc cell block hash: %w", err)
	}
	vcDisputeTime, err := c.blockTimestamp(ctx, *vcBlockHash)
	if err != nil {
		return fmt.Errorf("getting vc dispute block timestamp: %w", err)
	}
	vcConstants, err := molecule.VCChannelConstantsFromSlice(vcts.Args, false)
	if err != nil {
		return fmt.Errorf("decoding vc channel constants: %w", err)
	}
	vcChallenge := molecule2.UnpackUint64(vcConstants.Params().ChallengeDuration())
	header, err := c.waitForTimeLockExpired(ctx, parentDisputeTime+params.ChallengeDuration, vcDisputeTime+vcChallenge)
	if err != nil {
		return fmt.Errorf("waiting for force-close time-locks to expire: %w", err)
	}

	// Check version.
	if !checkVersion(state, status, vcstate, vcStatus) {
		log.Println("ForceCloseWithVC: old versions detected")
		state, err = updateState(state, status.State())
		if err != nil {
			return fmt.Errorf("updating state: %w", err)
		}
		vcstate, err = updateState(vcstate, vcStatus.Vcstate())
		if err != nil {
			return fmt.Errorf("updating vcstate: %w", err)
		}
	}

	// RestoredOwnerScript is only consumed by the second force close, which runs when
	// firstForceClose is true (see transaction.go ForceCloseWithVC). Resolve it lazily so a
	// recovery failure cannot abort the first force close, which does not use it.
	var restoredOwnerScript *types.Script
	if firstForceClose {
		restoredOwnerScript, err = ckbaddress.RecoverOnChainPaymentScript(vcStatus.Owner(), c.deployment.OmniLockScript.CodeHash)
		if err != nil {
			return fmt.Errorf("recovering virtual channel owner script: %w", err)
		}
	}

	fcvi := transaction.NewForceCloseWithVCInfo(
		channelCell.OutPoint,
		virtualChannelCell.OutPoint,
		vcts,
		state,
		vcstate,
		vcStatus,
		*sigA, *sigB,
		params,
		// Header deps: tip, the parent cell's block (PCTS load_header), and the VC
		// cell's block (VCTS load_header). All three are required since the parent
		// and VC cells can be in different blocks.
		[]types.Hash{header.Hash, *blockHash, *vcBlockHash},
		mkCellInputs(assets),
		channelCapacity,
		occupiedVirtualChannelCapacity,
		firstForceClose,
		indexMap,
		restoredOwnerScript,
	)

	builder, err := c.newPerunTransactionBuilder(nil)
	if err != nil {
		return fmt.Errorf("creating Perun transaction builder: %w", err)
	}

	if err := builder.ForceCloseWithVC(fcvi); err != nil {
		return fmt.Errorf("creating close transaction: %w", err)
	}
	tx, err := builder.Build(c.signer.Contexts())
	if err != nil {
		return fmt.Errorf("building close transaction: %w", err)
	}
	return c.submitTx(ctx, tx)
}

func (c Client) Abort(ctx context.Context, script *types.Script, params *channel.Params, state *channel.State) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	channelCell, err := c.getExactChannelLiveCell(ctx, script)
	if err != nil {
		return fmt.Errorf("getting channel live cell: %w", err)
	}
	pcts := channelCell.Output.Type
	assets, err := c.getAssets(ctx, pcts)
	if err != nil {
		return fmt.Errorf("retrieving assets locked in channel: %w", err)
	}
	header, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Header, error) {
		return c.client.GetTipHeader(ctx)
	})

	if err != nil {
		return fmt.Errorf("getting tip header: %w", err)
	}
	channelCapacity := channelCell.Output.Capacity
	ai := transaction.NewAbortInfo(
		types.CellInput{PreviousOutput: channelCell.OutPoint},
		mkCellInputs(assets),
		state,
		params,
		[]types.Hash{header.Hash},
		channelCapacity,
	)

	builder, err := c.newPerunTransactionBuilder(nil)
	if err != nil {
		return fmt.Errorf("creating Perun transaction builder: %w", err)
	}
	if err := builder.Abort(ai); err != nil {
		return fmt.Errorf("creating abort transaction: %w", err)
	}
	tx, err := builder.Build(c.signer.Contexts())
	if err != nil {
		return fmt.Errorf("building abort transaction: %w", err)
	}
	return c.submitTx(ctx, tx)
}

func (c Client) GetChannelWithExactPCTS(ctx context.Context, pcts *types.Script) (BlockNumber, *molecule.ChannelStatus, error) {
	cell, err := c.getExactChannelLiveCell(ctx, pcts)
	if err != nil {
		log.Printf("GetChannelWithExactPCTS: getExactChannelLiveCell err: %v", err)
		return 0, nil, fmt.Errorf("getting exact channel live cell: %w", err)
	}
	channelStatus, err := molecule.ChannelStatusFromSlice(cell.OutputData, false)
	if err != nil {
		log.Printf("GetChannelWithExactPCTS: ChannelStatusFromSlice err: %v (data len=%d)", err, len(cell.OutputData))
		return 0, nil, err
	}
	return cell.BlockNumber, channelStatus, nil
}

const defaultPollingInterval = 2 * time.Second

// sendAndAwait sends the given transaction and waits for it to be committed
// on-chain.
func (c Client) sendAndAwait(ctx context.Context, tx *types.Transaction) error {
	txHash, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Hash, error) {
		return c.client.SendTransaction(ctx, tx)
	})
	if err != nil {
		return fmt.Errorf("sending transaction: %w", err)
	}

	// Wait for the transaction to be committed on-chain.
	txWithStatus, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.TransactionWithStatus, error) {
		return c.client.GetTransaction(ctx, *txHash)
	})
	if err != nil {
		return fmt.Errorf("initially polling transaction: %w", err)
	}

	ticker := time.NewTicker(defaultPollingInterval)
	for txWithStatus.TxStatus.Status != types.TransactionStatusCommitted {
		if txWithStatus.TxStatus.Status == types.TransactionStatusRejected {
			return fmt.Errorf("transaction rejected with: %v", *txWithStatus.TxStatus.Reason)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		case <-ticker.C:
			txWithStatus, err = retryRPC(ctx, 3, 10*time.Second, func() (*types.TransactionWithStatus, error) {
				return c.client.GetTransaction(ctx, *txHash)
			})
			if err != nil {
				return fmt.Errorf("polling transaction: %w", err)
			}
		}
	}
	ticker.Stop()

	// The tx is committed on the node, but the ckb-indexer (which powers every
	// live-cell lookup and the fee-cell collector) trails it. Block until the
	// indexer reaches the committing block, else a follow-up tx could pick a cell
	// the indexer still lists as live but the node already spent.
	if txWithStatus.TxStatus.BlockHash != nil {
		committedHeader, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Header, error) {
			return c.client.GetHeader(ctx, *txWithStatus.TxStatus.BlockHash)
		})
		if err != nil {
			return fmt.Errorf("getting committing block header: %w", err)
		}
		if err := c.waitForIndexer(ctx, committedHeader.Number); err != nil {
			return fmt.Errorf("waiting for indexer to sync: %w", err)
		}
	}

	return nil
}

// waitForIndexer blocks until the ckb-indexer has processed at least up to the
// given block number. The indexer trails the node, so live-cell queries issued
// immediately after a tx commits can otherwise return cells that are already
// spent (or miss freshly created ones).
func (c Client) waitForIndexer(ctx context.Context, blockNumber uint64) error {
	for {
		tip, err := retryRPC(ctx, 3, 10*time.Second, func() (*indexer.TipHeader, error) {
			return c.client.GetIndexerTip(ctx)
		})
		if err != nil {
			return fmt.Errorf("getting indexer tip: %w", err)
		}
		if tip != nil && tip.BlockNumber >= blockNumber {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (c Client) GetChannelWithID(ctx context.Context, id channel.ID) (BlockNumber, *types.Script, *molecule.ChannelConstants, *molecule.ChannelStatus, error) {
	cell, status, err := c.getChannelLiveCellWithCache(ctx, id)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	channelConstants, err := molecule.ChannelConstantsFromSlice(cell.Output.Type.Args, false)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	return cell.BlockNumber, cell.Output.Type, channelConstants, status, nil
}

func (c Client) getFirstChannelLiveCellWithID(channels *indexer.LiveCells, id channel.ID) (*indexer.LiveCell, *molecule.ChannelStatus, error) {
	for _, cell := range channels.Objects {
		if !c.isValidChannelLiveCell(cell) {
			continue
		}
		channelStatus, err := molecule.ChannelStatusFromSlice(cell.OutputData, false)
		if err != nil {
			continue
		}
		if types.UnpackHash(channelStatus.State().ChannelId()) != id {
			continue
		}
		return cell, channelStatus, nil
	}
	return nil, nil, ErrNoChannelLiveCell
}

func (c Client) getFirstVirtualChannelLiveCellWithID(channels *indexer.LiveCells, id channel.ID) (*indexer.LiveCell, *molecule.VirtualChannelStatus, error) {
	for _, cell := range channels.Objects {
		if !c.isValidVirtualChannelLiveCell(cell) {
			continue
		}
		virtualChannelStatus, err := molecule.VirtualChannelStatusFromSlice(cell.OutputData, false)
		if err != nil {
			continue
		}
		if types.UnpackHash(virtualChannelStatus.Vcstate().ChannelId()) != id {
			continue
		}
		return cell, virtualChannelStatus, nil
	}
	return nil, nil, ErrNoChannelLiveCell
}

func (c Client) getAllChannelLiveCells(ctx context.Context) (*indexer.LiveCells, error) {
	pctsPrefix := &types.Script{
		CodeHash: c.deployment.PCTSCodeHash,
		HashType: c.deployment.PCTSHashType,
		Args:     []byte{},
	}
	searchKey := &indexer.SearchKey{
		Script:           pctsPrefix,
		ScriptType:       types.ScriptTypeType,
		ScriptSearchMode: types.ScriptSearchModePrefix,
		Filter:           nil,
		WithData:         true,
	}
	cells, err := retryRPC(ctx, 3, 10*time.Second, func() (*indexer.LiveCells, error) {
		return c.client.GetCells(ctx, searchKey, indexer.SearchOrderDesc, SearchIndexerLimit, "")
	})
	return cells, err
}

func (c Client) getAllVirtualChannelLiveCells(ctx context.Context) (*indexer.LiveCells, error) {
	vctsPrefix := &types.Script{
		CodeHash: c.deployment.VCTSCodeHash,
		HashType: c.deployment.VCTSHashType,
		Args:     []byte{},
	}
	searchKey := &indexer.SearchKey{
		Script:           vctsPrefix,
		ScriptType:       types.ScriptTypeType,
		ScriptSearchMode: types.ScriptSearchModePrefix,
		Filter:           nil,
		WithData:         true,
	}
	return c.client.GetCells(ctx, searchKey, indexer.SearchOrderDesc, math.MaxUint32, "")
}

func (c Client) getExactChannelLiveCell(ctx context.Context, pcts *types.Script) (*indexer.LiveCell, error) {
	searchKey := &indexer.SearchKey{
		Script:           pcts,
		ScriptType:       types.ScriptTypeType,
		ScriptSearchMode: types.ScriptSearchModeExact,
		Filter:           nil,
		WithData:         true,
	}
	cells, err := retryRPC(ctx, 3, 10*time.Second, func() (*indexer.LiveCells, error) {
		return c.client.GetCells(ctx, searchKey, indexer.SearchOrderDesc, SearchIndexerLimit, "")
	})
	if err != nil {
		log.Println("getExactChannelLiveCell: GetCells error: ", err)
		return nil, err
	}
	if len(cells.Objects) > 1 {
		return nil, errors.New("more than one live cell found for channel")
	}
	if len(cells.Objects) == 0 {
		return nil, ErrNoChannelLiveCell
	}
	return cells.Objects[0], nil
}

// getAllVirtualChannelLiveCellsForID returns every live VC cell sharing the
// virtual channel's vcts — up to two, when the channel's two parents disputed
// concurrently. Unlike getVirtualChannelLiveCellWithCache, it does not truncate
// to the first match on a cache miss, so CoordinateVC can detect (and merge) a
// split VC before coordinating either cell.
func (c Client) getAllVirtualChannelLiveCellsForID(ctx context.Context, id channel.ID) ([]*indexer.LiveCell, []*molecule.VirtualChannelStatus, error) {
	script, cached := c.vccache.Get(id)
	if !cached {
		liveCells, err := c.getAllVirtualChannelLiveCells(ctx)
		if err != nil {
			return nil, nil, err
		}
		cell, _, err := c.getFirstVirtualChannelLiveCellWithID(liveCells, id)
		if err != nil {
			return nil, nil, err
		}
		script = cell.Output.Type
		// Best-effort cache; the resolved script is used regardless.
		_ = c.vccache.Set(id, script)
	}
	cells, err := c.getExactVirtualChannelLiveCell(ctx, script)
	if err != nil {
		return nil, nil, err
	}
	statuses := make([]*molecule.VirtualChannelStatus, len(cells))
	for i, cell := range cells {
		status, err := molecule.VirtualChannelStatusFromSlice(cell.OutputData, false)
		if err != nil {
			return nil, nil, fmt.Errorf("converting cell outputdata to VirtualChannelStatus: %w", err)
		}
		statuses[i] = status
	}
	return cells, statuses, nil
}

func (c Client) getExactVirtualChannelLiveCell(ctx context.Context, vcts *types.Script) ([]*indexer.LiveCell, error) {
	searchKey := &indexer.SearchKey{
		Script:           vcts,
		ScriptType:       types.ScriptTypeType,
		ScriptSearchMode: types.ScriptSearchModeExact,
		Filter:           nil,
		WithData:         true,
	}
	cells, err := retryRPC(ctx, 3, 10*time.Second, func() (*indexer.LiveCells, error) {
		return c.client.GetCells(ctx, searchKey, indexer.SearchOrderDesc, SearchIndexerLimit, "")
	})
	log.Println("getExactVirtualChannelLiveCell: GetCells")
	if err != nil {
		log.Println("getExactVirtualChannelLiveCell: GetCells error: ", err)
		return nil, err
	}
	if len(cells.Objects) > 2 {
		return nil, errors.New("more than two live cell found for the virtual channel")
	}
	if len(cells.Objects) == 0 {
		return nil, ErrNoChannelLiveCell
	}
	return cells.Objects, nil
}

func (c Client) isValidChannelLiveCell(cell *indexer.LiveCell) bool {
	if cell.Output == nil ||
		cell.Output.Type == nil ||
		cell.Output.Type.CodeHash != c.deployment.PCTSCodeHash ||
		cell.Output.Type.HashType != c.deployment.PCTSHashType {
		return false
	}
	return true
}

func (c Client) isValidVirtualChannelLiveCell(cell *indexer.LiveCell) bool {
	if cell.Output == nil ||
		cell.Output.Type == nil ||
		cell.Output.Type.CodeHash != c.deployment.VCTSCodeHash ||
		cell.Output.Type.HashType != c.deployment.VCTSHashType {
		return false
	}
	return true
}

func (c Client) GetBlockTime(ctx context.Context, blockNumber BlockNumber) (time.Time, error) {
	block, err := retryRPC(ctx, 3, 10*time.Second, func() (*types.Block, error) {
		return c.client.GetBlockByNumber(ctx, blockNumber)
	})
	if err != nil {
		return time.Time{}, err
	}
	if block.Header.Timestamp > math.MaxInt64 {
		return time.Time{}, errors.New("block timestamp is too large")
	}
	return time.UnixMilli(int64(block.Header.Timestamp)), nil
}

func (c Client) getChannelLiveCellWithCache(ctx context.Context, id channel.ID) (*indexer.LiveCell, *molecule.ChannelStatus, error) {
	script, cached := c.cache.Get(id)
	log.Println("getChannelLiveCellWithCache: cached?", cached)
	if cached {
		cell, err := c.getExactChannelLiveCell(ctx, script)
		if err != nil {
			return nil, nil, err
		}
		status, err := molecule.ChannelStatusFromSlice(cell.OutputData, false)
		if err != nil {
			return nil, nil, fmt.Errorf("converting cell outputdata to ChannelStatus: %w", err)
		}
		return cell, status, nil
	}
	liveChannelCells, err := c.getAllChannelLiveCells(ctx)
	log.Println("getChannelLiveCellWithCache: getAllChannelLiveCells returned error: ", err)
	if err != nil {
		return nil, nil, err
	}
	cell, status, err := c.getFirstChannelLiveCellWithID(liveChannelCells, id)
	if err != nil {
		return nil, nil, err
	}
	errCache := c.cache.Set(id, cell.Output.Type)
	if errCache != nil {
		return c.getChannelLiveCellWithCache(ctx, id)
	}
	return cell, status, err
}

func (c Client) getVirtualChannelLiveCellWithCache(ctx context.Context, id channel.ID) ([]*indexer.LiveCell, []*molecule.VirtualChannelStatus, error) {
	script, cached := c.vccache.Get(id)
	log.Println("getVirtualChannelLiveCellWithCache: cached?", cached)
	if cached {
		cells, err := c.getExactVirtualChannelLiveCell(ctx, script)
		if err != nil {
			return nil, nil, err
		}
		statuses := make([]*molecule.VirtualChannelStatus, len(cells))
		for i, cell := range cells {
			status, err := molecule.VirtualChannelStatusFromSlice(cell.OutputData, false)
			if err != nil {
				return nil, nil, fmt.Errorf("converting cell outputdata to VirtualChannelStatus: %w", err)
			}
			statuses[i] = status
		}

		return cells, statuses, nil
	}
	liveChannelCells, err := c.getAllVirtualChannelLiveCells(ctx)
	log.Println("getVirtualChannelLiveCellWithCache: getAllVirtualChannelLiveCells returned error: ", err)
	if err != nil {
		return nil, nil, err
	}
	cell, status, err := c.getFirstVirtualChannelLiveCellWithID(liveChannelCells, id)
	if err != nil {
		return nil, nil, err
	}
	errCache := c.vccache.Set(id, cell.Output.Type)
	if errCache != nil {
		return c.getVirtualChannelLiveCellWithCache(ctx, id)
	}
	return []*indexer.LiveCell{cell}, []*molecule.VirtualChannelStatus{status}, err
}

func checkVersion(parentState *channel.State, parentStatus *molecule.ChannelStatus, vcState *channel.State, vcStatus *molecule.VirtualChannelStatus) bool {
	oldParentVersion := molecule2.UnpackUint64(parentStatus.State().Version())
	newParentVersion := parentState.Version

	if newParentVersion < oldParentVersion {
		return false
	}

	if vcState == nil && vcStatus == nil {
		if newParentVersion == oldParentVersion {
			return false // No VC state, no VC status, and parent state is not updated.
		}
		// No VC state, no VC status, but parent state is updated.
		return true
	}

	oldVcVersion := molecule2.UnpackUint64(vcStatus.Vcstate().Version())
	newVcVersion := vcState.Version

	if newVcVersion < oldVcVersion {
		return false
	}
	if newParentVersion == oldParentVersion && newVcVersion == oldVcVersion {
		return false
	}
	return true
}

// updateState updates the state with the newest balances to be used for final settlement.
func updateState(state *channel.State, newState *molecule.ChannelState) (*channel.State, error) {
	if state.Version < molecule2.UnpackUint64(newState.Version()) {
		state.Version = molecule2.UnpackUint64(newState.Version())
	}
	state.IsFinal = encoding.ToBool(*newState.IsFinal())
	assetIdx, ok := state.AssetIndex(asset.NewCKBytesAsset())
	if !ok {
		return state, errors.New("asset not found")
	}
	state.Balances[assetIdx] = []channel.Bal{
		0: big.NewInt(int64(molecule2.UnpackUint64(newState.Balances().Assets().Get(uint(assetIdx)).ToUnion().IntoCKByteDistribution().Nth0()))),
		1: big.NewInt(int64(molecule2.UnpackUint64(newState.Balances().Assets().Get(uint(assetIdx)).ToUnion().IntoCKByteDistribution().Nth1()))),
	}

	for sudtIndex, pAsset := range state.Assets {
		a, ckb := asset.IsCompatibleAsset(pAsset)
		if !ckb {
			continue
		}
		if a.IsInvalid() {
			return nil, errors.New("invalid asset")
		}
		if a.IsCKBytes {
			continue
		} else {
			_, err := asset.IsSUDTAsset(a)
			if err != nil {
				return nil, err
			}

			newSudtDistribution := newState.Balances().Assets().Get(uint(sudtIndex)).ToUnion().IntoSUDTBalances().Distribution()
			balA := newSudtDistribution.Nth0()
			balB := newSudtDistribution.Nth1()
			state.Balances[sudtIndex] = []channel.Bal{
				0: molecule2.UnpackUint128(balA).Big(),
				1: molecule2.UnpackUint128(balB).Big(),
			}
		}
	}

	return state, nil
}
