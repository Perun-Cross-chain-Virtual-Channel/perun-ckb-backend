package adjudicator

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/nervosnetwork/ckb-sdk-go/v2/types"
	"github.com/nervosnetwork/ckb-sdk-go/v2/types/molecule"
	"perun.network/go-perun/channel"
	"perun.network/perun-ckb-backend/client"
	"perun.network/perun-ckb-backend/encoding"
	molecule2 "perun.network/perun-ckb-backend/encoding/molecule"
)

const (
	DefaultBufferSize                  = 3
	DefaultSubscriptionPollingInterval = time.Duration(4) * time.Second
)

type PollingSubscription struct {
	PollingInterval   time.Duration
	client            client.CKBClient
	id                channel.ID
	pcts              *types.Script
	events            chan channel.AdjudicatorEvent
	err               error
	cancel            context.CancelFunc
	foundLiveCellOnce bool
	consecutiveMisses int
	concluded         chan struct{}
	fatalErrors       chan error
	challengeDuration *time.Duration
	// assetFactory reconstructs the canonical channel.State emitted in a
	// CoordinatedEvent from the on-chain molecule encoding. nil falls back to
	// encoding.DefaultAssetFactory (raw CKB-backend assets); the multi-ledger
	// harness supplies a factory that wraps assets to match the originals.
	assetFactory encoding.AssetFactory
}

// ConcludeMissThreshold is the number of consecutive missed live-cell lookups
// required before we emit a ConcludedEvent. This protects against transient
// indexer/RPC inconsistencies falsely concluding a live channel.
const ConcludeMissThreshold = 3

func NewAdjudicatorSubFromChannelID(ctx context.Context, ckbClient client.CKBClient, id channel.ID) *PollingSubscription {
	return NewAdjudicatorSubFromChannelIDWithAssetFactory(ctx, ckbClient, id, nil)
}

// NewAdjudicatorSubFromChannelIDWithAssetFactory is like
// NewAdjudicatorSubFromChannelID but lets the caller supply the AssetFactory
// used to reconstruct the canonical state emitted in a CoordinatedEvent. A nil
// factory falls back to encoding.DefaultAssetFactory.
func NewAdjudicatorSubFromChannelIDWithAssetFactory(ctx context.Context, ckbClient client.CKBClient, id channel.ID, assetFactory encoding.AssetFactory) *PollingSubscription {
	sub := &PollingSubscription{
		PollingInterval: DefaultSubscriptionPollingInterval,
		client:          ckbClient,
		id:              id,
		events:          make(chan channel.AdjudicatorEvent, DefaultBufferSize),
		concluded:       make(chan struct{}, 1),
		fatalErrors:     make(chan error, 1),
		assetFactory:    assetFactory,
	}
	ctx, sub.cancel = context.WithCancel(ctx)
	go sub.run(ctx)
	return sub
}

func (a *PollingSubscription) run(ctx context.Context) {
	finish := func(err error) {
		a.err = err
		close(a.events)
	}
	var oldStatus *molecule.ChannelStatus
	for {
		select {
		case err := <-a.fatalErrors:
			finish(err)
			return
		case <-a.concluded:
			finish(nil)
			return
		case <-ctx.Done():
			finish(nil)
			return
		case <-time.After(a.PollingInterval):
			blockNumber, newStatus, err := a.pollStatus(ctx)
			foundLiveCell := true
			if err != nil {
				if !errors.Is(err, client.ErrNoChannelLiveCell) {
					continue
				}
				foundLiveCell = false
			}
			statusDidChange := a.emitEventIfNecessary(ctx, oldStatus, newStatus, blockNumber, foundLiveCell)
			if statusDidChange {
				oldStatus = newStatus
			}
		}
	}
}

func (a *PollingSubscription) pollStatus(ctx context.Context) (client.BlockNumber, *molecule.ChannelStatus, error) {
	if a.pcts != nil {
		return a.client.GetChannelWithExactPCTS(ctx, a.pcts)
	}
	b, pcts, _, status, err := a.client.GetChannelWithID(ctx, a.id)
	if err != nil {
		return 0, nil, err
	}
	a.pcts = pcts
	return b, status, nil
}

// emitEventIfNecessary emits an event if the difference between oldStatus and newStatus indicates an event.
// It returns ture, iff the status has changed.
// Note: The status can change without warranting emission of an event!
func (a *PollingSubscription) emitEventIfNecessary(
	ctx context.Context,
	oldStatus *molecule.ChannelStatus,
	newStatus *molecule.ChannelStatus,
	newBlockNumber client.BlockNumber,
	foundLiveCell bool,
) bool {
	a.foundLiveCellOnce = a.foundLiveCellOnce || foundLiveCell
	if !a.foundLiveCellOnce {
		return false
	}
	if !foundLiveCell {
		// Require several consecutive misses before concluding, to avoid false
		// positives from transient indexer/RPC inconsistency or short reorgs.
		a.consecutiveMisses++
		if a.consecutiveMisses < ConcludeMissThreshold {
			return false
		}
		// TODO: figure out how to set the timeout and version for concluded events.
		// TODO: Do we want to verify that the channel is actually concluded here?
		a.events <- channel.NewConcludedEvent(a.id, &channel.ElapsedTimeout{}, 0)
		close(a.concluded)
		return true
	}
	// Live cell found again: reset miss counter.
	a.consecutiveMisses = 0
	if newStatus == nil {
		a.fatalErrors <- fmt.Errorf("a live cell was found but newStatus is nil")
		return false
	}

	// If oldStatus is nil, this is the first live cell we ever observed. We
	// establish it as the baseline. If the channel is ALREADY disputed or
	// coordinated when we first see it, emit the corresponding event
	// immediately: the transition may have happened before this subscription
	// started (typical when a peer or the coordinator subscribes after the
	// dispute landed, or when the poll interval is longer than the gap between
	// funding and dispute). Without this, a late subscriber would wait forever
	// for a transition that already occurred.
	if oldStatus == nil {
		if encoding.ToBool(*newStatus.Coordinated()) {
			return a.emitCoordinated(newStatus)
		}
		if encoding.ToBool(*newStatus.Disputed()) {
			return a.emitRegistered(ctx, newStatus, newBlockNumber)
		}
		return true
	}

	// If the status has not changed, we do not emit an event and signal that the status has not changed.
	if bytes.Equal(oldStatus.AsSlice(), newStatus.AsSlice()) {
		return false
	}
	log.Printf("adjudicator_sub: status changed (id=%x) funded=%v disputed=%v coordinated=%v",
		a.id[:4],
		encoding.ToBool(*newStatus.Funded()),
		encoding.ToBool(*newStatus.Disputed()),
		encoding.ToBool(*newStatus.Coordinated()),
	)
	if !encoding.ToBool(*oldStatus.Funded()) {
		return true
	}

	// Coordinated supersedes disputed: the contract only allows coordinated to
	// flip true after disputed is already set, so a false->true transition of
	// coordinated is the canonical-settlement signal. Emit it before the
	// dispute branch and carry the on-chain canonical state so the client's
	// machine adopts it for withdrawal (see go-perun machine.SetCoordinated).
	if encoding.ToBool(*newStatus.Coordinated()) && !encoding.ToBool(*oldStatus.Coordinated()) {
		return a.emitCoordinated(newStatus)
	}

	if !encoding.ToBool(*newStatus.Disputed()) {
		a.fatalErrors <- fmt.Errorf(
			"adjudicator_sub: channel received update but is not disputed. oldStatus: %s, newStatus: %s",
			hex.EncodeToString(oldStatus.AsSlice()),
			hex.EncodeToString(newStatus.AsSlice()),
		)
		return false
	}
	return a.emitRegistered(ctx, newStatus, newBlockNumber)
}

// emitRegistered emits a RegisteredEvent for the given disputed status. The
// challenge timeout is computed from the observation block; observing a dispute
// late only pushes the deadline later, which is safe (the contract enforces the
// real on-chain deadline regardless).
func (a *PollingSubscription) emitRegistered(ctx context.Context, status *molecule.ChannelStatus, blockNumber client.BlockNumber) bool {
	challengeDurationStart, err := a.getChallengeDurationStart(ctx, blockNumber)
	if err != nil {
		a.fatalErrors <- fmt.Errorf("could not get challenge duration start: %v", err)
		return false
	}
	challengeDuration, err := a.getChallengeDuration()
	if err != nil {
		a.fatalErrors <- fmt.Errorf("could not get challenge duration: %v", err)
		return false
	}
	a.events <- channel.NewRegisteredEvent(
		a.id,
		&channel.TimeTimeout{Time: challengeDurationStart.Add(challengeDuration)},
		molecule2.UnpackUint64(status.State().Version()),
		nil, // only needed for virtual channels
		nil, // only needed for virtual channels
	)
	return true
}

// emitCoordinated emits a CoordinatedEvent carrying the on-chain canonical
// state reconstructed from the molecule encoding (see Layer 4 / UnpackChannelState).
func (a *PollingSubscription) emitCoordinated(status *molecule.ChannelStatus) bool {
	state, err := encoding.UnpackChannelState(status.State(), a.assetFactory)
	if err != nil {
		a.fatalErrors <- fmt.Errorf("could not unpack coordinated channel state: %v", err)
		return false
	}
	a.events <- channel.NewCoordinatedEvent(
		a.id,
		&channel.ElapsedTimeout{},
		state,
		nil, // on-chain acceptance is authoritative; the client keeps its local sigs.
	)
	return true
}

// Next returns the next event from the subscription.
// It blocks until an event is available or the subscription is closed.
// It returns nil if the subscription is closed. or an error occurs
func (a *PollingSubscription) Next() channel.AdjudicatorEvent {
	return <-a.events
}

// Err returns the error that caused the subscription to close.
// The returned error is ErrChannelConcluded, iff the channel was concluded.
// The returned error is ErrSubscriptionClosedByContext, iff the subscription was closed by the context or via Close.
func (a *PollingSubscription) Err() error {
	return a.err
}

// Close closes the subscription.
func (a *PollingSubscription) Close() error {
	a.cancel()
	return nil
}

func (a *PollingSubscription) getChallengeDuration() (time.Duration, error) {
	if a.challengeDuration != nil {
		return *a.challengeDuration, nil
	}
	if a.pcts == nil {
		return 0, fmt.Errorf("cannot get challenge duration: pcts not set")
	}
	channelConstants, err := molecule.ChannelConstantsFromSlice(a.pcts.Args, false)
	if err != nil {
		return 0, err
	}

	duration := molecule2.UnpackUint64(channelConstants.Params().ChallengeDuration())
	if duration > math.MaxInt64 {
		panic(fmt.Sprintf("adjudicator_sub: challenge duration %d is too large, max: %d", duration, math.MaxInt64))
	}
	a.challengeDuration = new(time.Duration)
	*a.challengeDuration = time.Duration(duration) * time.Millisecond
	return *a.challengeDuration, nil
}

func (a *PollingSubscription) getChallengeDurationStart(ctx context.Context, blockNumber client.BlockNumber) (time.Time, error) {
	const retries = 5
	var challengeDurationStart time.Time
	var err error
	for i := 0; i < retries; i++ {
		challengeDurationStart, err = a.client.GetBlockTime(ctx, blockNumber)
		if err == nil {
			break
		}
	}
	return challengeDurationStart, err
}
