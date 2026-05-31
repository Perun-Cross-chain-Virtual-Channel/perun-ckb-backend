package adjudicator

import (
	"context"
	"log"

	"github.com/pkg/errors"
	"perun.network/go-perun/channel"
	"perun.network/perun-ckb-backend/client"
	"perun.network/perun-ckb-backend/encoding"
)

type Adjudicator struct {
	client       client.CKBClient
	assetFactory encoding.AssetFactory
}

func NewAdjudicator(client client.CKBClient) *Adjudicator {
	return &Adjudicator{client: client}
}

// NewAdjudicatorWithAssetFactory is like NewAdjudicator but injects an
// AssetFactory into the polling subscription emitted by Subscribe. The factory
// controls how the CoordinatedEvent state is reconstructed from the on-chain
// molecule encoding (needed for multi-ledger harnesses where assets must match
// the originals). nil falls back to encoding.DefaultAssetFactory.
func NewAdjudicatorWithAssetFactory(client client.CKBClient, factory encoding.AssetFactory) *Adjudicator {
	return &Adjudicator{client: client, assetFactory: factory}
}

func (a Adjudicator) Register(ctx context.Context, req channel.AdjudicatorReq, states []channel.SignedState) error {
	// If sub-states are present, register them first
	if len(states) > 0 {
		vcstate := states[0]
		indexMap := req.Tx.Locked[0].IndexMap

		if err := a.client.DisputeVC(ctx, vcstate.State.ID, req.Tx.ID, vcstate.State, req.Tx.State, vcstate.Params, req.Params, vcstate.Sigs, req.Tx.Sigs, indexMap); err != nil {
			return errors.WithMessage(err, "failed to dispute virtual channel")
		}
		return nil // Only one virtual channel is supported

	}
	return a.client.Dispute(ctx, req.Tx.ID, req.Tx.State, req.Tx.Sigs, req.Params)
}

func (a Adjudicator) Withdraw(ctx context.Context, req channel.AdjudicatorReq, stateMap channel.StateMap) error {
	if req.Secondary { // Secondary withdraw already handled by first withdraw.
		log.Println("Adjudicator: Secondary withdraw already handled by first withdraw.")
		return nil
	}
	if req.Tx.IsFinal {
		return a.client.Close(ctx, req.Tx.ID, req.Tx.State, req.Tx.Sigs, req.Params)
	} else {
		// Check length of state map: currentyl only one virtual channel is supported
		if len(stateMap) > 0 {
			if len(stateMap) > 1 {
				return errors.New("only one virtual channel is supported")
			}

			// Force Close with Virtual Channel.
			for _, vcstate := range stateMap {
				indexMap := req.Tx.Locked[0].IndexMap
				return a.client.ForceCloseWithVC(ctx, req.Tx.ID, vcstate.ID, req.Tx.State, vcstate, req.Tx.Sigs, req.Params, indexMap)
			}
		}

		return a.client.ForceClose(ctx, req.Tx.ID, req.Tx.State, req.Params)
	}

}

func (a Adjudicator) Progress(ctx context.Context, req channel.ProgressReq) error {
	// Progress only needed for state channels
	panic("unimplemented: Progress only needed for state channels")
}

func (a Adjudicator) Subscribe(ctx context.Context, id channel.ID) (channel.AdjudicatorSubscription, error) {
	return NewAdjudicatorSubFromChannelIDWithAssetFactory(ctx, a.client, id, a.assetFactory), nil
}
