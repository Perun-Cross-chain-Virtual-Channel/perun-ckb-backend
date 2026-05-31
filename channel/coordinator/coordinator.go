// Copyright 2025 PolyCrypt GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package coordinator implements the CKB side of go-perun's coordinated
// settlement protocol. The Coordinator mirrors the adjudicator but only ever
// observes on-chain events (via the shared polling subscription) and issues the
// `coordinate` action; it never registers or withdraws. The trusted
// cross-chain-coordinator service drives it through go-perun's
// multi.Coordinator aggregator.
package coordinator

import (
	"context"

	"github.com/pkg/errors"
	"perun.network/go-perun/channel"
	"perun.network/go-perun/wallet"

	"perun.network/perun-ckb-backend/channel/adjudicator"
	"perun.network/perun-ckb-backend/client"
	"perun.network/perun-ckb-backend/encoding"
)

// Compile-time check that Coordinator satisfies the go-perun interface the
// cross-chain coordinator service dials.
var _ channel.CoordinatorSubscriber = (*Coordinator)(nil)

// Coordinator issues `coordinate` actions on the CKB ledger and exposes a
// read-only event subscription. It holds no signing keys of its own: the
// coordinator signatures are produced by the calling service and passed in.
type Coordinator struct {
	client client.CKBClient
	// assetFactory is forwarded to the polling subscription so the
	// CoordinatedEvent it emits carries a faithfully reconstructed state. nil
	// uses encoding.DefaultAssetFactory.
	assetFactory encoding.AssetFactory
}

// NewCoordinator creates a Coordinator over the given CKB client. The emitted
// CoordinatedEvent states are reconstructed with encoding.DefaultAssetFactory.
func NewCoordinator(c client.CKBClient) *Coordinator {
	return &Coordinator{client: c}
}

// NewCoordinatorWithAssetFactory is like NewCoordinator but lets the caller
// supply the AssetFactory used to reconstruct CoordinatedEvent states (needed
// by the multi-ledger harness so wrapped assets round-trip).
func NewCoordinatorWithAssetFactory(c client.CKBClient, factory encoding.AssetFactory) *Coordinator {
	return &Coordinator{client: c, assetFactory: factory}
}

// Coordinate satisfies channel.Coordinator. multi.Coordinator dispatches the
// same (req, signedStates, coordSigs) to every ledger coordinator after the
// cross-chain service has selected and signed the canonical states.
//
// Signature layout (mirrors the eth-backend recursive coordinate):
//   - coordSigs[0] is the coordinator signature over req.Tx.State.
//   - coordSigs[i+1] is the coordinator signature over signedStates[i].State.
//
// The CKB backend supports at most one virtual channel per parent, so at most
// one sub-state is accepted.
func (c *Coordinator) Coordinate(ctx context.Context, req channel.AdjudicatorReq, signedStates []channel.SignedState, coordSigs []wallet.Sig) error {
	switch len(signedStates) {
	case 0:
		if len(coordSigs) < 1 {
			return errors.New("coordinate: missing coordinator signature")
		}
		return c.client.Coordinate(
			ctx,
			req.Tx.ID,
			req.Tx.State,
			req.Tx.Sigs,
			coordSigs[0],
			req.Params,
		)
	case 1:
		if len(coordSigs) < 2 {
			return errors.New("coordinate (vc): need coordinator signatures for parent and sub-channel")
		}
		vc := signedStates[0]
		return c.client.CoordinateVC(
			ctx,
			req.Tx.ID,    // parent ledger channel
			vc.State.ID,  // virtual channel
			req.Tx.State, // parent canonical state
			vc.State,     // vc canonical state
			req.Tx.Sigs,  // parent participant sigs
			vc.Sigs,      // vc participant sigs
			coordSigs[0], // parent coordinator sig
			coordSigs[1], // vc coordinator sig
			req.Params,
			vc.Params,
		)
	default:
		return errors.Errorf("coordinate: CKB backend supports at most one virtual channel per parent, got %d sub-states", len(signedStates))
	}
}

// Subscribe satisfies channel.EventSubscriber. It reuses the adjudicator
// polling subscription: the coordinator only needs to observe RegisteredEvent
// (dispute landed) and CoordinatedEvent (coordinate landed); it never issues
// Register itself.
func (c *Coordinator) Subscribe(ctx context.Context, id channel.ID) (channel.AdjudicatorSubscription, error) {
	return adjudicator.NewAdjudicatorSubFromChannelIDWithAssetFactory(ctx, c.client, id, c.assetFactory), nil
}
