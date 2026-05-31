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

package transaction

import (
	"fmt"

	"github.com/nervosnetwork/ckb-sdk-go/v2/types"
	"github.com/nervosnetwork/ckb-sdk-go/v2/types/molecule"
	"perun.network/go-perun/channel"
	gpwallet "perun.network/go-perun/wallet"
	"perun.network/perun-ckb-backend/encoding"
)

// CoordinateInfo carries the data needed to materialise a `coordinate` action
// on a PCTS channel cell: the live cell to consume, the canonical state the
// coordinator certified, the two participant signatures and the coordinator
// signature over that state, and the prior on-chain ChannelStatus. The output
// status is derived from Status with `coordinated=true` and `state` replaced.
type CoordinateInfo struct {
	ChannelCell types.OutPoint
	Status      molecule.ChannelStatus
	NewState    *channel.State
	Params      *channel.Params
	// Headers must include the block header of the channel input cell (for the
	// contract's load_header(0, GroupInput) in verify_time_lock_expired) and the
	// current tip header (for find_closest_current_time). Mirrors ForceCloseInfo.
	Headers  []types.Hash
	PCTS     *types.Script
	SigA     gpwallet.Sig
	SigB     gpwallet.Sig
	CoordSig gpwallet.Sig

	// InputChannelCapacity is the actual capacity of the channel cell being
	// consumed. The rebuilt channel cell preserves it so the reserved sub-alloc
	// capacity stays conserved across the coordinate (cf.
	// encoding.LockedSubAllocReserve).
	InputChannelCapacity uint64
}

func NewCoordinateInfo(
	channelCell types.OutPoint,
	status molecule.ChannelStatus,
	newState *channel.State,
	params *channel.Params,
	headers []types.Hash,
	pcts *types.Script,
	sigA, sigB, coordSig gpwallet.Sig,
	inputChannelCapacity uint64,
) *CoordinateInfo {
	return &CoordinateInfo{
		ChannelCell:          channelCell,
		Status:               status,
		NewState:             newState,
		Params:               params,
		Headers:              headers,
		PCTS:                 pcts,
		SigA:                 sigA,
		SigB:                 sigB,
		CoordSig:             coordSig,
		InputChannelCapacity: inputChannelCapacity,
	}
}

// updatedStatus returns the output ChannelStatus: state replaced with the
// canonical state, coordinated set to True. The disputed flag is preserved
// from the input status (the contract requires disputed=true to coordinate).
func (ci *CoordinateInfo) updatedStatus() (molecule.ChannelStatus, error) {
	packed, err := encoding.PackChannelState(ci.NewState)
	if err != nil {
		return molecule.ChannelStatus{}, fmt.Errorf("packing canonical state: %w", err)
	}
	b := ci.Status.AsBuilder()
	return b.State(packed).Coordinated(encoding.True).Build(), nil
}
