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

// VCCoordinateInfo carries the data needed to materialise a recursive
// coordinate action that locks both the parent ledger channel and its
// virtual-channel child to canonical states in one transaction. The contract
// requires both witnesses in the same tx (VCTS verifies the parent is
// coordinating via `verify_parent_in_coordinate`).
type VCCoordinateInfo struct {
	// Parent LC fields.
	ChannelCell types.OutPoint
	LCStatus    molecule.ChannelStatus
	LCState     *channel.State
	LCSigA      gpwallet.Sig
	LCSigB      gpwallet.Sig
	LCCoordSig  gpwallet.Sig

	// VC fields.
	VCCell     types.OutPoint
	VCStatus   molecule.VirtualChannelStatus
	VCState    *channel.State
	VCSigA     gpwallet.Sig
	VCSigB     gpwallet.Sig
	VCCoordSig gpwallet.Sig

	Params types.Hash // placeholder; unused — kept symmetric with VcDisputeInfo
	Header types.Hash
	PCTS   *types.Script
	VCTS   *types.Script

	// InputChannelCapacity / InputVCCapacity preserve the consumed cells'
	// capacities into the rebuilt output cells, mirroring the dispute path.
	InputChannelCapacity uint64
	InputVCCapacity      uint64
}

// updatedLCStatus and updatedVCStatus return the output statuses with the
// canonical state replaced and `coordinated=true` set. disputed/vc_disputed
// are preserved from the input statuses (the contract requires them set).
func (ci *VCCoordinateInfo) updatedLCStatus() (molecule.ChannelStatus, error) {
	packed, err := encoding.PackChannelState(ci.LCState)
	if err != nil {
		return molecule.ChannelStatus{}, fmt.Errorf("packing canonical LC state: %w", err)
	}
	b := ci.LCStatus.AsBuilder()
	return b.State(packed).Coordinated(encoding.True).Build(), nil
}

func (ci *VCCoordinateInfo) updatedVCStatus() (molecule.VirtualChannelStatus, error) {
	packed, err := encoding.PackChannelState(ci.VCState)
	if err != nil {
		return molecule.VirtualChannelStatus{}, fmt.Errorf("packing canonical VC state: %w", err)
	}
	b := ci.VCStatus.AsBuilder()
	return b.Vcstate(packed).Coordinated(encoding.True).Build(), nil
}
