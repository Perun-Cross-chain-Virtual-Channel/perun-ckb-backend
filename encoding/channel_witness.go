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

package encoding

import (
	"fmt"

	"github.com/nervosnetwork/ckb-sdk-go/v2/types/molecule"
	gpwallet "perun.network/go-perun/wallet"
)

// PackCoordinateWitness packs the canonical state, the two participant
// signatures, and the coordinator signature into a ChannelWitness::Coordinate
// redeemer. All three signatures are normalised through NewMoleculeSignature
// (raw 65-byte → DER, low-S canonicalised) so they verify against the contract's
// k256 ECDSA::from_der path.
func PackCoordinateWitness(
	state molecule.ChannelState,
	sigA, sigB, coordSig gpwallet.Sig,
) (molecule.ChannelWitness, error) {
	a, err := NewMoleculeSignature(sigA)
	if err != nil {
		return molecule.ChannelWitness{}, fmt.Errorf("packing sig A: %w", err)
	}
	b, err := NewMoleculeSignature(sigB)
	if err != nil {
		return molecule.ChannelWitness{}, fmt.Errorf("packing sig B: %w", err)
	}
	c, err := NewMoleculeSignature(coordSig)
	if err != nil {
		return molecule.ChannelWitness{}, fmt.Errorf("packing coordinator sig: %w", err)
	}
	coordinate := molecule.NewCoordinateBuilder().
		State(state).
		SigA(*a).
		SigB(*b).
		CoordSig(*c).
		Build()
	return molecule.NewChannelWitnessBuilder().
		Set(molecule.ChannelWitnessUnionFromCoordinate(coordinate)).
		Build(), nil
}
