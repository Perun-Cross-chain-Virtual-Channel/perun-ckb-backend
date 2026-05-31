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

package coordinator_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nervosnetwork/ckb-sdk-go/v2/types"
	"github.com/nervosnetwork/ckb-sdk-go/v2/types/molecule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"perun.network/go-perun/channel"
	"perun.network/go-perun/wallet"

	"perun.network/perun-ckb-backend/channel/coordinator"
	"perun.network/perun-ckb-backend/client"
)

// fakeClient records Coordinate/CoordinateVC invocations and can be told to
// fail (modelling a contract rejection, e.g. a wrong coordinator signature).
type fakeClient struct {
	coordinateCalls   int
	coordinateVCCalls int

	lastID       channel.ID
	lastState    *channel.State
	lastSigs     []wallet.Sig
	lastCoordSig wallet.Sig

	lastParentID, lastVCID         channel.ID
	lastParentCoordSig, lastVCCSig wallet.Sig

	coordinateErr error
}

func (f *fakeClient) Coordinate(ctx context.Context, id channel.ID, canonicalState *channel.State, sigs []wallet.Sig, coordSig wallet.Sig, params *channel.Params) error {
	f.coordinateCalls++
	f.lastID = id
	f.lastState = canonicalState
	f.lastSigs = sigs
	f.lastCoordSig = coordSig
	return f.coordinateErr
}

func (f *fakeClient) CoordinateVC(ctx context.Context, parentID, vcID channel.ID, parentState, vcState *channel.State, parentSigs, vcSigs []wallet.Sig, parentCoordSig, vcCoordSig wallet.Sig, parentParams, vcParams *channel.Params) error {
	f.coordinateVCCalls++
	f.lastParentID = parentID
	f.lastVCID = vcID
	f.lastParentCoordSig = parentCoordSig
	f.lastVCCSig = vcCoordSig
	return f.coordinateErr
}

func (f *fakeClient) Start(context.Context, *channel.Params, *channel.State) (*types.Script, error) {
	panic("unexpected")
}
func (f *fakeClient) Abort(context.Context, *types.Script, *channel.Params, *channel.State) error {
	panic("unexpected")
}
func (f *fakeClient) Fund(context.Context, *types.Script, *channel.State, *channel.Params) error {
	panic("unexpected")
}
func (f *fakeClient) Dispute(context.Context, channel.ID, *channel.State, []wallet.Sig, *channel.Params) error {
	panic("unexpected")
}
func (f *fakeClient) DisputeVC(context.Context, channel.ID, channel.ID, *channel.State, *channel.State, *channel.Params, *channel.Params, []wallet.Sig, []wallet.Sig, []channel.Index) error {
	panic("unexpected")
}
func (f *fakeClient) Close(context.Context, channel.ID, *channel.State, []wallet.Sig, *channel.Params) error {
	panic("unexpected")
}
func (f *fakeClient) ForceClose(context.Context, channel.ID, *channel.State, *channel.Params) error {
	panic("unexpected")
}
func (f *fakeClient) ForceCloseWithVC(context.Context, channel.ID, channel.ID, *channel.State, *channel.State, []wallet.Sig, *channel.Params, []channel.Index) error {
	panic("unexpected")
}
func (f *fakeClient) GetChannelWithID(context.Context, channel.ID) (client.BlockNumber, *types.Script, *molecule.ChannelConstants, *molecule.ChannelStatus, error) {
	return 0, nil, nil, nil, client.ErrNoChannelLiveCell
}
func (f *fakeClient) GetChannelWithExactPCTS(context.Context, *types.Script) (client.BlockNumber, *molecule.ChannelStatus, error) {
	return 0, nil, client.ErrNoChannelLiveCell
}
func (f *fakeClient) GetBlockTime(context.Context, client.BlockNumber) (time.Time, error) {
	return time.Time{}, nil
}

func mkReq(id channel.ID, version uint64) channel.AdjudicatorReq {
	st := &channel.State{ID: id, Version: version, App: channel.NoApp(), Data: channel.NoData()}
	return channel.AdjudicatorReq{
		Params: &channel.Params{},
		Tx:     channel.Transaction{State: st, Sigs: []wallet.Sig{{0xA}, {0xB}}},
	}
}

func TestCoordinateLedgerOnly(t *testing.T) {
	fc := &fakeClient{}
	c := coordinator.NewCoordinator(fc)
	id := channel.ID{0xAA}
	req := mkReq(id, 7)

	err := c.Coordinate(context.Background(), req, nil, []wallet.Sig{{0xC0}})
	require.NoError(t, err)

	assert.Equal(t, 1, fc.coordinateCalls, "ledger Coordinate called once")
	assert.Equal(t, 0, fc.coordinateVCCalls, "VC path not taken")
	assert.Equal(t, id, fc.lastID)
	assert.Equal(t, req.Tx.State, fc.lastState)
	assert.Equal(t, req.Tx.Sigs, fc.lastSigs, "participant sigs forwarded")
	assert.Equal(t, wallet.Sig{0xC0}, fc.lastCoordSig, "coordSigs[0] used as coordinator sig")
}

func TestCoordinateVC(t *testing.T) {
	fc := &fakeClient{}
	c := coordinator.NewCoordinator(fc)
	parentID := channel.ID{0x01}
	vcID := channel.ID{0x02}
	req := mkReq(parentID, 5)
	vc := channel.SignedState{
		Params: &channel.Params{},
		State:  &channel.State{ID: vcID, Version: 5, App: channel.NoApp(), Data: channel.NoData()},
		Sigs:   []wallet.Sig{{0x1}, {0x2}},
	}

	err := c.Coordinate(context.Background(), req, []channel.SignedState{vc}, []wallet.Sig{{0xAA}, {0xBB}})
	require.NoError(t, err)

	assert.Equal(t, 0, fc.coordinateCalls)
	assert.Equal(t, 1, fc.coordinateVCCalls, "VC Coordinate called once")
	assert.Equal(t, parentID, fc.lastParentID)
	assert.Equal(t, vcID, fc.lastVCID)
	assert.Equal(t, wallet.Sig{0xAA}, fc.lastParentCoordSig, "coordSigs[0] -> parent")
	assert.Equal(t, wallet.Sig{0xBB}, fc.lastVCCSig, "coordSigs[1] -> vc")
}

func TestCoordinateTooManySubStates(t *testing.T) {
	fc := &fakeClient{}
	c := coordinator.NewCoordinator(fc)
	req := mkReq(channel.ID{0x01}, 1)
	sub := channel.SignedState{Params: &channel.Params{}, State: &channel.State{App: channel.NoApp(), Data: channel.NoData()}}

	err := c.Coordinate(context.Background(), req, []channel.SignedState{sub, sub}, []wallet.Sig{{1}, {2}, {3}})
	require.Error(t, err)
	assert.Equal(t, 0, fc.coordinateCalls)
	assert.Equal(t, 0, fc.coordinateVCCalls)
}

func TestCoordinateMissingCoordSig(t *testing.T) {
	fc := &fakeClient{}
	c := coordinator.NewCoordinator(fc)
	req := mkReq(channel.ID{0x01}, 1)

	err := c.Coordinate(context.Background(), req, nil, nil)
	require.Error(t, err, "missing coordinator signature must error before hitting the client")
	assert.Equal(t, 0, fc.coordinateCalls)
}

// TestCoordinateWrongCoordinatorRejected models the contract-level rejection of
// a coordinate tx whose coordinator signature does not match the configured
// coordinator (Rust test_coordinate_wrong_coordinator_rejected). The CKB
// client surfaces the revert as an error, which the Coordinator propagates
// verbatim.
func TestCoordinateWrongCoordinatorRejected(t *testing.T) {
	wantErr := errors.New("contract: invalid coordinator signature")
	fc := &fakeClient{coordinateErr: wantErr}
	c := coordinator.NewCoordinator(fc)
	req := mkReq(channel.ID{0x01}, 1)

	err := c.Coordinate(context.Background(), req, nil, []wallet.Sig{{0xBA, 0xD0}})
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.Equal(t, 1, fc.coordinateCalls, "client was invoked; rejection comes from the contract")
}
