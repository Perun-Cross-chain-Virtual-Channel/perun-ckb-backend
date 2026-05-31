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

package adjudicator_test

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/nervosnetwork/ckb-sdk-go/v2/types"
	"github.com/nervosnetwork/ckb-sdk-go/v2/types/molecule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"perun.network/go-perun/channel"
	"perun.network/go-perun/wallet"

	"perun.network/perun-ckb-backend/channel/adjudicator"
	"perun.network/perun-ckb-backend/channel/asset"
	"perun.network/perun-ckb-backend/client"
	"perun.network/perun-ckb-backend/encoding"
)

// stubClient is a minimal CKBClient that replays a fixed sequence of channel
// statuses to the polling subscription. Only the lookup methods used by the
// subscription are implemented; the rest panic to catch unexpected calls.
type stubClient struct {
	mu       sync.Mutex
	pcts     *types.Script
	statuses []*molecule.ChannelStatus
	idx      int
}

func (c *stubClient) next() *molecule.ChannelStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.statuses[c.idx]
	if c.idx < len(c.statuses)-1 {
		c.idx++
	}
	return s
}

func (c *stubClient) GetChannelWithID(ctx context.Context, id channel.ID) (client.BlockNumber, *types.Script, *molecule.ChannelConstants, *molecule.ChannelStatus, error) {
	return 1, c.pcts, nil, c.next(), nil
}

func (c *stubClient) GetChannelWithExactPCTS(ctx context.Context, pcts *types.Script) (client.BlockNumber, *molecule.ChannelStatus, error) {
	return 1, c.next(), nil
}

func (c *stubClient) GetBlockTime(ctx context.Context, blockNumber client.BlockNumber) (time.Time, error) {
	return time.Now(), nil
}

func (c *stubClient) Start(context.Context, *channel.Params, *channel.State) (*types.Script, error) {
	panic("unexpected")
}
func (c *stubClient) Abort(context.Context, *types.Script, *channel.Params, *channel.State) error {
	panic("unexpected")
}
func (c *stubClient) Fund(context.Context, *types.Script, *channel.State, *channel.Params) error {
	panic("unexpected")
}
func (c *stubClient) Dispute(context.Context, channel.ID, *channel.State, []wallet.Sig, *channel.Params) error {
	panic("unexpected")
}
func (c *stubClient) DisputeVC(context.Context, channel.ID, channel.ID, *channel.State, *channel.State, *channel.Params, *channel.Params, []wallet.Sig, []wallet.Sig, []channel.Index) error {
	panic("unexpected")
}
func (c *stubClient) Close(context.Context, channel.ID, *channel.State, []wallet.Sig, *channel.Params) error {
	panic("unexpected")
}
func (c *stubClient) ForceClose(context.Context, channel.ID, *channel.State, *channel.Params) error {
	panic("unexpected")
}
func (c *stubClient) ForceCloseWithVC(context.Context, channel.ID, channel.ID, *channel.State, *channel.State, []wallet.Sig, *channel.Params, []channel.Index) error {
	panic("unexpected")
}
func (c *stubClient) Coordinate(context.Context, channel.ID, *channel.State, []wallet.Sig, wallet.Sig, *channel.Params) error {
	panic("unexpected")
}
func (c *stubClient) CoordinateVC(context.Context, channel.ID, channel.ID, *channel.State, *channel.State, []wallet.Sig, []wallet.Sig, wallet.Sig, wallet.Sig, *channel.Params, *channel.Params) error {
	panic("unexpected")
}

// mkStatus builds a synthetic ChannelStatus carrying a single-CKByte-asset
// state at the given version with the given flags set.
func mkStatus(t *testing.T, version uint64, funded, disputed, coordinated bool) *molecule.ChannelStatus {
	t.Helper()
	st := &channel.State{
		ID:      channel.ID{1, 2, 3},
		Version: version,
		App:     channel.NoApp(),
		Data:    channel.NoData(),
		Allocation: channel.Allocation{
			Assets:   []channel.Asset{asset.NewCKBytesNervosAsset()},
			Backends: []wallet.BackendID{wallet.BackendID(asset.CKBBackendID)},
			Balances: channel.Balances{{big.NewInt(70), big.NewInt(130)}},
		},
		IsFinal: false,
	}
	packed, err := encoding.PackChannelState(st)
	require.NoError(t, err)
	status := molecule.NewChannelStatusBuilder().
		State(packed).
		Funded(encoding.FromBool(funded)).
		Disputed(encoding.FromBool(disputed)).
		Coordinated(encoding.FromBool(coordinated)).
		Build()
	return &status
}

// TestPollingSubscriptionEmitsCoordinatedEvent asserts that when the on-chain
// channel status transitions coordinated false->true, the subscription emits a
// *channel.CoordinatedEvent whose State.Version matches the on-chain status.
func TestPollingSubscriptionEmitsCoordinatedEvent(t *testing.T) {
	pcts := &types.Script{Args: []byte{0x01}}
	stub := &stubClient{
		pcts: pcts,
		statuses: []*molecule.ChannelStatus{
			mkStatus(t, 1, true, false, false), // funded baseline (no event)
			mkStatus(t, 2, true, true, true),   // canonical v2, disputed+coordinated
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Default polling interval (4s) keeps the run loop race-free; two polls
	// (disputed, then coordinated) land well within the 30s context timeout.
	sub := adjudicator.NewAdjudicatorSubFromChannelID(ctx, stub, channel.ID{1, 2, 3})
	defer sub.Close()

	ev := sub.Next()
	require.NotNil(t, ev, "expected an event before subscription closed; err=%v", sub.Err())

	coord, ok := ev.(*channel.CoordinatedEvent)
	require.Truef(t, ok, "expected *channel.CoordinatedEvent, got %T", ev)
	assert.Equal(t, uint64(2), coord.Version(), "event version")
	require.NotNil(t, coord.State)
	assert.Equal(t, uint64(2), coord.State.Version, "reconstructed state version")
	require.Len(t, coord.State.Assets, 1)
	assert.Equal(t, big.NewInt(70), coord.State.Balances[0][0])
	assert.Equal(t, big.NewInt(130), coord.State.Balances[0][1])
}
