// Copyright 2025 PolyCrypt GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !testnet

package client_test

import (
	"context"
	"testing"

	ctest "perun.network/go-perun/client/test"
	clienttest "perun.network/perun-ckb-backend/client/test"
)

// multiLedgerVirtualChallengeDuration mirrors the value baked into the harness
// setup; the go-perun VC harnesses read the challenge from the proposal, so this
// is only the value passed to the (unused-by-some) challengeDuration parameter.
const multiLedgerVirtualChallengeDuration = uint64(180)

// TestMultiLedgerVirtualHappy is the optimistic multi-ledger virtual-channel
// sanity check: open Alice-Hub and Bob-Hub parent ledger channels spanning the
// CKB (real) + synthetic-ETH ledgers, open an Alice-Bob virtual channel, update
// it, settle it cooperatively, then settle the parents. No dispute or coordinate.
func TestMultiLedgerVirtualHappy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()
	mlv := clienttest.SetupMultiLedgerVirtualTest(t, testDuration)
	ctest.TestMultiLedgerVirtualHappy(ctx, t, mlv, multiLedgerVirtualChallengeDuration)
}

// TestMultiLedgerVirtualDispute is Model 2 (time-lock fallback): the parent
// ledger channels carry NO coordinator; they dispute and force-close via their
// challenge window, and the virtual channel settles via its own time-lock. This
// exercises the existing DisputeVC / ForceCloseWithVC path with multi-ledger
// parents (a new dimension over the single-asset TestCrossVirtualChannelDispute).
func TestMultiLedgerVirtualDispute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()
	mlv := clienttest.SetupMultiLedgerVirtualTest(t, testDuration)
	ctest.TestMultiLedgerVirtualDispute(ctx, t, mlv, multiLedgerVirtualChallengeDuration)
}

// TestMultiLedgerVirtualCoordinate is Model 1 (recursive coordinate): the parent
// ledger channels carry a coordinator; after the channels are registered the
// coordinator co-signs the canonical tree (parent + bundled virtual sub-channel)
// and the parents force-close. This is the first on-chain validation of the
// recursive CoordinateVC path.
func TestMultiLedgerVirtualCoordinate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()
	mlv := clienttest.SetupMultiLedgerVirtualTest(t, testDuration)
	ctest.TestMultiLedgerVirtualCoordinate(ctx, t, mlv, multiLedgerVirtualChallengeDuration)
}
