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

// multiLedgerChallengeDuration is the channel challenge duration the contract
// treats as milliseconds (it is added to a CKB block timestamp in
// verify_time_lock_expired). We use the same small value as the existing CKB
// client tests (channel/test ChallengeDurationBlocks*time.Second/BlockInterval
// = 90*2 = 180): the window elapses quickly so that by the time the coordinator
// submits — several seconds later, after the watcher polls — the tip block's
// timestamp is safely past the deadline. A large (e.g. 45s) value makes the
// flow boundary-sensitive, since the latest available block timestamp always
// lags wall-clock by the block interval.
const multiLedgerChallengeDuration = uint64(180)

// TestMultiLedgerCoordinate tests the happy-path coordinated settlement:
// Alice and Bob open a multi-ledger channel, Alice disputes at version N, the
// challenge window elapses, Charlie coordinates the dispute to the canonical
// state, and both parties withdraw correctly.
func TestMultiLedgerCoordinate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()
	mlt := clienttest.SetupMultiLedgerTest(t, testDuration)
	ctest.TestMultiLedgerCoordinate(ctx, t, mlt, multiLedgerChallengeDuration)
}

// skipReasonTwoChain explains why the divergent-settlement attack harnesses
// cannot run against this single-devnet setup.
//
// Both go-perun attack harnesses drive TWO independent, fully functional
// ledgers: they call bob.Adjudicator2.Register(v1) / .Withdraw, wait on
// sub2.Next() for that ledger's RegisteredEvent, coordinate on bID2, and read
// BalanceReader2 — producing genuinely divergent per-chain balances that the
// assertions compare. This setup backs Asset2 (the synthetic ETH ledger) with a
// no-op adjudicator and a zero balance reader: enough for TestMultiLedger
// Coordinate (the coordinate happens on the one real CKB chain with Asset2
// zeroed), but the attack harnesses would block forever on sub2.Next() and
// cannot reproduce a divergent settlement.
//
// The attack scenario is inherently the CKB<->ETH two-chain PoC: it needs a
// real second ledger (perun-eth-backend's simulated EVM chain, or a functional
// in-memory mock) as chain B. That harness is the tracked follow-up (see the
// plan's "Out of scope" + Layer 6 notes). TestMultiLedgerCoordinate already
// validates the on-chain coordinate action, CoordinatedEvent emission, and
// coordinated settlement against the live devnet.
const skipReasonTwoChain = "requires a functional second (ETH) ledger; the divergent-settlement attack is the CKB<->ETH two-chain PoC follow-up. TestMultiLedgerCoordinate covers the on-chain coordinate path on the single devnet."

// TestMultiLedgerAttackNoCoordinator proves the divergent-settlement attack
// succeeds WITHOUT a coordinator. Skipped here: needs a functional second
// ledger (see skipReasonTwoChain).
func TestMultiLedgerAttackNoCoordinator(t *testing.T) {
	t.Skip(skipReasonTwoChain)
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()
	mlt := clienttest.SetupMultiLedgerTest(t, testDuration)
	ctest.TestMultiLedgerAttackNoCoordinator(ctx, t, mlt, multiLedgerChallengeDuration)
}

// TestMultiLedgerAttackCoordinate is the key security test: Bob tries to
// register a stale state v1 on one ledger while Alice has updated to v2; the
// coordinator coordinates to v2 on both ledgers so Bob cannot profit from the
// divergent-settlement attack. Skipped here: needs a functional second ledger
// (see skipReasonTwoChain).
func TestMultiLedgerAttackCoordinate(t *testing.T) {
	t.Skip(skipReasonTwoChain)
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()
	mlt := clienttest.SetupMultiLedgerTest(t, testDuration)
	ctest.TestMultiLedgerAttackCoordinate(ctx, t, mlt, multiLedgerChallengeDuration)
}
