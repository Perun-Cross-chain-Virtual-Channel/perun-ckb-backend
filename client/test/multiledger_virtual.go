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

package test

import (
	"math/big"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"perun.network/go-perun/channel"
	ctest "perun.network/go-perun/client/test"
	"perun.network/go-perun/wire"

	"perun.network/perun-ckb-backend/channel/asset"
	chtest "perun.network/perun-ckb-backend/channel/test"
	"perun.network/perun-ckb-backend/wallet"
	"perun.network/perun-ckb-backend/wallet/address"
)

// SetupMultiLedgerVirtualTest creates a multi-ledger virtual-channel test setup
// against the local CKB devnet, suitable for go-perun's shared VC harnesses:
// ctest.TestMultiLedgerVirtualHappy, TestMultiLedgerVirtualDispute,
// TestMultiLedgerVirtualCoordinate.
//
// Topology (mirrors the LC harness, one CKB devnet = two logical ledgers):
//   - Asset1: CKByte (NervosAsset, backend 3, ledger "03") — real on-chain.
//   - Asset2: synthetic EthAsset (backend 1, chainID 1) — encoded in each CKB
//     cell as a zeroed ETHBalances row so is_multi_ledger_state=true and the
//     contract's coordinator gate engages. BalanceReader2 always returns 0; all
//     Asset2 rows stay {0,0} so the harness's Asset2 assertions pass trivially.
//
// Participants: Alice, Bob, Hub (=ingrid). The Coordinator (Charlie) signs the
// canonical state (coord_sig). Its CKB client is built with the HUB's signer:
// the Hub is the common participant of BOTH parent channels (Alice-Hub and
// Bob-Hub), so a Hub-locked fee input satisfies each parent's PCLS when the
// coordinate tx is submitted (see mlCoordinator). Charlie's key is still used
// for coord_sig via WalletAccount.
func SetupMultiLedgerVirtualTest(t *testing.T, testDuration time.Duration) ctest.MultiLedgerVirtualSetup {
	t.Helper()
	rng := rand.New(rand.NewSource(0))
	_ = testDuration

	// Load deployment.
	sudtOwnerLockArg, err := chtest.ParseSUDTOwnerLockArg(devnetDir + "/accounts/sudt-owner-lock-hash.txt")
	require.NoError(t, err)
	d, _, err := chtest.GetDeployment(
		devnetDir+"/contract/migrations_0/dev/",
		devnetDir+"/contract/migrations_1/dev/",
		devnetDir+"/contract/migrations_vc/dev/",
		devnetDir+"/system_scripts",
		sudtOwnerLockArg,
	)
	require.NoError(t, err)

	// Load account keys: Alice, Bob, Hub (=ingrid), Charlie (=genesis-1, the
	// coordinator — distinct from the three channel participants).
	keyAlice, err := chtest.GetKey(devnetDir + "/accounts/alice.pk")
	require.NoError(t, err)
	keyBob, err := chtest.GetKey(devnetDir + "/accounts/bob.pk")
	require.NoError(t, err)
	keyHub, err := chtest.GetKey(devnetDir + "/accounts/ingrid.pk")
	require.NoError(t, err)
	keyCharlie, err := chtest.GetKey(devnetDir + "/accounts/genesis-1.pk")
	require.NoError(t, err)

	// Derive omni-lock participants and EVM addresses.
	_, evmAlice, err := address.NewEthereumParticipantFromPublicKey(keyAlice.PubKey(), d.OmniLockScript.CodeHash)
	require.NoError(t, err)
	_, evmBob, err := address.NewEthereumParticipantFromPublicKey(keyBob.PubKey(), d.OmniLockScript.CodeHash)
	require.NoError(t, err)
	_, evmHub, err := address.NewEthereumParticipantFromPublicKey(keyHub.PubKey(), d.OmniLockScript.CodeHash)
	require.NoError(t, err)

	accAlice := wallet.NewAccountFromPrivateKey(keyAlice, d.OmniLockScript.CodeHash, false)
	accBob := wallet.NewAccountFromPrivateKey(keyBob, d.OmniLockScript.CodeHash, false)
	accHub := wallet.NewAccountFromPrivateKey(keyHub, d.OmniLockScript.CodeHash, false)
	accCharlie := wallet.NewAccountFromPrivateKey(keyCharlie, d.OmniLockScript.CodeHash, false)

	// Two logical ledger IDs (same as the LC harness).
	l1ID := asset.MakeCCID(asset.MakeContractID(ckbLedgerContractID)) // CKByte, backend 3
	l2ID := asset.MakeLedgerBackendID(big.NewInt(ethFakeChainID))     // ETH (synthetic), backend 1

	// Assets.
	asset1 := asset.NewCKBytesNervosAsset()
	ethZeroAddr := asset.EthAddress{}
	asset2Raw := asset.MakeEthAsset(big.NewInt(ethFakeChainID), &ethZeroAddr)
	asset2 := &asset2Raw

	bus := wire.NewLocalBus()

	alice := mlParticipant(t, rng, keyAlice, accAlice, evmAlice, d, asset1, asset2, l1ID, l2ID, bus)
	bob := mlParticipant(t, rng, keyBob, accBob, evmBob, d, asset1, asset2, l1ID, l2ID, bus)
	hub := mlParticipant(t, rng, keyHub, accHub, evmHub, d, asset1, asset2, l1ID, l2ID, bus)

	// Charlie signs the canonical state (coord_sig); the Hub (a participant in
	// both parent channels) is the CKB submitter that unlocks the parent channel
	// cell for the coordinate tx (PCLS requires a participant input).
	charlie := mlCoordinator(t, rng, accCharlie, keyHub, accHub, evmHub, d, asset1, asset2, l1ID, l2ID)

	// Balances mirror the single-ledger VC test (MakeVirtualChannelSetupCross),
	// projected onto the Asset1 (CKByte) row; the Asset2 (ETH) row is zeroed
	// everywhere so BalanceReader2=0 stays consistent. Parent channels start at
	// 100/100 CKB, the VC at 50/50, and one update moves 30 CKB Alice→Bob.
	ckb := func(v float64) *big.Int { return asset.CKByteToShannon(big.NewFloat(v)) }
	zero := func() *big.Int { return big.NewInt(0) }

	balances := ctest.MultiLedgerVirtualBalances{
		// [Asset1,Asset2][Alice,Hub]
		InitBalsAliceHub: channel.Balances{
			{ckb(100), ckb(100)},
			{zero(), zero()},
		},
		// [Asset1,Asset2][Bob,Hub]
		InitBalsBobHub: channel.Balances{
			{ckb(100), ckb(100)},
			{zero(), zero()},
		},
		// [Asset1,Asset2][Alice,Bob]
		InitBalsVirtual: channel.Balances{
			{ckb(50), ckb(50)},
			{zero(), zero()},
		},
		// VC after one update: Alice 50→20, Bob 50→80 (30 CKB Alice→Bob).
		UpdateBalsVirtual: channel.Balances{
			{ckb(20), ckb(80)},
			{zero(), zero()},
		},
		// Alice-Hub final: Alice=100-50+20=70, Hub=100-50+80=130.
		FinalBalsAlice: channel.Balances{
			{ckb(70), ckb(130)},
			{zero(), zero()},
		},
		// Bob-Hub final: Bob=100-50+80=130, Hub=100-50+20=70.
		FinalBalsBob: channel.Balances{
			{ckb(130), ckb(70)},
			{zero(), zero()},
		},
	}

	return ctest.MultiLedgerVirtualSetup{
		Alice:       alice,
		Bob:         bob,
		Hub:         hub,
		Coordinator: charlie,
		Asset1:      asset1,
		Asset2:      asset2,
		Balances:    balances,
		// Small challenge duration so the time-lock window elapses well before
		// the coordinate/force-close lands (see multiLedgerChallengeDuration).
		ChallengeDuration: multiLedgerVirtualChallengeDuration,
		// Allow up to 10 CKB fee tolerance per party (the VC + parents incur
		// several on-chain txs across open/fund/register/coordinate/settle).
		BalanceDelta:       asset.CKByteToShannon(big.NewFloat(10)),
		WaitWatcherTimeout: 1 * time.Second,
		IsUTXO:             true,
	}
}

// multiLedgerVirtualChallengeDuration is the challenge duration (treated as
// milliseconds by the contract) for the VC harnesses. Kept small for the same
// reason as multiLedgerChallengeDuration: the window must elapse before the
// coordinate/force-close tx is mined despite the newest block timestamp lagging
// wall-clock by the block interval.
const multiLedgerVirtualChallengeDuration = uint64(180)
