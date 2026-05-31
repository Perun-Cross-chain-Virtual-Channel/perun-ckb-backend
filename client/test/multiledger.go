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
	"context"
	"math/big"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/nervosnetwork/ckb-sdk-go/v2/rpc"
	ckbsigner "github.com/nervosnetwork/ckb-sdk-go/v2/transaction/signer"
	"github.com/nervosnetwork/ckb-sdk-go/v2/types"
	"github.com/stretchr/testify/require"

	gpwiretest "perun.network/go-perun/backend/sim/wire"
	"perun.network/go-perun/channel"
	"perun.network/go-perun/channel/multi"
	gpclient "perun.network/go-perun/client"
	ctest "perun.network/go-perun/client/test"
	gpwallet "perun.network/go-perun/wallet"
	"perun.network/go-perun/watcher/local"
	"perun.network/go-perun/wire"

	"perun.network/perun-ckb-backend/backend"
	"perun.network/perun-ckb-backend/channel/adjudicator"
	"perun.network/perun-ckb-backend/channel/asset"
	"perun.network/perun-ckb-backend/channel/coordinator"
	"perun.network/perun-ckb-backend/channel/funder"
	chtest "perun.network/perun-ckb-backend/channel/test"
	"perun.network/perun-ckb-backend/client"
	"perun.network/perun-ckb-backend/encoding"
	"perun.network/perun-ckb-backend/wallet"
	"perun.network/perun-ckb-backend/wallet/address"
	ckbwallettest "perun.network/perun-ckb-backend/wallet/test"
)

const (
	// ckbLedgerContractID is the ContractLID used for the CKB-native ledger.
	ckbLedgerContractID = "03"
	// ethFakeChainID is the chain ID of the synthetic ETH-ledger asset embedded
	// in the CKB channel cell to satisfy is_multi_ledger_state.
	ethFakeChainID = int64(1)
	// devnetDir is the path from the test binary working directory (client/) to
	// the devnet directory. Tests in client/ run with cwd=client/.
	devnetDir = "../devnet"
)

// noopFunder is a channel.Funder that succeeds without doing anything, used as
// a placeholder for the synthetic ETH-ledger slot in multi.Funder.
type noopFunder struct{}

func (noopFunder) Fund(_ context.Context, _ channel.FundingReq) error { return nil }

var _ channel.Funder = noopFunder{}

// noopAdjudicator is a channel.Adjudicator that succeeds without doing
// anything. The real CKB adjudicator handles both asset ledgers since the
// entire channel cell lives on CKB.
type noopAdjudicator struct{}

func (noopAdjudicator) Register(_ context.Context, _ channel.AdjudicatorReq, _ []channel.SignedState) error {
	return nil
}
func (noopAdjudicator) Withdraw(_ context.Context, _ channel.AdjudicatorReq, _ channel.StateMap) error {
	return nil
}
func (noopAdjudicator) Progress(_ context.Context, _ channel.ProgressReq) error { return nil }
func (noopAdjudicator) Subscribe(_ context.Context, _ channel.ID) (channel.AdjudicatorSubscription, error) {
	return newBlockingSub(), nil
}

var _ channel.Adjudicator = noopAdjudicator{}

// blockingSub is an AdjudicatorSubscription that blocks on Next() until
// Close() is called. The no-op adjudicator/coordinator must return this so
// that multi.AdjudicatorSubscription does not close prematurely: a nil return
// from Next() signals "subscription closed" to the aggregator, which would
// stop the watcher goroutine before any real events arrive from the CKB sub.
type blockingSub struct {
	done chan struct{}
	once sync.Once
}

func newBlockingSub() *blockingSub { return &blockingSub{done: make(chan struct{})} }

func (s *blockingSub) Next() channel.AdjudicatorEvent {
	<-s.done
	return nil
}
func (s *blockingSub) Err() error { return nil }
func (s *blockingSub) Close() error {
	s.once.Do(func() { close(s.done) })
	return nil
}

// noopCoordinator is a channel.CoordinatorSubscriber that does nothing.
type noopCoordinator struct{}

func (noopCoordinator) Coordinate(_ context.Context, _ channel.AdjudicatorReq, _ []channel.SignedState, _ []gpwallet.Sig) error {
	return nil
}
func (noopCoordinator) Subscribe(_ context.Context, _ channel.ID) (channel.AdjudicatorSubscription, error) {
	return newBlockingSub(), nil
}

var _ channel.CoordinatorSubscriber = noopCoordinator{}

// zeroBalanceReader returns zero for all assets — used for the synthetic
// ETH-ledger asset whose balances are not tracked on the CKB chain.
type zeroBalanceReader struct{}

func (*zeroBalanceReader) Balance(channel.Asset) channel.Bal { return big.NewInt(0) }

// multiLedgerAssetFactory maps the AssetDescriptors recovered from the on-chain
// molecule encoding back to the exact Asset1/Asset2 objects used at channel
// open, so the CoordinatedEvent's reconstructed state carries matching
// LedgerBackendIDs for multi.Adjudicator routing.
func multiLedgerAssetFactory(asset1 *asset.NervosAsset, asset2 *asset.EthAsset) encoding.AssetFactory {
	return func(d encoding.AssetDescriptor) (channel.Asset, gpwallet.BackendID, error) {
		if d.IsCKByte || d.SUDT != nil {
			return asset1, gpwallet.BackendID(asset.CKBBackendID), nil
		}
		return asset2, gpwallet.BackendID(asset.EthBackendID), nil
	}
}

// mlParticipant builds a ctest.MultiLedgerClient for one channel participant.
func mlParticipant(
	t *testing.T,
	rng *rand.Rand,
	key *secp256k1.PrivateKey,
	acc *wallet.Account,
	evmAddress [20]byte,
	d backend.Deployment,
	asset1 *asset.NervosAsset,
	asset2 *asset.EthAsset,
	l1ID multi.LedgerBackendID,
	l2ID multi.LedgerBackendID,
	bus wire.Bus,
) ctest.MultiLedgerClient {
	t.Helper()

	rpcClient, err := rpc.Dial(chtest.DevnetRpcNodeURL)
	require.NoError(t, err)

	evmSigner := backend.NewEVMSignerInstance(
		address.AsParticipant(acc.Address()).ToCKBAddress(types.NetworkTest),
		*key,
		types.NetworkTest,
		evmAddress,
	)
	txSigner := evmSigner.Signer()
	txSigner.RegisterLockSigner(d.OmniLockScript.CodeHash, &ckbsigner.OmnilockSigner{})

	ckbClient, err := client.NewClient(rpcClient, evmSigner, d)
	require.NoError(t, err)

	factory := multiLedgerAssetFactory(asset1, asset2)

	// multi.Funder: CKB side does all real funding (the ETH row lives inside
	// the CKB cell). ETH side is a no-op.
	multiFunder := multi.NewFunder()
	multiFunder.RegisterFunder(l1ID, funder.NewDefaultFunder(ckbClient, d))
	multiFunder.RegisterFunder(l2ID, noopFunder{})

	// multi.Adjudicator: CKB side handles the whole channel. ETH side is no-op.
	multiAdj := multi.NewAdjudicator()
	adj1 := adjudicator.NewAdjudicatorWithAssetFactory(ckbClient, factory)
	multiAdj.RegisterAdjudicator(l1ID, adj1)
	multiAdj.RegisterAdjudicator(l2ID, noopAdjudicator{})

	watcher, err := local.NewWatcher(multiAdj)
	require.NoError(t, err)

	epWallet := ckbwallettest.NewTestEphemeralWallet(acc)
	require.NoError(t, epWallet.AddAccount(acc))

	part := address.AsParticipant(acc.Address())
	wireAcc := gpwiretest.NewRandomAccount(rng)
	wireAddr := wireAcc.Address()

	perunWallet := map[gpwallet.BackendID]gpwallet.Wallet{
		gpwallet.BackendID(asset.CKBBackendID): epWallet,
	}
	wireAddress := map[gpwallet.BackendID]wire.Address{
		gpwallet.BackendID(asset.CKBBackendID): wireAddr,
	}

	c, err := gpclient.New(wireAddress, bus, multiFunder, multiAdj, perunWallet, watcher)
	require.NoError(t, err)

	rpcForBalance, err := rpc.Dial(chtest.DevnetRpcNodeURL)
	require.NoError(t, err)

	return ctest.MultiLedgerClient{
		Client:         c,
		Adjudicator1:   adj1,
		Adjudicator2:   noopAdjudicator{},
		WireAddress:    wireAddress,
		WalletAddress:  map[gpwallet.BackendID]gpwallet.Address{gpwallet.BackendID(asset.CKBBackendID): part},
		WalletAccount:  map[gpwallet.BackendID]gpwallet.Account{gpwallet.BackendID(asset.CKBBackendID): acc},
		Events:         make(chan channel.AdjudicatorEvent, 10),
		BalanceReader1: chtest.NewBalanceReader(rpcForBalance, acc.Address()),
		BalanceReader2: &zeroBalanceReader{},
	}
}

// mlCoordinator builds the ctest.MultiLedgerCoordinator (Charlie).
//
// The PCLS lock script requires that any transaction consuming the channel cell
// includes an input cell unlocked by one of the two channel PARTICIPANTS
// (party_a or party_b). The coordinator (Charlie) is a third party and cannot
// satisfy this on its own. The contract separates the two concerns: the
// coordinator only signs the canonical STATE (carried as coord_sig in the
// Coordinate witness), while a participant submits and unlocks the transaction.
// We mirror that here by building the coordinator's CKB client with a
// participant's (Alice's) signer, so the auto-added fee input carries party_a's
// lock hash and PCLS passes. Charlie's key is still used for coord_sig via
// WalletAccount (go-perun's MultiLedgerCoordinator.Sign reads it).
func mlCoordinator(
	t *testing.T,
	rng *rand.Rand,
	coordAcc *wallet.Account, // Charlie: signs the canonical state (coord_sig)
	submitterKey *secp256k1.PrivateKey, // Alice: unlocks/submits the coordinate tx
	submitterAcc *wallet.Account,
	submitterEvm [20]byte,
	d backend.Deployment,
	asset1 *asset.NervosAsset,
	asset2 *asset.EthAsset,
	l1ID multi.LedgerBackendID,
	l2ID multi.LedgerBackendID,
) ctest.MultiLedgerCoordinator {
	t.Helper()

	rpcClient, err := rpc.Dial(chtest.DevnetRpcNodeURL)
	require.NoError(t, err)

	evmSigner := backend.NewEVMSignerInstance(
		address.AsParticipant(submitterAcc.Address()).ToCKBAddress(types.NetworkTest),
		*submitterKey,
		types.NetworkTest,
		submitterEvm,
	)
	txSigner := evmSigner.Signer()
	txSigner.RegisterLockSigner(d.OmniLockScript.CodeHash, &ckbsigner.OmnilockSigner{})

	ckbClient, err := client.NewClient(rpcClient, evmSigner, d)
	require.NoError(t, err)

	factory := multiLedgerAssetFactory(asset1, asset2)

	coord1 := coordinator.NewCoordinatorWithAssetFactory(ckbClient, factory)

	multiCoord := multi.NewCoordinator()
	multiCoord.RegisterCoordinator(l1ID, coord1)
	multiCoord.RegisterCoordinator(l2ID, noopCoordinator{})

	part := address.AsParticipant(coordAcc.Address())
	wireAcc := gpwiretest.NewRandomAccount(rng)
	wireAddr := wireAcc.Address()

	return ctest.MultiLedgerCoordinator{
		WireAddress:      map[gpwallet.BackendID]wire.Address{gpwallet.BackendID(asset.CKBBackendID): wireAddr},
		WalletAddress:    map[gpwallet.BackendID]gpwallet.Address{gpwallet.BackendID(asset.CKBBackendID): part},
		WalletAccount:    map[gpwallet.BackendID]gpwallet.Account{gpwallet.BackendID(asset.CKBBackendID): coordAcc},
		Multicoordinator: multiCoord,
		Coordinator1:     coord1,
		Coordinator2:     noopCoordinator{},
	}
}

// SetupMultiLedgerTest creates a multi-ledger test setup against the local CKB
// devnet, suitable for go-perun's shared harnesses:
// ctest.TestMultiLedgerCoordinate, TestMultiLedgerAttackNoCoordinator,
// TestMultiLedgerAttackCoordinate.
//
// Two logical ledgers are derived from one devnet:
//   - Asset1: CKByte (NervosAsset, backend 3, ledger "03") — real on-chain.
//   - Asset2: synthetic EthAsset (backend 1, chainID 1) — encoded in the CKB
//     cell as an ETHBalances row; its presence makes is_multi_ledger_state=true
//     so the contract's coordinator gate engages.
//
// Because Asset2 has no real Ethereum chain, BalanceReader2 always returns 0.
// InitBalances and both UpdateBalances keep the Asset2 row at {0,0}, so the
// balance-delta assertions in the harness pass trivially for that row.
func SetupMultiLedgerTest(t *testing.T, testDuration time.Duration) ctest.MultiLedgerSetup {
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

	// Load account keys (Alice, Bob, Charlie=ingrid).
	keyAlice, err := chtest.GetKey(devnetDir + "/accounts/alice.pk")
	require.NoError(t, err)
	keyBob, err := chtest.GetKey(devnetDir + "/accounts/bob.pk")
	require.NoError(t, err)
	keyCharlie, err := chtest.GetKey(devnetDir + "/accounts/ingrid.pk")
	require.NoError(t, err)

	// Derive omni-lock participants and EVM addresses.
	_, evmAlice, err := address.NewEthereumParticipantFromPublicKey(keyAlice.PubKey(), d.OmniLockScript.CodeHash)
	require.NoError(t, err)
	_, evmBob, err := address.NewEthereumParticipantFromPublicKey(keyBob.PubKey(), d.OmniLockScript.CodeHash)
	require.NoError(t, err)
	_, evmCharlie, err := address.NewEthereumParticipantFromPublicKey(keyCharlie.PubKey(), d.OmniLockScript.CodeHash)
	require.NoError(t, err)

	accAlice := wallet.NewAccountFromPrivateKey(keyAlice, d.OmniLockScript.CodeHash, false)
	accBob := wallet.NewAccountFromPrivateKey(keyBob, d.OmniLockScript.CodeHash, false)
	accCharlie := wallet.NewAccountFromPrivateKey(keyCharlie, d.OmniLockScript.CodeHash, false)

	// Two logical ledger IDs.
	l1ID := asset.MakeCCID(asset.MakeContractID(ckbLedgerContractID)) // CKByte, backend 3
	l2ID := asset.MakeLedgerBackendID(big.NewInt(ethFakeChainID))     // ETH (synthetic), backend 1

	// Assets.
	asset1 := asset.NewCKBytesNervosAsset()
	ethZeroAddr := asset.EthAddress{}
	asset2Raw := asset.MakeEthAsset(big.NewInt(ethFakeChainID), &ethZeroAddr)
	asset2 := &asset2Raw

	bus := wire.NewLocalBus()

	c1 := mlParticipant(t, rng, keyAlice, accAlice, evmAlice, d, asset1, asset2, l1ID, l2ID, bus)
	c2 := mlParticipant(t, rng, keyBob, accBob, evmBob, d, asset1, asset2, l1ID, l2ID, bus)
	// Charlie signs the canonical state (coord_sig); Alice (a participant) is the
	// CKB submitter that unlocks the channel cell for the coordinate tx (PCLS
	// requires a participant input — see mlCoordinator).
	charlie := mlCoordinator(t, rng, accCharlie, keyAlice, accAlice, evmAlice, d, asset1, asset2, l1ID, l2ID)
	_ = evmCharlie

	// The PCLS (funds lock) cell requires ~73 CKByte of occupied capacity to
	// hold the omni-lock args; the existing single-asset tests use 100 CKB.
	ckbAmt := asset.CKByteToShannon(big.NewFloat(100))
	delta10 := asset.CKByteToShannon(big.NewFloat(10))
	delta15 := asset.CKByteToShannon(big.NewFloat(15))
	zero := big.NewInt(0)

	return ctest.MultiLedgerSetup{
		Client1:     c1,
		Client2:     c2,
		Coordinator: charlie,
		Asset1:      asset1,
		Asset2:      asset2,
		InitBalances: channel.Balances{
			{ckbAmt, ckbAmt}, // CKByte row — each party starts with 100 CKB
			{zero, zero},     // ETH row — synthetic; kept zero throughout
		},
		UpdateBalances1: channel.Balances{
			{new(big.Int).Add(ckbAmt, delta10), new(big.Int).Sub(ckbAmt, delta10)},
			{zero, zero},
		},
		UpdateBalances2: channel.Balances{
			{new(big.Int).Sub(ckbAmt, delta15), new(big.Int).Add(ckbAmt, delta15)},
			{zero, zero},
		},
		// Allow 3 CKB fee tolerance per transaction.
		BalanceDelta: asset.CKByteToShannon(big.NewFloat(3)),
	}
}
