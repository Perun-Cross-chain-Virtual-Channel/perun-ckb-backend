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
	"math/big"

	"github.com/nervosnetwork/ckb-sdk-go/v2/types/molecule"
	pchannel "perun.network/go-perun/channel"
	gpwallet "perun.network/go-perun/wallet"
	"perun.network/perun-ckb-backend/channel/asset"
	molecule2 "perun.network/perun-ckb-backend/encoding/molecule"
)

// AssetDescriptor is the chain-agnostic description of a single channel asset as
// recovered from the on-chain molecule encoding. Exactly one of its arms is set.
// It is the input to an AssetFactory, which maps it back to the concrete perun
// channel.Asset Go object the caller expects (e.g. a multi-ledger wrapper).
type AssetDescriptor struct {
	// IsCKByte is true for the native CKByte asset.
	IsCKByte bool
	// SUDT is non-nil for an SUDT asset (mutually exclusive with the others).
	SUDT *asset.SUDT
	// ETHChainID / ETHAddress describe an Ethereum-ledger asset stored in the
	// CKB channel cell (an ETHBalances entry). ETHChainID is nil otherwise.
	ETHChainID *big.Int
	ETHAddress [20]byte
}

// AssetFactory maps an AssetDescriptor recovered from on-chain data back to the
// concrete perun channel.Asset (and the wallet backend that asset lives on).
// The multi-ledger harness supplies a factory that wraps assets so their
// LedgerBackendID matches the originals; passing nil uses DefaultAssetFactory.
type AssetFactory func(AssetDescriptor) (pchannel.Asset, gpwallet.BackendID, error)

// DefaultAssetFactory reconstructs raw CKB-backend asset objects. CKByte and
// SUDT assets become *asset.NervosAsset on the CKB backend; ETH-ledger assets
// become *asset.EthAsset on the eth backend. This is sufficient for
// single-chain CKB channels; cross-chain harnesses override it to match the
// exact asset objects used at channel open.
func DefaultAssetFactory(d AssetDescriptor) (pchannel.Asset, gpwallet.BackendID, error) {
	switch {
	case d.IsCKByte:
		return asset.NewCKBytesNervosAsset(), gpwallet.BackendID(asset.CKBBackendID), nil
	case d.SUDT != nil:
		ccid := asset.MakeCCID(asset.MakeContractID("03"))
		na := asset.NewNervosAsset(*asset.NewSUDTAsset(d.SUDT), ccid)
		return &na, gpwallet.BackendID(asset.CKBBackendID), nil
	case d.ETHChainID != nil:
		ethAddr := asset.EthAddress{}
		if err := (&ethAddr).UnmarshalBinary(d.ETHAddress[:]); err != nil {
			return nil, 0, fmt.Errorf("decoding eth asset address: %w", err)
		}
		ea := asset.MakeEthAsset(new(big.Int).Set(d.ETHChainID), &ethAddr)
		return &ea, gpwallet.BackendID(asset.EthBackendID), nil
	default:
		return nil, 0, fmt.Errorf("empty asset descriptor")
	}
}

// UnpackChannelState reverses PackChannelState: it reconstructs a perun
// channel.State from the on-chain molecule encoding. Because the molecule
// ChannelState stores only ID, version, isFinal and balances (no app/data),
// the result carries channel.NoApp/NoData — correct for CKB payment channels.
//
// factory maps each recovered asset back to a concrete channel.Asset; pass nil
// to use DefaultAssetFactory. The asset order, balance indexing and locked
// sub-allocations mirror PackBalances exactly.
func UnpackChannelState(s *molecule.ChannelState, factory AssetFactory) (*pchannel.State, error) {
	if factory == nil {
		factory = DefaultAssetFactory
	}

	id, err := molecule2.UnpackByte32(s.ChannelId())
	if err != nil {
		return nil, fmt.Errorf("unpacking channel id: %w", err)
	}

	assets, backends, balances, err := unpackAllocation(s.Balances().Assets(), factory)
	if err != nil {
		return nil, err
	}

	locked, err := unpackLocked(s.Balances().Locked(), len(assets))
	if err != nil {
		return nil, err
	}

	return &pchannel.State{
		ID:      id,
		Version: molecule2.UnpackUint64(s.Version()),
		App:     pchannel.NoApp(),
		Data:    pchannel.NoData(),
		Allocation: pchannel.Allocation{
			Assets:   assets,
			Backends: backends,
			Balances: balances,
			Locked:   locked,
		},
		IsFinal: ToBool(*s.IsFinal()),
	}, nil
}

// unpackAllocation reconstructs the per-asset assets, backends and the
// [asset][participant] balance matrix from the molecule Allocation.
func unpackAllocation(alloc *molecule.Allocation, factory AssetFactory) ([]pchannel.Asset, []gpwallet.BackendID, pchannel.Balances, error) {
	n := int(alloc.ItemCount())
	assets := make([]pchannel.Asset, n)
	backends := make([]gpwallet.BackendID, n)
	balances := make(pchannel.Balances, n)

	for i := 0; i < n; i++ {
		union := alloc.Get(uint(i)).ToUnion()
		desc, bals, err := unpackAnyBalances(union)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("unpacking asset %d: %w", i, err)
		}
		a, backend, err := factory(desc)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("reconstructing asset %d: %w", i, err)
		}
		assets[i] = a
		backends[i] = backend
		balances[i] = bals
	}
	return assets, backends, balances, nil
}

// unpackAnyBalances decodes one AnyBalances entry into its asset descriptor and
// the two participant balances. The two-element balance slice mirrors the
// [partyA, partyB] distribution PackBalances wrote.
func unpackAnyBalances(union *molecule.AnyBalancesUnion) (AssetDescriptor, []pchannel.Bal, error) {
	switch union.ItemID() {
	case 0: // CKByteDistribution
		d := union.IntoCKByteDistribution()
		bals := []pchannel.Bal{
			new(big.Int).SetUint64(molecule2.UnpackUint64(d.Nth0())),
			new(big.Int).SetUint64(molecule2.UnpackUint64(d.Nth1())),
		}
		return AssetDescriptor{IsCKByte: true}, bals, nil
	case 1: // SUDTBalances
		b := union.IntoSUDTBalances()
		sudt := &asset.SUDT{}
		sudt.Unpack(b.Asset())
		dist := b.Distribution()
		bals := []pchannel.Bal{
			molecule2.UnpackUint128(dist.Nth0()).Big(),
			molecule2.UnpackUint128(dist.Nth1()).Big(),
		}
		return AssetDescriptor{SUDT: sudt}, bals, nil
	case 2: // ETHBalances
		b := union.IntoETHBalances()
		chainID := molecule2.UnpackUint128(b.Asset().ChainId()).Big()
		var addr [20]byte
		copy(addr[:], b.Asset().AssetAddress().RawData())
		dist := b.Distribution()
		bals := []pchannel.Bal{
			molecule2.UnpackUint128(dist.Nth0()).Big(),
			molecule2.UnpackUint128(dist.Nth1()).Big(),
		}
		return AssetDescriptor{ETHChainID: chainID, ETHAddress: addr}, bals, nil
	default:
		return AssetDescriptor{}, nil, fmt.Errorf("unknown AnyBalances item id %d", union.ItemID())
	}
}

// unpackLocked reconstructs the locked sub-allocations. Each SubAlloc's
// SubBalances is a flat per-asset total (one Uint128 per channel asset),
// matching PackSubAlloc; per-participant attribution is carried by IndexMap.
func unpackLocked(locked *molecule.LockedBalances, numAssets int) ([]pchannel.SubAlloc, error) {
	n := int(locked.ItemCount())
	if n == 0 {
		return nil, nil
	}
	out := make([]pchannel.SubAlloc, 0, n)
	for i := 0; i < n; i++ {
		sa := locked.Get(uint(i))
		id, err := molecule2.UnpackByte32(sa.Id())
		if err != nil {
			return nil, fmt.Errorf("unpacking locked suballoc %d id: %w", i, err)
		}
		subBals := sa.Balances()
		if int(subBals.ItemCount()) != numAssets {
			return nil, fmt.Errorf("locked suballoc %d has %d balances but state has %d assets", i, subBals.ItemCount(), numAssets)
		}
		bals := make([]pchannel.Bal, numAssets)
		for j := 0; j < numAssets; j++ {
			bals[j] = molecule2.UnpackUint128(subBals.Get(uint(j))).Big()
		}
		idxMap := sa.IdxMap()
		indexMap := []pchannel.Index{
			pchannel.Index(idxMap.Nth0()[0]),
			pchannel.Index(idxMap.Nth1()[0]),
		}
		out = append(out, *pchannel.NewSubAlloc(id, bals, indexMap))
	}
	return out, nil
}
