# ibkr — IBKR Provider Package

Self-contained package for Interactive Brokers Gateway integration. Provides an MCP server (ibkr-mcp), a Python client facade (`IBKRClient`), and low-level modules for market data, account operations, and metadata queries.

## Architecture

### Connection Model

TWS/IB Gateway listens on a single port (default 7496) and multiplexes API connections by **client ID**. Each client ID can only be used by one process at a time — TWS rejects duplicates with `Error 326: client id already in use`.

Three client IDs are used, each serving a different purpose:

| Client ID | Default | Env Var | Module | Lifecycle | Purpose |
|-----------|---------|---------|--------|-----------|---------|
| 20 | `IBKR_CLIENT_ID` | `IBKR_CLIENT_ID` | `connection.py` (`IBKRConnectionManager`) | **Mode-dependent** via `IBKR_CONNECTION_MODE` (default ephemeral) | Account data, positions, PnL, contract details, option chains |
| 21 | `IBKR_CLIENT_ID + 1` | (derived) | `market_data.py` (`IBKRMarketDataClient`) | **Ephemeral** — fresh connection per request, disconnects after | Price snapshots, historical bars, option Greeks |
| 22 | `IBKR_CLIENT_ID + 2` | `IBKR_TRADE_CLIENT_ID` | `brokerage/ibkr/adapter.py` | **Separate** non-singleton `IBKRConnectionManager(client_id=22)` | Trade execution (preview/execute) |

**Why separate client IDs?** TWS allows only one connection per client ID. The ibkr-mcp server account path (client 20) runs in ephemeral mode by default (or persistent when configured), while market data (client 21) always uses short-lived connections. The trading adapter (client 22) is isolated so trade execution never collides with read-only operations.

### Connection Routing in IBKRClient

`IBKRClient` (in `client.py`) is the main facade. It routes calls through two paths:

```
IBKRClient
├── Account/metadata calls → IBKRConnectionManager (client 20, mode from `IBKR_CONNECTION_MODE`)
│   ├── get_positions()
│   ├── get_account_summary()
│   ├── get_pnl()
│   ├── get_contract_details()
│   └── get_option_chain()
│
└── Market data calls → IBKRMarketDataClient (client 21, ephemeral)
    ├── fetch_snapshot()      — real-time quotes + option Greeks
    ├── fetch_series()        — historical daily bars
    └── fetch_futures_curve() — term structure snapshots
```

### Locking

- `ibkr_shared_lock` (in `locks.py`) — Global lock for account/metadata calls. In persistent mode it serializes shared-connection access; in ephemeral mode it prevents overlapping connections on the same client ID.
- `_ibkr_request_lock` (in `market_data.py`) — Module-level lock for historical bar requests only. Does NOT wrap snapshot requests.

### Option Snapshot Behavior

IBKR's `reqMktData(snapshot=True)` does **not** work for option contracts (returns `nan` for all fields). The market data client detects option contracts in a batch and automatically switches to **streaming mode**:

1. Calls `reqMktData(snapshot=False)` to start a subscription
2. Polls in `IBKR_SNAPSHOT_POLL_INTERVAL` intervals for bid/ask/last data
3. For OPT contracts, also waits for `modelGreeks` (IBKR computes server-side, takes ~1s)
4. Cancels all subscriptions once data arrives or timeout is reached

## Tools (MCP Server)

| Tool | Description |
|------|-------------|
| `get_ibkr_market_data` | Historical OHLCV bars for any contract |
| `get_ibkr_positions` | Current portfolio positions and P&L |
| `get_ibkr_account` | Account summary (balances, margin, NAV) |
| `get_ibkr_contract` | Contract lookup and option chain metadata |
| `get_ibkr_option_prices` | Option strike pricing with Greeks |
| `get_ibkr_snapshot` | Real-time quote snapshot |
| `get_ibkr_status` | Connection diagnostics (connected, client ID, config) |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `IBKR_GATEWAY_HOST` | `127.0.0.1` | TWS/Gateway hostname |
| `IBKR_GATEWAY_PORT` | `7496` | TWS/Gateway API port |
| `IBKR_CLIENT_ID` | `1` | Base client ID (account/metadata connection) |
| `IBKR_TRADE_CLIENT_ID` | `IBKR_CLIENT_ID + 2` | Trading adapter client ID |
| `IBKR_CONNECTION_MODE` | `ephemeral` | Account/metadata mode: `ephemeral` or `persistent` |
| `IBKR_TIMEOUT` | `10` | Connection timeout (seconds) |
| `IBKR_READONLY` | `false` | Read-only mode for persistent connection |
| `IBKR_AUTHORIZED_ACCOUNTS` | | Comma-separated account whitelist |
| `IBKR_CONNECT_MAX_ATTEMPTS` | `3` | Max connection retry attempts |
| `IBKR_RECONNECT_DELAY` | `5` | Base delay between reconnect attempts (seconds) |
| `IBKR_MAX_RECONNECT_ATTEMPTS` | `3` | Max background reconnect attempts |
| `IBKR_OPTION_SNAPSHOT_TIMEOUT` | `15` | Timeout for option snapshots (seconds) |
| `IBKR_SNAPSHOT_TIMEOUT` | `5.0` | Timeout for stock snapshots (seconds) |
| `IBKR_SNAPSHOT_POLL_INTERVAL` | `0.5` | Poll interval for streaming snapshots (seconds) |
| `IBKR_MARKET_DATA_RETRY_DELAY` | `2.0` | Delay between bar request retries (seconds) |
| `IBKR_FUTURES_CURVE_TIMEOUT` | `8.0` | Timeout for futures curve snapshots (seconds) |
| `IBKR_PNL_TIMEOUT` | `5.0` | Timeout for PnL subscription data (seconds) |
| `IBKR_PNL_POLL_INTERVAL` | `0.1` | Poll interval for PnL data (seconds) |

### Connection Mode (`IBKR_CONNECTION_MODE`)

- `ephemeral` (default): Account/metadata calls create a fresh client 20 connection per request and disconnect on exit. This minimizes stale client ID collisions across multiple processes.
- `persistent`: Restores singleton behavior for client 20 (`ensure_connected()`), keeping a long-lived connection between requests.
- Invalid values are normalized to `ephemeral`.

All config is in `config.py` with env var overrides. The package auto-loads `.env` from the package directory and parent.

## Troubleshooting

### "client id already in use" / TimeoutError on persistent connection

TWS only allows one connection per client ID. Common causes:
- **Stale MCP processes**: Multiple Claude sessions spawn separate ibkr-mcp processes, all trying client ID 20. Check with `ps aux | grep ibkr` and kill stale ones.
- **TWS holds dead connections**: If a process is `kill`'d without disconnecting cleanly, TWS keeps the client ID reserved. Fix: toggle API off/on in TWS settings (Edit → Global Configuration → API → Settings → uncheck/recheck "Enable ActiveX and Socket Clients"), or restart TWS.
- **Diagnosis**: Use `get_ibkr_status` tool — in `persistent` mode, `connected: false` with working snapshots often means client 20 is blocked while client 21 is fine.

### Market data works but contract/account queries fail

Market data uses ephemeral connections (client 21) — fresh each request. Account/contract queries use client 20 with mode controlled by `IBKR_CONNECTION_MODE` (ephemeral by default, persistent when configured). In persistent mode, if client 20 is stuck, market data still works.

### Option snapshots return null Greeks

Greeks (`modelGreeks`) take ~1s to arrive from IBKR (computed server-side). The `IBKR_OPTION_SNAPSHOT_TIMEOUT` (default 15s) controls how long to wait. If Greeks are still null, check that you have market data subscriptions for the underlying.

## Module Map

| File | Role |
|------|------|
| `config.py` | All env var constants (connection, timeout, retry) |
| `connection.py` | `IBKRConnectionManager` — mode-aware connection manager (ephemeral default, persistent optional) |
| `market_data.py` | `IBKRMarketDataClient` — ephemeral per-request connections |
| `client.py` | `IBKRClient` — unified facade routing to connection/market_data |
| `account.py` | Account summary, positions, PnL fetchers |
| `metadata.py` | Contract details, option chain, bond CUSIP resolution |
| `server.py` | MCP server (FastMCP tool registration) |
| `_logging.py` | `log_event()` structured logging, `TimingContext` |
| `locks.py` | `ibkr_shared_lock` global lock |
| `flex.py` | Flex Query download + trade normalization |
| `compat.py` | Public interface for external callers |
| `contracts.py` | Contract construction helpers |
| `exceptions.py` | Package-specific exception types |
| `exchange_mappings.yaml` | IBKR exchange/futures mappings |

## Usage

### Claude Code

```bash
claude mcp add ibkr-mcp -- python -m ibkr.server
```

### Standalone

```bash
python -m ibkr.server
```

## License

MIT
