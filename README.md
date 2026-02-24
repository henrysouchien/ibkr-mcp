# ibkr-mcp

MCP server for Interactive Brokers Gateway — 6 tools for market data and account access via Claude.

## Tools

| Tool | Description |
|------|-------------|
| `get_ibkr_market_data` | Historical OHLCV bars for any contract |
| `get_ibkr_positions` | Current portfolio positions and P&L |
| `get_ibkr_account` | Account summary (balances, margin, NAV) |
| `get_ibkr_contract` | Contract lookup and details |
| `get_ibkr_option_prices` | Option chain pricing |
| `get_ibkr_snapshot` | Real-time quote snapshot |

## Install

```bash
pip install interactive-brokers-mcp
```

## Prerequisites

- [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php) or TWS running
- API access enabled in Gateway/TWS settings
- Host/port/client ID matching your environment variables

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `IBKR_GATEWAY_HOST` | `127.0.0.1` | Gateway hostname |
| `IBKR_GATEWAY_PORT` | `7496` | Gateway port |
| `IBKR_CLIENT_ID` | `1` | API client ID |
| `IBKR_TIMEOUT` | `10` | Connection timeout (seconds) |
| `IBKR_READONLY` | `false` | Read-only mode |
| `IBKR_AUTHORIZED_ACCOUNTS` | | Comma-separated account whitelist |
| `IBKR_CACHE_DIR` | | Optional cache directory override |

The package auto-loads `.env` from the package directory and parent.

## Usage

### Claude Code

```bash
claude mcp add ibkr-mcp -- ibkr-mcp
```

Or in `~/.claude.json`:

```json
{
  "mcpServers": {
    "ibkr-mcp": {
      "type": "stdio",
      "command": "ibkr-mcp"
    }
  }
}
```

### Standalone

```bash
ibkr-mcp          # via installed entry point
python -m ibkr.server   # via module
```

## License

MIT
