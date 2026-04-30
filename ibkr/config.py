"""IBKR package-local configuration loaded from environment variables."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    _pkg_dir = Path(__file__).resolve().parent
    load_dotenv(_pkg_dir.parent / ".env", override=False)
except Exception:
    # Keep imports resilient when python-dotenv is unavailable.
    pass


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    if value != value or value == float("inf") or value == float("-inf"):
        return default
    return value


IBKR_GATEWAY_HOST: str = os.getenv("IBKR_GATEWAY_HOST", "127.0.0.1")
IBKR_GATEWAY_PORT: int = _int_env("IBKR_GATEWAY_PORT", 7496)
IBKR_CLIENT_ID: int = _int_env("IBKR_CLIENT_ID", 1)
# Separate client ID for the trading adapter connection so it doesn't collide
# with ibkr-mcp (IBKR_CLIENT_ID) or market data (IBKR_CLIENT_ID + 1).
IBKR_TRADE_CLIENT_ID: int = _int_env("IBKR_TRADE_CLIENT_ID", IBKR_CLIENT_ID + 2)
IBKR_TIMEOUT: int = _int_env("IBKR_TIMEOUT", 10)
# Timeout for individual IB API requests (e.g. reqCompletedOrders).
# ib_async defaults to 0 (infinite) — this prevents indefinite hangs.
IBKR_REQUEST_TIMEOUT: int = _int_env("IBKR_REQUEST_TIMEOUT", 30)
IBKR_READONLY: bool = os.getenv("IBKR_READONLY", "false").lower() == "true"
IBKR_AUTHORIZED_ACCOUNTS: list[str] = [
    account.strip()
    for account in os.getenv("IBKR_AUTHORIZED_ACCOUNTS", "").split(",")
    if account.strip()
]

# Option snapshots need more time — IBKR computes model Greeks server-side.
IBKR_OPTION_SNAPSHOT_TIMEOUT: int = _int_env("IBKR_OPTION_SNAPSHOT_TIMEOUT", 15)

# Max attempts for initial connection (used by IBKRConnectionManager and market data retry).
IBKR_CONNECT_MAX_ATTEMPTS: int = _int_env("IBKR_CONNECT_MAX_ATTEMPTS", 3)

# --- Connection retry ---
IBKR_RECONNECT_DELAY: int = _int_env("IBKR_RECONNECT_DELAY", 5)
IBKR_MAX_RECONNECT_ATTEMPTS: int = _int_env("IBKR_MAX_RECONNECT_ATTEMPTS", 3)

# --- Connection mode ---
_raw_mode = os.getenv("IBKR_CONNECTION_MODE", "ephemeral").lower()
IBKR_CONNECTION_MODE: str = _raw_mode if _raw_mode in ("ephemeral", "persistent") else "ephemeral"

# --- Market data ---
IBKR_MARKET_DATA_RETRY_DELAY: float = _float_env("IBKR_MARKET_DATA_RETRY_DELAY", 2.0)
IBKR_SNAPSHOT_TIMEOUT: float = _float_env("IBKR_SNAPSHOT_TIMEOUT", 5.0)
IBKR_SNAPSHOT_POLL_INTERVAL: float = _float_env("IBKR_SNAPSHOT_POLL_INTERVAL", 0.5)
IBKR_FUTURES_CURVE_TIMEOUT: float = _float_env("IBKR_FUTURES_CURVE_TIMEOUT", 8.0)

# --- Account PnL ---
IBKR_PNL_TIMEOUT: float = _float_env("IBKR_PNL_TIMEOUT", 5.0)
IBKR_PNL_POLL_INTERVAL: float = _float_env("IBKR_PNL_POLL_INTERVAL", 0.1)
