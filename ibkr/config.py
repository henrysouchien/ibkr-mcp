"""IBKR package-local configuration loaded from environment variables."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    _pkg_dir = Path(__file__).resolve().parent
    load_dotenv(_pkg_dir / ".env", override=False)
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


IBKR_GATEWAY_HOST: str = os.getenv("IBKR_GATEWAY_HOST", "127.0.0.1")
IBKR_GATEWAY_PORT: int = _int_env("IBKR_GATEWAY_PORT", 7496)
IBKR_CLIENT_ID: int = _int_env("IBKR_CLIENT_ID", 1)
IBKR_TIMEOUT: int = _int_env("IBKR_TIMEOUT", 10)
IBKR_READONLY: bool = os.getenv("IBKR_READONLY", "false").lower() == "true"
IBKR_AUTHORIZED_ACCOUNTS: list[str] = [
    account.strip()
    for account in os.getenv("IBKR_AUTHORIZED_ACCOUNTS", "").split(",")
    if account.strip()
]
