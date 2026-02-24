"""Public interface for IBKR provider.

Most external code should import from this module.

Agent orientation:
    Compatibility boundary for callers that should not depend on IBKR internals.
    This module delegates to ``ibkr.market_data`` and ``ibkr.flex`` while
    normalizing failure behavior for higher layers.
"""

from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Union

import pandas as pd
import yaml

from .exceptions import (
    IBKRAccountError,
    IBKRConnectionError,
    IBKRContractError,
    IBKRDataError,
    IBKREntitlementError,
    IBKRNoDataError,
    IBKRTimeoutError,
)
from ._logging import portfolio_logger

IBKRMarketDataClient = None


@lru_cache(maxsize=1)
def _load_ibkr_exchange_mappings() -> dict[str, Any]:
    path = Path(__file__).resolve().with_name("exchange_mappings.yaml")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_ibkr_futures_fmp_map() -> dict[str, str]:
    """Load IBKR futures-root -> FMP symbol mappings."""
    raw_map = _load_ibkr_exchange_mappings().get("ibkr_futures_to_fmp", {})
    out: dict[str, str] = {}
    if isinstance(raw_map, dict):
        for symbol, mapped in raw_map.items():
            key = str(symbol or "").strip().upper()
            val = str(mapped or "").strip().upper()
            if key and val:
                out[key] = val
    return out


def get_ibkr_futures_exchanges() -> dict[str, dict[str, str]]:
    """Load IBKR futures-root exchange metadata."""
    raw_map = _load_ibkr_exchange_mappings().get("ibkr_futures_exchanges", {})
    out: dict[str, dict[str, str]] = {}
    if isinstance(raw_map, dict):
        for symbol, meta in raw_map.items():
            if not isinstance(meta, dict):
                continue
            key = str(symbol or "").strip().upper()
            exchange = str(meta.get("exchange") or "").strip().upper()
            currency = str(meta.get("currency") or "USD").strip().upper()
            if key and exchange:
                out[key] = {"exchange": exchange, "currency": currency}
    return out


def get_futures_currency(symbol: str) -> str:
    """Return settlement currency for a futures root symbol."""
    key = str(symbol or "").strip().upper()
    if not key:
        return "USD"
    return get_ibkr_futures_exchanges().get(key, {}).get("currency", "USD")


def fetch_ibkr_monthly_close(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    currency: str = "USD",
) -> pd.Series:
    """Fetch month-end futures close series through the IBKR market data client.

    Upstream references:
    - IBKR historical bars: https://interactivebrokers.github.io/tws-api/historical_bars.html
    """
    del currency
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_monthly_close_futures(symbol, start_date, end_date)
    except Exception as exc:
        portfolio_logger.warning("IBKR futures fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def fetch_ibkr_daily_close_futures(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Fetch daily futures close series through the IBKR market data client."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_daily_close_futures(symbol, start_date, end_date)
    except Exception as exc:
        portfolio_logger.warning("IBKR daily futures fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def fetch_ibkr_fx_monthly_close(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Fetch month-end FX close series through the IBKR market data client."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_monthly_close_fx(symbol, start_date, end_date)
    except Exception as exc:
        portfolio_logger.warning("IBKR FX fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def fetch_ibkr_bond_monthly_close(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    contract_identity: dict | None = None,
) -> pd.Series:
    """Fetch month-end bond close series through the IBKR market data client."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_monthly_close_bond(
            symbol,
            start_date,
            end_date,
            contract_identity=contract_identity,
        )
    except Exception as exc:
        portfolio_logger.warning("IBKR bond fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def fetch_ibkr_option_monthly_mark(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    contract_identity: dict | None = None,
) -> pd.Series:
    """Fetch month-end option marks through the IBKR market data client."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_monthly_close_option(
            symbol,
            start_date,
            end_date,
            contract_identity=contract_identity,
        )
    except Exception as exc:
        portfolio_logger.warning("IBKR option fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def fetch_ibkr_flex_trades(
    token: str = "",
    query_id: str = "",
    path: str | None = None,
):
    """Fetch and normalize IBKR Flex trades.

    Upstream reference:
    - IBKR Flex Web Service: https://www.interactivebrokers.com/en/software/am-api/am/flex-web-service.htm
    """
    from .flex import fetch_ibkr_flex_trades as _fetch_ibkr_flex_trades

    return _fetch_ibkr_flex_trades(token=token, query_id=query_id, path=path)


def fetch_ibkr_flex_payload(
    token: str = "",
    query_id: str = "",
    path: str | None = None,
) -> dict[str, Any]:
    """Fetch IBKR Flex trades/cash rows with fetch diagnostics."""
    from .flex import fetch_ibkr_flex_payload as _fetch_ibkr_flex_payload

    return _fetch_ibkr_flex_payload(token=token, query_id=query_id, path=path)


def __getattr__(name: str):
    if name == "IBKRClient":
        from .client import IBKRClient as _IBKRClient

        return _IBKRClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "IBKRClient",
    "fetch_ibkr_monthly_close",
    "fetch_ibkr_daily_close_futures",
    "fetch_ibkr_fx_monthly_close",
    "fetch_ibkr_bond_monthly_close",
    "fetch_ibkr_option_monthly_mark",
    "fetch_ibkr_flex_trades",
    "fetch_ibkr_flex_payload",
    "get_ibkr_futures_fmp_map",
    "get_ibkr_futures_exchanges",
    "get_futures_currency",
    "IBKRDataError",
    "IBKRConnectionError",
    "IBKRContractError",
    "IBKRNoDataError",
    "IBKREntitlementError",
    "IBKRAccountError",
    "IBKRTimeoutError",
]
