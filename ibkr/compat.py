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
import os
from pathlib import Path
from typing import Any, Optional, Union

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
from ._logging import logger
from .config import IBKR_FUTURES_CURVE_TIMEOUT

IBKRMarketDataClient = None


def _ibkr_fx_daily_enabled():
    return os.getenv("IBKR_FX_DAILY_ENABLED", "0").strip() == "1"


def _ibkr_bond_daily_enabled():
    return os.getenv("IBKR_BOND_DAILY_ENABLED", "0").strip() == "1"


def _ibkr_ts_cache_enabled():
    return os.getenv("IBKR_TIMESERIES_CACHE_ENABLED", "1").strip() == "1"


@lru_cache(maxsize=1)
def _load_ibkr_exchange_mappings() -> dict[str, Any]:
    path = Path(__file__).resolve().with_name("exchange_mappings.yaml")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_ibkr_futures_fmp_map() -> dict[str, str]:
    """Return IBKR-routable futures-root -> FMP symbol mappings."""
    from brokerage.futures import load_contract_specs

    ibkr_routing = get_ibkr_futures_exchanges()
    all_specs = load_contract_specs()

    out: dict[str, str] = {}
    for symbol, spec in all_specs.items():
        if symbol not in ibkr_routing:
            continue
        mapped = str(spec.data_symbol or "").strip().upper()
        if mapped:
            out[symbol] = mapped
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


def get_ibkr_futures_contract_meta() -> dict[str, dict[str, Any]]:
    """Return IBKR-routable futures metadata from the canonical futures catalog."""
    from brokerage.futures import load_contract_specs

    ibkr_routing = get_ibkr_futures_exchanges()
    all_specs = load_contract_specs()

    out: dict[str, dict[str, Any]] = {}
    for symbol, spec in all_specs.items():
        if symbol not in ibkr_routing:
            continue

        identity = spec.to_contract_identity()
        # IBKR routing values are authoritative inside IBKR contexts.
        identity["exchange"] = ibkr_routing[symbol]["exchange"]
        identity["currency"] = ibkr_routing[symbol]["currency"]
        out[symbol] = identity
    return out


def get_futures_contract_meta(symbol: str) -> Optional[dict[str, Any]]:
    """Return full contract metadata for an IBKR futures root symbol."""
    return get_ibkr_futures_contract_meta().get(str(symbol or "").strip().upper())


def get_futures_currency(symbol: str) -> str:
    """Return settlement currency for a futures root symbol."""
    key = str(symbol or "").strip().upper()
    if not key:
        return "USD"
    from brokerage.futures import get_contract_spec

    spec = get_contract_spec(key)
    return spec.currency if spec else "USD"


def get_futures_months(symbol: str) -> list[dict[str, Any]]:
    """Discover available contract months for a futures root symbol."""
    from .client import IBKRClient

    client = IBKRClient()
    return client.get_futures_months(symbol)


def get_futures_curve_snapshot(
    symbol: str,
    timeout: float = IBKR_FUTURES_CURVE_TIMEOUT,
) -> list[dict[str, Any]]:
    """Snapshot prices for all active contract months of a futures root symbol."""
    from .client import IBKRClient

    client = IBKRClient()
    return client.get_futures_curve_snapshot(symbol, timeout=timeout)


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
        logger.warning("IBKR futures fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_futures(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Raw IBKR daily futures fetch (fail-open, existing contract)."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_daily_close_futures(symbol, start_date, end_date)
    except Exception as exc:
        logger.warning("IBKR daily futures fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_futures_for_cache(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Raw fetch for cache loader with transient failures propagated."""
    client_cls = IBKRMarketDataClient
    if client_cls is None:
        from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

        client_cls = _IBKRMarketDataClient
    client = client_cls()
    return client.fetch_daily_close_futures(
        symbol,
        start_date,
        end_date,
        raise_on_transient=True,
    )


def fetch_ibkr_daily_close_futures(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Public wrapper for daily futures close with optional cache."""
    if _ibkr_ts_cache_enabled():
        try:
            from .timeseries_cache import cached_daily_fetch

            return cached_daily_fetch(
                symbol,
                start_date,
                end_date,
                instrument_type="futures",
                raw_fetcher=_raw_daily_futures_for_cache,
            )
        except Exception as exc:
            logger.warning("IBKR cache-backed daily futures failed for %s, falling back: %s", symbol, exc)
            return _raw_daily_futures(symbol, start_date, end_date)
    return _raw_daily_futures(symbol, start_date, end_date)


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
        logger.warning("IBKR FX fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_fx(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Raw IBKR daily FX fetch (fail-open, existing contract)."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_daily_close_fx(symbol, start_date, end_date)
    except Exception as exc:
        logger.warning("IBKR daily FX fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_fx_for_cache(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Raw IBKR daily FX fetch with transient failures propagated."""
    client_cls = IBKRMarketDataClient
    if client_cls is None:
        from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

        client_cls = _IBKRMarketDataClient
    client = client_cls()
    return client.fetch_daily_close_fx(
        symbol,
        start_date,
        end_date,
        raise_on_transient=True,
    )


def fetch_ibkr_daily_close_fx(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> pd.Series:
    """Fetch daily FX close series through the IBKR market data client."""
    if not _ibkr_fx_daily_enabled():
        return pd.Series(dtype=float)
    if _ibkr_ts_cache_enabled():
        try:
            from .timeseries_cache import cached_daily_fetch

            return cached_daily_fetch(
                symbol,
                start_date,
                end_date,
                instrument_type="fx",
                raw_fetcher=_raw_daily_fx_for_cache,
            )
        except Exception as exc:
            logger.warning("IBKR cache-backed daily FX failed for %s, falling back: %s", symbol, exc)
            return _raw_daily_fx(symbol, start_date, end_date)
    return _raw_daily_fx(symbol, start_date, end_date)


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
        logger.warning("IBKR bond fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_bond(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    contract_identity: dict | None = None,
) -> pd.Series:
    """Raw IBKR daily bond fetch (fail-open, existing contract)."""
    try:
        client_cls = IBKRMarketDataClient
        if client_cls is None:
            from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

            client_cls = _IBKRMarketDataClient
        client = client_cls()
        return client.fetch_daily_close_bond(
            symbol,
            start_date,
            end_date,
            contract_identity=contract_identity,
        )
    except Exception as exc:
        logger.warning("IBKR daily bond fetch failed for %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _raw_daily_bond_for_cache(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    contract_identity: dict | None = None,
) -> pd.Series:
    """Raw IBKR daily bond fetch with transient failures propagated."""
    client_cls = IBKRMarketDataClient
    if client_cls is None:
        from .market_data import IBKRMarketDataClient as _IBKRMarketDataClient

        client_cls = _IBKRMarketDataClient
    client = client_cls()
    return client.fetch_daily_close_bond(
        symbol,
        start_date,
        end_date,
        contract_identity=contract_identity,
        raise_on_transient=True,
    )


def fetch_ibkr_daily_close_bond(
    symbol: str,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    contract_identity: dict | None = None,
) -> pd.Series:
    """Fetch daily bond close series through the IBKR market data client."""
    if not _ibkr_bond_daily_enabled():
        return pd.Series(dtype=float)
    if _ibkr_ts_cache_enabled():
        try:
            from .timeseries_cache import cached_daily_fetch

            return cached_daily_fetch(
                symbol,
                start_date,
                end_date,
                instrument_type="bond",
                raw_fetcher=_raw_daily_bond_for_cache,
                contract_identity=contract_identity,
            )
        except Exception as exc:
            logger.warning("IBKR cache-backed daily bond failed for %s, falling back: %s", symbol, exc)
            return _raw_daily_bond(symbol, start_date, end_date, contract_identity=contract_identity)
    return _raw_daily_bond(symbol, start_date, end_date, contract_identity=contract_identity)


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
        logger.warning("IBKR option fetch failed for %s: %s", symbol, exc)
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
    "fetch_ibkr_daily_close_fx",
    "fetch_ibkr_daily_close_bond",
    "fetch_ibkr_fx_monthly_close",
    "fetch_ibkr_bond_monthly_close",
    "fetch_ibkr_option_monthly_mark",
    "fetch_ibkr_flex_trades",
    "fetch_ibkr_flex_payload",
    "get_ibkr_futures_fmp_map",
    "get_ibkr_futures_exchanges",
    "get_ibkr_futures_contract_meta",
    "get_futures_contract_meta",
    "get_futures_currency",
    "get_futures_months",
    "get_futures_curve_snapshot",
    "IBKRDataError",
    "IBKRConnectionError",
    "IBKRContractError",
    "IBKRNoDataError",
    "IBKREntitlementError",
    "IBKRAccountError",
    "IBKRTimeoutError",
]
