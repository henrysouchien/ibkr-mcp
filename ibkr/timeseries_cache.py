"""Incremental daily time-series cache for IBKR market data."""

from __future__ import annotations

import hashlib
import os
import threading
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from ibkr.contracts import _normalize_fx_pair
from ibkr.exceptions import IBKRConnectionError, IBKREntitlementError
from ibkr._shared.timeseries_store import TimeSeriesStore

_stores: dict[str, TimeSeriesStore] = {}
_store_guard = threading.Lock()


class IBKRTransientError(Exception):
    """Raised when IBKR fetch fails transiently (Gateway down, entitlement)."""


def _resolve_cache_dir() -> Path:
    """Resolve the IBKR timeseries cache directory."""
    ts_env = os.getenv("IBKR_TIMESERIES_CACHE_DIR")
    if ts_env:
        resolved = Path(ts_env).expanduser().resolve()
        resolved.mkdir(parents=True, exist_ok=True)
        return resolved

    ibkr_env = os.getenv("IBKR_CACHE_DIR")
    if ibkr_env:
        resolved = Path(ibkr_env).expanduser().resolve().parent / "ibkr_timeseries"
        resolved.mkdir(parents=True, exist_ok=True)
        return resolved

    root = Path(__file__).parent.parent
    if (root / "settings.py").is_file():
        resolved = root / "cache" / "ibkr_timeseries"
    else:
        resolved = Path.home() / ".cache" / "ibkr-mcp" / "ibkr_timeseries"
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def get_ibkr_timeseries_store(cache_dir: str | Path | None = None) -> TimeSeriesStore:
    """Get or create per-dir TimeSeriesStore singleton."""
    resolved = Path(cache_dir or _resolve_cache_dir()).expanduser().resolve()
    key = str(resolved)
    with _store_guard:
        store = _stores.get(key)
        if store is None:
            store = TimeSeriesStore(resolved.parent)
            store.cache_dir = resolved
            store.cache_dir.mkdir(parents=True, exist_ok=True)
            _stores[key] = store
        return store


def _reset_stores_for_tests() -> None:
    """Drop all store singletons for test isolation."""
    with _store_guard:
        _stores.clear()


def _cache_ticker(
    symbol: str,
    instrument_type: str,
    contract_identity: dict[str, Any] | None = None,
) -> str:
    """Build collision-free, path-safe cache ticker."""
    raw = str(symbol or "").strip().upper()
    if not raw:
        raise ValueError("Cannot build cache ticker for empty symbol")
    itype = (instrument_type or "").strip().lower()

    if itype == "fx":
        try:
            raw = _normalize_fx_pair(raw)
        except Exception:
            raw = raw.replace("/", "").replace(".", "")

    if itype == "bond" and isinstance(contract_identity, dict):
        id_parts: list[str] = []
        for key in ("con_id", "cusip", "isin"):
            value = contract_identity.get(key)
            if value is not None and str(value).strip():
                id_parts.append(f"{key}={value}")
        if id_parts:
            digest = hashlib.md5("|".join(sorted(id_parts)).encode()).hexdigest()[:8]
            raw = f"{raw}_{digest}"

    return raw.replace("/", "_").replace("\\", "_")


def cached_daily_fetch(
    symbol: str,
    start_date: Any,
    end_date: Any,
    *,
    instrument_type: str,
    raw_fetcher: Callable[..., pd.Series],
    contract_identity: dict[str, Any] | None = None,
) -> pd.Series:
    """Cache-backed daily series fetch using incremental coverage extension."""
    store = get_ibkr_timeseries_store()
    ticker = _cache_ticker(symbol, instrument_type, contract_identity)
    kind = f"{instrument_type}_daily"
    start = pd.Timestamp(start_date).date().isoformat() if start_date else None
    end = pd.Timestamp(end_date).date().isoformat() if end_date else None

    def _loader(loader_start: str | None, loader_end: str | None) -> pd.Series:
        kwargs: dict[str, Any] = {"start_date": loader_start, "end_date": loader_end}
        if contract_identity is not None:
            kwargs["contract_identity"] = contract_identity
        try:
            return raw_fetcher(symbol, **kwargs)
        except (IBKRConnectionError, IBKREntitlementError) as exc:
            raise IBKRTransientError(str(exc)) from exc

    return store.read(
        ticker=ticker,
        series_kind=kind,
        start=start,
        end=end,
        loader=_loader,
        max_age_days=1,
    )
