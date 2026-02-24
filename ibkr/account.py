"""Account-level IBKR data helpers."""

from __future__ import annotations

import math
import time
from typing import Any

import pandas as pd

from .exceptions import IBKRTimeoutError


_POSITION_COLUMNS = [
    "account",
    "symbol",
    "sec_type",
    "currency",
    "exchange",
    "con_id",
    "position",
    "avg_cost",
]

_SUMMARY_TAGS = {
    "NetLiquidation": "net_liquidation",
    "TotalCashValue": "total_cash_value",
    "BuyingPower": "buying_power",
    "GrossPositionValue": "gross_position_value",
    "MaintMarginReq": "maint_margin_req",
    "AvailableFunds": "available_funds",
    "ExcessLiquidity": "excess_liquidity",
    "SMA": "sma",
}


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_not_nan(value: Any) -> bool:
    as_float = _safe_float(value)
    if as_float is None:
        return False
    return not math.isnan(as_float)


def _ib_sleep(ib, seconds: float) -> None:
    try:
        sleep_fn = getattr(ib, "sleep", None)
        if callable(sleep_fn):
            sleep_fn(seconds)
        else:
            time.sleep(seconds)
    except Exception:
        time.sleep(seconds)


def _wait_for_pnl_ready(ib, pnl_obj, *, timeout_seconds: float, poll_interval: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _is_not_nan(getattr(pnl_obj, "dailyPnL", None)):
            return
        _ib_sleep(ib, poll_interval)
    raise IBKRTimeoutError("Timed out waiting for IBKR PnL update")


def fetch_positions(ib, account_id: str | None = None) -> pd.DataFrame:
    """Fetch IBKR positions and normalize to DataFrame."""
    ib.reqPositions()
    positions = list(ib.positions() or [])

    rows: list[dict[str, Any]] = []
    for pos in positions:
        acct = getattr(pos, "account", None)
        if account_id and acct != account_id:
            continue
        contract = getattr(pos, "contract", None)
        rows.append(
            {
                "account": acct,
                "symbol": getattr(contract, "symbol", None) if contract else None,
                "sec_type": getattr(contract, "secType", None) if contract else None,
                "currency": getattr(contract, "currency", None) if contract else None,
                "exchange": getattr(contract, "exchange", None) if contract else None,
                "con_id": getattr(contract, "conId", None) if contract else None,
                "position": _safe_float(getattr(pos, "position", None)),
                "avg_cost": _safe_float(getattr(pos, "avgCost", None)),
            }
        )

    if not rows:
        return pd.DataFrame(columns=_POSITION_COLUMNS)
    frame = pd.DataFrame(rows, columns=_POSITION_COLUMNS)
    return frame.sort_values(["account", "symbol"], na_position="last").reset_index(drop=True)


def fetch_account_summary(ib, account_id: str | None = None) -> dict[str, float]:
    """Fetch account summary values with USD-only tag normalization."""
    account_values = list(ib.accountValues(account=account_id) or [])
    if not account_values:
        ib.reqAccountUpdates(account=account_id)
        account_values = list(ib.accountValues(account=account_id) or [])

    summary: dict[str, float] = {}
    for av in account_values:
        if account_id and getattr(av, "account", None) not in (None, "", account_id):
            continue
        if getattr(av, "currency", None) != "USD":
            continue
        key = _SUMMARY_TAGS.get(getattr(av, "tag", None))
        if key is None:
            continue
        val = _safe_float(getattr(av, "value", None))
        if val is None:
            continue
        summary[key] = val

    return summary


def fetch_pnl(
    ib,
    account_id: str,
    *,
    timeout_seconds: float = 5.0,
    poll_interval: float = 0.1,
) -> dict[str, float | str | None]:
    """Fetch account-level PnL via streaming subscription with timeout polling."""
    pnl_obj = ib.reqPnL(account_id, modelCode="")
    try:
        _wait_for_pnl_ready(
            ib,
            pnl_obj,
            timeout_seconds=timeout_seconds,
            poll_interval=poll_interval,
        )
        return {
            "account_id": account_id,
            "daily_pnl": _safe_float(getattr(pnl_obj, "dailyPnL", None)),
            "unrealized_pnl": _safe_float(getattr(pnl_obj, "unrealizedPnL", None)),
            "realized_pnl": _safe_float(getattr(pnl_obj, "realizedPnL", None)),
        }
    finally:
        try:
            ib.cancelPnL(account_id, modelCode="")
        except Exception:
            pass


def fetch_pnl_single(
    ib,
    account_id: str,
    con_id: int,
    *,
    timeout_seconds: float = 5.0,
    poll_interval: float = 0.1,
) -> dict[str, float | int | str | None]:
    """Fetch contract-level PnL via streaming subscription with timeout polling."""
    pnl_obj = ib.reqPnLSingle(account_id, modelCode="", conId=int(con_id))
    try:
        _wait_for_pnl_ready(
            ib,
            pnl_obj,
            timeout_seconds=timeout_seconds,
            poll_interval=poll_interval,
        )
        return {
            "account_id": account_id,
            "con_id": int(con_id),
            "daily_pnl": _safe_float(getattr(pnl_obj, "dailyPnL", None)),
            "unrealized_pnl": _safe_float(getattr(pnl_obj, "unrealizedPnL", None)),
            "realized_pnl": _safe_float(getattr(pnl_obj, "realizedPnL", None)),
            "position": _safe_float(getattr(pnl_obj, "position", None)),
            "value": _safe_float(getattr(pnl_obj, "value", None)),
        }
    finally:
        try:
            ib.cancelPnLSingle(account_id, modelCode="", conId=int(con_id))
        except Exception:
            pass
