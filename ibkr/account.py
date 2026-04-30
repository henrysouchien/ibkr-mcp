"""Account-level IBKR data helpers."""

from __future__ import annotations

import math
import time
from typing import Any

import pandas as pd

from app_platform.api_budget import BudgetExceededError

from ._budget import guard_ib_call
from .config import IBKR_PNL_POLL_INTERVAL, IBKR_PNL_TIMEOUT
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

_PORTFOLIO_COLUMNS = [
    "account",
    "symbol",
    "sec_type",
    "currency",
    "exchange",
    "con_id",
    "local_symbol",
    "last_trade_date",
    "strike",
    "right",
    "multiplier",
    "position",
    "avg_cost",
    "market_price",
    "market_value",
    "unrealized_pnl",
    "realized_pnl",
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


def _portfolio_frame_from_items(
    items: list[Any],
    *,
    account_id: str | None = None,
) -> pd.DataFrame:
    """Normalize IBKR portfolio items into the canonical DataFrame schema."""
    rows: list[dict[str, Any]] = []
    for item in items:
        acct = getattr(item, "account", None)
        if account_id and acct != account_id:
            continue
        contract = getattr(item, "contract", None)
        rows.append(
            {
                "account": acct,
                "symbol": getattr(contract, "symbol", None) if contract else None,
                "sec_type": getattr(contract, "secType", None) if contract else None,
                "currency": getattr(contract, "currency", None) if contract else None,
                "exchange": getattr(contract, "exchange", None) if contract else None,
                "con_id": getattr(contract, "conId", None) if contract else None,
                "local_symbol": getattr(contract, "localSymbol", None) if contract else None,
                "last_trade_date": (
                    getattr(contract, "lastTradeDateOrContractMonth", None)
                    if contract
                    else None
                ),
                "strike": _safe_float(getattr(contract, "strike", None)) if contract else None,
                "right": getattr(contract, "right", None) if contract else None,
                "multiplier": getattr(contract, "multiplier", None) if contract else None,
                "position": _safe_float(getattr(item, "position", None)),
                "avg_cost": _safe_float(getattr(item, "averageCost", None)),
                "market_price": _safe_float(getattr(item, "marketPrice", None)),
                "market_value": _safe_float(getattr(item, "marketValue", None)),
                "unrealized_pnl": _safe_float(getattr(item, "unrealizedPNL", None)),
                "realized_pnl": _safe_float(getattr(item, "realizedPNL", None)),
            }
        )

    if not rows:
        return pd.DataFrame(columns=_PORTFOLIO_COLUMNS)
    frame = pd.DataFrame(rows, columns=_PORTFOLIO_COLUMNS)
    return frame.sort_values(["account", "symbol"], na_position="last").reset_index(drop=True)


def _cash_balances_from_account_values(
    account_values: list[Any],
    *,
    account_id: str | None = None,
) -> dict[str, float]:
    """Extract per-currency cash balances from IBKR account values."""
    balances: dict[str, float] = {}
    for av in account_values:
        if account_id and getattr(av, "account", None) not in (None, "", account_id):
            continue
        if getattr(av, "tag", None) != "CashBalance":
            continue
        currency = getattr(av, "currency", None)
        if not currency or currency == "BASE":
            continue
        value = _safe_float(getattr(av, "value", None))
        if value is not None:
            balances[str(currency)] = value

    return balances


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


def fetch_positions(
    ib,
    account_id: str | None = None,
    *,
    budget_user_id: int | None = None,
) -> pd.DataFrame:
    """Fetch IBKR positions and normalize to DataFrame."""
    guard_ib_call(
        operation="reqPositions",
        fn=ib.reqPositions,
        budget_user_id=budget_user_id,
    )
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


def fetch_portfolio_items(
    ib,
    account_id: str | None = None,
    *,
    budget_user_id: int | None = None,
) -> pd.DataFrame:
    """Fetch IBKR portfolio items with market values via reqAccountUpdates."""
    items = list(ib.portfolio() or [])
    if not items:
        guard_ib_call(
            operation="reqAccountUpdates",
            fn=ib.reqAccountUpdates,
            kwargs={"account": account_id or ""},
            budget_user_id=budget_user_id,
        )
        items = list(ib.portfolio() or [])
    return _portfolio_frame_from_items(items, account_id=account_id)


def fetch_cash_balances(
    ib,
    account_id: str | None = None,
    *,
    budget_user_id: int | None = None,
) -> dict[str, float]:
    """Fetch per-currency cash balances from accountValues (CashBalance tag)."""
    account_values = list(ib.accountValues(account=account_id) or [])
    if not account_values:
        guard_ib_call(
            operation="reqAccountUpdates",
            fn=ib.reqAccountUpdates,
            kwargs={"account": account_id or ""},
            budget_user_id=budget_user_id,
        )
        account_values = list(ib.accountValues(account=account_id) or [])
    return _cash_balances_from_account_values(account_values, account_id=account_id)


def fetch_portfolio_with_cash(
    ib,
    account_id: str | None = None,
    *,
    budget_user_id: int | None = None,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Fetch portfolio items and cash balances with at most one cold refresh."""
    items = list(ib.portfolio() or [])
    account_values = list(ib.accountValues(account=account_id) or [])

    if not items or not account_values:
        guard_ib_call(
            operation="reqAccountUpdates",
            fn=ib.reqAccountUpdates,
            kwargs={"account": account_id or ""},
            budget_user_id=budget_user_id,
        )
        if not items:
            items = list(ib.portfolio() or [])
        if not account_values:
            account_values = list(ib.accountValues(account=account_id) or [])

    return (
        _portfolio_frame_from_items(items, account_id=account_id),
        _cash_balances_from_account_values(account_values, account_id=account_id),
    )


def fetch_account_summary(
    ib,
    account_id: str | None = None,
    *,
    budget_user_id: int | None = None,
) -> dict[str, float]:
    """Fetch account summary values with USD-only tag normalization."""
    account_values = list(ib.accountValues(account=account_id) or [])
    if not account_values:
        guard_ib_call(
            operation="reqAccountUpdates",
            fn=ib.reqAccountUpdates,
            kwargs={"account": account_id},
            budget_user_id=budget_user_id,
        )
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
    budget_user_id: int | None = None,
    timeout_seconds: float = IBKR_PNL_TIMEOUT,
    poll_interval: float = IBKR_PNL_POLL_INTERVAL,
) -> dict[str, float | str | None]:
    """Fetch account-level PnL via streaming subscription with timeout polling."""
    pnl_obj = guard_ib_call(
        operation="reqPnL",
        fn=ib.reqPnL,
        args=(account_id,),
        kwargs={"modelCode": ""},
        budget_user_id=budget_user_id,
    )
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
            guard_ib_call(
                operation="cancelPnL",
                fn=ib.cancelPnL,
                args=(account_id,),
                kwargs={"modelCode": ""},
                budget_user_id=budget_user_id,
            )
        except BudgetExceededError:
            raise
        except Exception:
            pass


def fetch_pnl_single(
    ib,
    account_id: str,
    con_id: int,
    *,
    budget_user_id: int | None = None,
    timeout_seconds: float = IBKR_PNL_TIMEOUT,
    poll_interval: float = IBKR_PNL_POLL_INTERVAL,
) -> dict[str, float | int | str | None]:
    """Fetch contract-level PnL via streaming subscription with timeout polling."""
    pnl_obj = guard_ib_call(
        operation="reqPnLSingle",
        fn=ib.reqPnLSingle,
        args=(account_id,),
        kwargs={"modelCode": "", "conId": int(con_id)},
        budget_user_id=budget_user_id,
    )
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
            guard_ib_call(
                operation="cancelPnLSingle",
                fn=ib.cancelPnLSingle,
                args=(account_id,),
                kwargs={"modelCode": "", "conId": int(con_id)},
                budget_user_id=budget_user_id,
            )
        except BudgetExceededError:
            raise
        except Exception:
            pass
