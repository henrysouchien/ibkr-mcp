"""Project-specific starter costs for the API budget guard.

Units:
- COST_PER_CALL: USD per outbound call.
- SUBSCRIPTION_COSTS_PER_ITEM_MONTH: USD per (Item, calendar month).
  Applies to Plaid: "first call of the month charges the full subscription,
  subsequent calls charge $0."
- SNAPTRADE_SUBSCRIPTION_OPS + SNAPTRADE_PER_CONNECTED_USER_MONTH_RATE:
  SnapTrade bills $1.50/Connected User/month covering 16 of 18 subscription ops
  (V4c R2 verified contracted rate from dashboard.snaptrade.com, 2026-04-25).
  Two per-call carve-outs in COST_PER_CALL above:
    • connections.refresh_brokerage_authorization (Manual Refresh): $0.05/call
    • accounts.orders (Recent Orders): $0.002/call
- LLM_PRICES: USD per 1 million input/output tokens.

Subject identifier conventions at guard_call site:
- Plaid subscription ops MUST receive an item_id.
- SnapTrade subscription ops MUST have a non-None budget_user_id (the
  existing guard_call kwarg). Several production callsites currently
  do NOT pass it — they're wired in P3 (see plan §6).
See app_platform/api_budget/guard.py.

Plaid pricing source: Master Agreement on dashboard.plaid.com (verified 2026-04-25).
SnapTrade pricing source: snaptrade.com/pricing + developer-terms-of-use (V4c, 2026-04-25).
"""

from __future__ import annotations
from decimal import Decimal
from typing import Literal


COST_PER_CALL: dict[tuple[str, str], Decimal] = {
    # Plaid per-call
    ("plaid", "accounts_balance_get"): Decimal("0.1000"),  # was 0.3000 — D5
    ("plaid", "investments_refresh"):  Decimal("0.1200"),  # new
    # REMOVED ("plaid", "accounts_get") — Plaid lists free (D5)
    # REMOVED ("plaid", "item_get")     — Plaid lists free (D5)

    # SnapTrade per-call (V4c R2 verified 2026-04-25 from dashboard.snaptrade.com)
    ("snaptrade", "connections.refresh_brokerage_authorization"): Decimal("0.0500"),  # Manual Refresh
    ("snaptrade", "accounts.orders"):                              Decimal("0.0020"),  # Recent Orders (separate per-call axis)
    # REMOVED ("snaptrade", "accounts.list")      — wrong; subscription-billed (D5/V4c)
    # REMOVED ("snaptrade", "accounts.positions") — wrong; subscription-billed (D5/V4c)

    # Schwab per-call (V4d corrected to $0.00 — Trader API is rate-limit-only, free for account holders)
    ("schwab", "get_account"):  Decimal("0.0000"),
    ("schwab", "get_accounts"): Decimal("0.0000"),

    # IBKR per-call (V4d corrected to $0.00 — data API rate-limit-only; cost is market-data subscriptions)
    ("ibkr", "reqPositions"):      Decimal("0.0000"),
    ("ibkr", "reqAccountSummary"): Decimal("0.0000"),

    # FMP (flat-subscription; per-call cost $0)
    ("fmp", "fetch"):           Decimal("0.0000"),
    ("fmp_estimates", "get"):   Decimal("0.0000"),
}


SUBSCRIPTION_COSTS_PER_ITEM_MONTH: dict[tuple[str, str], Decimal] = {
    ("plaid", "investments_holdings_get"):     Decimal("0.1800"),
    ("plaid", "investments_transactions_get"): Decimal("0.3500"),
    ("plaid", "transactions_get"):             Decimal("0.3000"),  # defensive
    ("plaid", "liabilities_get"):              Decimal("0.2000"),  # defensive
}


# SnapTrade Connected-User-per-month subscription: $1.50 covers 16 of 18 wrapped ops
# (V4c R2 verified 2026-04-25 from dashboard.snaptrade.com, key HENRY-CHIEN-LLC-RTXYG).
# Two per-call carve-outs (in COST_PER_CALL above):
#   - `connections.refresh_brokerage_authorization` (Manual Refresh, $0.05/call)
#   - `accounts.orders` (Recent Orders, $0.002/call — separate per-call axis)
SNAPTRADE_PER_CONNECTED_USER_MONTH_RATE: Decimal = Decimal("1.5000")

SNAPTRADE_SUBSCRIPTION_OPS: frozenset[str] = frozenset({
    # Names are taken verbatim from the guard_call(operation="...") strings in
    # brokerage/snaptrade/client.py. The two per-call carve-outs above (Manual
    # Refresh, Recent Orders) intentionally do NOT appear here.
    "authentication.register_snap_trade_user",
    "authentication.login_snap_trade_user",
    "authentication.delete_snap_trade_user",
    "authentication.reset_snap_trade_user_secret",
    "accounts.list",
    "accounts.positions",
    "accounts.balance",
    "accounts.activities",
    "connections.list_brokerage_authorizations",
    "connections.detail_brokerage_authorization",
    "connections.remove_brokerage_authorization",
    "reference_data.symbol_search_user_account",
    "trading.get_order_impact",
    "trading.place_order",
    "trading.cancel_order",
    "transactions_and_reporting.get_activities",
})

_LLM_PROVIDERS_LOCAL = frozenset({"openai", "anthropic"})


def get_cost_model_and_rate(
    provider: str, operation: str,
) -> tuple[
    Literal["per_call", "per_item_month", "per_connected_user_month", "per_token"],
    Decimal | None,
]:
    provider_key = str(provider or "").strip().lower()
    operation_key = str(operation or "").strip()
    key = (provider_key, operation_key)

    plaid_sub_rate = SUBSCRIPTION_COSTS_PER_ITEM_MONTH.get(key)
    if plaid_sub_rate is not None:
        return ("per_item_month", plaid_sub_rate)

    if provider_key == "snaptrade" and operation_key in SNAPTRADE_SUBSCRIPTION_OPS:
        return ("per_connected_user_month", SNAPTRADE_PER_CONNECTED_USER_MONTH_RATE)

    if provider_key in _LLM_PROVIDERS_LOCAL:
        return ("per_token", None)

    return ("per_call", COST_PER_CALL.get(key, Decimal("0")))


LLM_PRICES: dict[str, dict[str, float]] = {  # unchanged from current file (V4d-corrected rates)
    "gpt-4.1":          {"input_per_1m_tokens": 2.50, "output_per_1m_tokens": 10.00},
    "gpt-4.1-mini":     {"input_per_1m_tokens": 0.50, "output_per_1m_tokens":  2.00},
    "gpt-4o-mini":      {"input_per_1m_tokens": 0.20, "output_per_1m_tokens":  0.80},
    "claude-sonnet-4-6":{"input_per_1m_tokens": 3.00, "output_per_1m_tokens": 15.00},
    "claude-haiku-4-5": {"input_per_1m_tokens": 1.00, "output_per_1m_tokens":  5.00},
}


__all__ = [
    "COST_PER_CALL",
    "SUBSCRIPTION_COSTS_PER_ITEM_MONTH",
    "SNAPTRADE_SUBSCRIPTION_OPS",
    "SNAPTRADE_PER_CONNECTED_USER_MONTH_RATE",
    "LLM_PRICES",
    "get_cost_model_and_rate",
]
