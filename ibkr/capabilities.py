"""Capability descriptors for the IBKR API facade."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IBKRCapability:
    name: str
    category: str
    description: str
    parameters: list[dict[str, Any]]
    return_type: str
    requires_gateway: bool


_CAPABILITIES: dict[str, IBKRCapability] = {}


def register(cap: IBKRCapability) -> None:
    _CAPABILITIES[cap.name] = cap


def get_capability(name: str) -> IBKRCapability | None:
    return _CAPABILITIES.get(name)


def list_capabilities(category: str | None = None) -> list[IBKRCapability]:
    if category is None:
        return sorted(_CAPABILITIES.values(), key=lambda c: c.name)
    category_norm = str(category).strip().lower()
    return sorted(
        [c for c in _CAPABILITIES.values() if c.category == category_norm],
        key=lambda c: c.name,
    )


register(
    IBKRCapability(
        name="fetch_series",
        category="market_data",
        description="Fetch historical bars for a symbol/instrument type.",
        parameters=[
            {"name": "symbol", "type": "str", "required": True, "default": None, "description": "Ticker or pair"},
            {
                "name": "instrument_type",
                "type": "str",
                "required": True,
                "default": None,
                "description": "One of futures/fx/bond/option",
            },
            {"name": "start_date", "type": "date", "required": True, "default": None, "description": "Range start"},
            {"name": "end_date", "type": "date", "required": True, "default": None, "description": "Range end"},
            {
                "name": "what_to_show",
                "type": "str",
                "required": False,
                "default": None,
                "description": "IBKR whatToShow override",
            },
        ],
        return_type="Series",
        requires_gateway=True,
    )
)

register(
    IBKRCapability(
        name="fetch_monthly_close_futures",
        category="market_data",
        description="Fetch futures month-end close series.",
        parameters=[
            {"name": "symbol", "type": "str", "required": True, "default": None, "description": "Futures root"},
            {"name": "start_date", "type": "date", "required": True, "default": None, "description": "Range start"},
            {"name": "end_date", "type": "date", "required": True, "default": None, "description": "Range end"},
        ],
        return_type="Series",
        requires_gateway=True,
    )
)

register(
    IBKRCapability(
        name="fetch_monthly_close_fx",
        category="market_data",
        description="Fetch FX month-end close series.",
        parameters=[
            {"name": "symbol", "type": "str", "required": True, "default": None, "description": "FX pair"},
            {"name": "start_date", "type": "date", "required": True, "default": None, "description": "Range start"},
            {"name": "end_date", "type": "date", "required": True, "default": None, "description": "Range end"},
        ],
        return_type="Series",
        requires_gateway=True,
    )
)

register(
    IBKRCapability(
        name="fetch_monthly_close_bond",
        category="market_data",
        description="Fetch bond month-end close series.",
        parameters=[
            {"name": "symbol", "type": "str", "required": True, "default": None, "description": "Bond identifier"},
            {"name": "start_date", "type": "date", "required": True, "default": None, "description": "Range start"},
            {"name": "end_date", "type": "date", "required": True, "default": None, "description": "Range end"},
        ],
        return_type="Series",
        requires_gateway=True,
    )
)

register(
    IBKRCapability(
        name="fetch_monthly_close_option",
        category="market_data",
        description="Fetch option month-end mark series.",
        parameters=[
            {"name": "symbol", "type": "str", "required": True, "default": None, "description": "Option symbol"},
            {"name": "start_date", "type": "date", "required": True, "default": None, "description": "Range start"},
            {"name": "end_date", "type": "date", "required": True, "default": None, "description": "Range end"},
        ],
        return_type="Series",
        requires_gateway=True,
    )
)

register(
    IBKRCapability(
        name="positions",
        category="account",
        description="Fetch current positions for an IBKR account.",
        parameters=[
            {"name": "account_id", "type": "str", "required": False, "default": None, "description": "IBKR account ID"},
        ],
        return_type="DataFrame",
        requires_gateway=True,
    )
)

register(
    IBKRCapability(
        name="account_summary",
        category="account",
        description="Fetch account-level summary metrics.",
        parameters=[
            {"name": "account_id", "type": "str", "required": False, "default": None, "description": "IBKR account ID"},
        ],
        return_type="dict",
        requires_gateway=True,
    )
)

register(
    IBKRCapability(
        name="pnl",
        category="account",
        description="Fetch account-level daily/unrealized/realized PnL.",
        parameters=[
            {"name": "account_id", "type": "str", "required": False, "default": None, "description": "IBKR account ID"},
        ],
        return_type="dict",
        requires_gateway=True,
    )
)

register(
    IBKRCapability(
        name="pnl_single",
        category="account",
        description="Fetch contract-level daily/unrealized/realized PnL.",
        parameters=[
            {"name": "account_id", "type": "str", "required": True, "default": None, "description": "IBKR account ID"},
            {"name": "con_id", "type": "int", "required": True, "default": None, "description": "IBKR contract conId"},
        ],
        return_type="dict",
        requires_gateway=True,
    )
)

register(
    IBKRCapability(
        name="contract_details",
        category="metadata",
        description="Fetch normalized contract details.",
        parameters=[
            {"name": "symbol", "type": "str", "required": True, "default": None, "description": "Underlying symbol"},
            {"name": "sec_type", "type": "str", "required": False, "default": "STK", "description": "IBKR security type"},
            {"name": "exchange", "type": "str", "required": False, "default": "SMART", "description": "Exchange"},
            {"name": "currency", "type": "str", "required": False, "default": "USD", "description": "Currency"},
        ],
        return_type="list[dict]",
        requires_gateway=True,
    )
)

register(
    IBKRCapability(
        name="option_chain",
        category="metadata",
        description="Fetch option chain strikes/expirations for STK/FUT underlyings.",
        parameters=[
            {"name": "symbol", "type": "str", "required": True, "default": None, "description": "Underlying symbol"},
            {"name": "sec_type", "type": "str", "required": False, "default": "STK", "description": "STK or FUT"},
            {"name": "exchange", "type": "str", "required": False, "default": "SMART", "description": "Exchange"},
        ],
        return_type="dict",
        requires_gateway=True,
    )
)
