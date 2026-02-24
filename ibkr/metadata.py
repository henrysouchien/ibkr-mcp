"""IBKR metadata helpers for contracts and option chains."""

from __future__ import annotations

from typing import Any

from .contracts import resolve_futures_contract
from .exceptions import IBKRContractError


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_contract(symbol: str, sec_type: str, exchange: str, currency: str):
    from ib_async import Contract, Future, Stock

    sym = str(symbol or "").strip().upper()
    if not sym:
        raise IBKRContractError("Symbol is required")

    sec = str(sec_type or "STK").strip().upper()
    exch = str(exchange or "SMART").strip().upper()
    curr = str(currency or "USD").strip().upper()

    if sec == "STK":
        return Stock(sym, exch, curr)
    if sec == "FUT":
        # Futures need their actual exchange, not SMART.
        # Use YAML mapping when exchange not explicitly overridden.
        if exch == "SMART":
            return resolve_futures_contract(sym)
        return Future(symbol=sym, exchange=exch, currency=curr)
    if sec == "OPT":
        return Contract(symbol=sym, secType="OPT", exchange=exch, currency=curr)
    return Contract(symbol=sym, secType=sec, exchange=exch, currency=curr)


def _normalize_contract_detail(contract_detail) -> dict[str, Any]:
    contract = getattr(contract_detail, "contract", None)
    valid_exchanges_raw = getattr(contract_detail, "validExchanges", None)
    valid_exchanges: list[str] = []
    if isinstance(valid_exchanges_raw, str):
        valid_exchanges = [e.strip() for e in valid_exchanges_raw.split(",") if e.strip()]
    elif isinstance(valid_exchanges_raw, (list, tuple, set)):
        valid_exchanges = [str(e) for e in valid_exchanges_raw if str(e).strip()]

    return {
        "con_id": getattr(contract, "conId", None) if contract else None,
        "symbol": getattr(contract, "symbol", None) if contract else None,
        "sec_type": getattr(contract, "secType", None) if contract else None,
        "exchange": getattr(contract, "exchange", None) if contract else None,
        "primary_exchange": getattr(contract, "primaryExchange", None) if contract else None,
        "currency": getattr(contract, "currency", None) if contract else None,
        "multiplier": getattr(contract, "multiplier", None) if contract else None,
        "min_tick": _safe_float(getattr(contract_detail, "minTick", None)),
        "trading_class": getattr(contract, "tradingClass", None) if contract else None,
        "valid_exchanges": valid_exchanges,
        "long_name": getattr(contract_detail, "longName", None),
        "industry": getattr(contract_detail, "industry", None),
        "category": getattr(contract_detail, "category", None),
        "subcategory": getattr(contract_detail, "subcategory", None),
        "trading_hours": getattr(contract_detail, "tradingHours", None),
        "liquid_hours": getattr(contract_detail, "liquidHours", None),
        "last_trade_date": (
            getattr(contract, "lastTradeDateOrContractMonth", None) if contract else None
        ),
    }


def fetch_contract_details(
    ib,
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
) -> list[dict[str, Any]]:
    """Fetch normalized contract details."""
    contract = _build_contract(symbol, sec_type, exchange, currency)
    details = list(ib.reqContractDetails(contract) or [])
    if not details:
        raise IBKRContractError(
            f"No contract details found for symbol={symbol} sec_type={sec_type}"
        )
    return [_normalize_contract_detail(detail) for detail in details]


def _normalize_chain(chain) -> dict[str, Any]:
    strikes_raw = list(getattr(chain, "strikes", []) or [])
    strikes = sorted(
        [strike for strike in (_safe_float(v) for v in strikes_raw) if strike is not None]
    )
    expirations = sorted([str(v) for v in list(getattr(chain, "expirations", []) or [])])
    return {
        "exchange": getattr(chain, "exchange", None),
        "expirations": expirations,
        "strikes": strikes,
        "multiplier": getattr(chain, "multiplier", None),
    }


def fetch_option_chain(
    ib,
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
) -> dict[str, Any]:
    """Fetch option chain metadata for STK/FUT underlyings."""
    from ib_async import Stock

    sec = str(sec_type or "STK").strip().upper()
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise IBKRContractError("Symbol is required")

    if sec == "STK":
        underlying = Stock(sym, exchange, "USD")
        fut_fop_exchange = ""
    elif sec == "FUT":
        underlying = resolve_futures_contract(sym)
        fut_fop_exchange = str(getattr(underlying, "exchange", "") or exchange)
    else:
        raise IBKRContractError(f"Option chain supports STK or FUT underlyings, got {sec}")

    qualified = list(ib.qualifyContracts(underlying) or [])
    if not qualified:
        raise IBKRContractError(f"Unable to qualify underlying for option chain: {sym}")

    resolved = qualified[0]
    con_id = getattr(resolved, "conId", None)
    if con_id in (None, ""):
        raise IBKRContractError(f"Qualified underlying missing conId for {sym}")

    underlying_symbol = str(getattr(resolved, "symbol", sym)).upper()
    if sec == "FUT":
        fut_fop_exchange = str(getattr(resolved, "exchange", "") or fut_fop_exchange or exchange)

    chains = list(
        ib.reqSecDefOptParams(
            underlyingSymbol=underlying_symbol,
            futFopExchange=fut_fop_exchange,
            underlyingSecType=sec,
            underlyingConId=int(con_id),
        )
        or []
    )

    return {
        "underlying": underlying_symbol,
        "con_id": int(con_id),
        "chains": [_normalize_chain(chain) for chain in chains],
    }
