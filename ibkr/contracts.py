"""IBKR contract resolution helpers."""

from __future__ import annotations

import math
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from ._logging import portfolio_logger
from ._types import InstrumentType, coerce_instrument_type

from .exceptions import IBKRContractError

Contract = Any

_FX_PAIR_RE = re.compile(r"^[A-Z]{3}[./]?[A-Z]{3}$")


@lru_cache(maxsize=1)
def _load_ibkr_exchange_mappings() -> dict[str, Any]:
    path = Path(__file__).resolve().with_name("exchange_mappings.yaml")
    try:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        portfolio_logger.warning("IBKR exchange_mappings.yaml not found at %s", path)
    except Exception as exc:
        portfolio_logger.warning("Failed to load IBKR exchange mappings from %s: %s", path, exc)
    return {}


def _futures_exchange_meta(symbol: str) -> tuple[str, str]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise IBKRContractError("Missing futures symbol")

    mappings = _load_ibkr_exchange_mappings().get("ibkr_futures_exchanges", {})
    raw = mappings.get(sym)
    if not isinstance(raw, dict):
        raise IBKRContractError(f"No IBKR futures exchange mapping configured for '{sym}'")

    exchange = str(raw.get("exchange") or "").strip()
    currency = str(raw.get("currency") or "USD").strip().upper()
    if not exchange:
        raise IBKRContractError(f"Invalid IBKR futures exchange mapping for '{sym}'")
    return exchange, currency


def resolve_futures_contract(symbol: str) -> Contract:
    """Resolve a futures root symbol into an IBKR continuous futures contract."""
    from ib_async import ContFuture

    sym = str(symbol or "").strip().upper()
    exchange, currency = _futures_exchange_meta(sym)
    return ContFuture(symbol=sym, exchange=exchange, currency=currency)


def _normalize_fx_pair(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        raise IBKRContractError("Missing FX symbol")

    # Accept canonical forms like GBP.HKD, GBP/HKD, or GBPHKD.
    if not _FX_PAIR_RE.match(raw) and len(re.sub(r"[^A-Z]", "", raw)) != 6:
        raise IBKRContractError(f"Invalid FX symbol '{symbol}'")

    pair = re.sub(r"[^A-Z]", "", raw)
    if len(pair) != 6:
        raise IBKRContractError(f"Invalid FX symbol '{symbol}'")
    return pair


def resolve_fx_contract(symbol: str) -> Contract:
    """Resolve an FX pair symbol into an IBKR Forex contract."""
    from ib_async import Forex

    pair = _normalize_fx_pair(symbol)
    return Forex(pair=pair)


def _coerce_con_id(contract_identity: dict[str, Any] | None) -> int | None:
    if not isinstance(contract_identity, dict):
        return None
    con_id = contract_identity.get("con_id")
    if con_id in (None, ""):
        return None
    # Reject non-finite or non-integer float values (e.g., NaN, inf, 123.9)
    if isinstance(con_id, float):
        if math.isnan(con_id) or math.isinf(con_id):
            raise IBKRContractError(f"Invalid con_id '{con_id}' in contract_identity") from None
        if con_id != int(con_id):
            raise IBKRContractError(f"Invalid con_id '{con_id}' in contract_identity (non-integer float)") from None
    try:
        return int(con_id)
    except (TypeError, ValueError, OverflowError):
        raise IBKRContractError(f"Invalid con_id '{con_id}' in contract_identity") from None


def resolve_bond_contract(symbol: str, contract_identity: dict[str, Any] | None = None) -> Contract:
    """Resolve a bond contract. v1 requires IBKR conId."""
    del symbol
    con_id = _coerce_con_id(contract_identity)
    if con_id is None:
        raise IBKRContractError("Bond pricing requires contract_identity.con_id")

    try:
        from ib_async import Bond

        return Bond(conId=con_id)
    except Exception:
        from ib_async import Contract

        return Contract(conId=con_id, secType="BOND")


_OCC_OPTION_RE = re.compile(r"^([A-Z]{1,6})\s*\d{6,8}[CP]\d+$")


def _infer_option_underlying(symbol: str) -> str:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return ""
    occ_match = _OCC_OPTION_RE.match(re.sub(r"\s+", "", sym))
    if occ_match:
        return occ_match.group(1)
    return sym


def resolve_option_contract(symbol: str, contract_identity: dict[str, Any] | None = None) -> Contract:
    """Resolve an option contract from conId or full contract identity."""
    identity = contract_identity if isinstance(contract_identity, dict) else {}
    con_id = _coerce_con_id(identity)

    from ib_async import Contract, Option

    if con_id is not None:
        try:
            return Option(conId=con_id)
        except Exception:
            return Contract(conId=con_id, secType="OPT")

    expiry = identity.get("expiry")
    strike = identity.get("strike")
    right_raw = str(identity.get("right") or "").strip().upper()
    right = right_raw[:1] if right_raw else ""
    if not expiry or strike in (None, "") or right not in {"C", "P"}:
        raise IBKRContractError(
            "Option pricing requires contract_identity with either con_id or (expiry, strike, right)"
        )

    underlying = str(
        identity.get("underlying")
        or identity.get("underlying_symbol")
        or identity.get("symbol")
        or _infer_option_underlying(symbol)
    ).strip().upper()
    if not underlying:
        raise IBKRContractError("Option pricing requires an underlying symbol in contract_identity")

    kwargs: dict[str, Any] = {
        "symbol": underlying,
        "lastTradeDateOrContractMonth": str(expiry),
        "strike": float(strike),
        "right": right,
        "exchange": str(identity.get("exchange") or "SMART"),
        "currency": str(identity.get("currency") or "USD").upper(),
    }
    multiplier = identity.get("multiplier")
    if multiplier not in (None, ""):
        kwargs["multiplier"] = str(multiplier)
    return Option(**kwargs)


def resolve_contract(
    symbol: str,
    instrument_type: str | InstrumentType,
    contract_identity: dict[str, Any] | None = None,
) -> Contract:
    """Resolve an IBKR contract for the requested instrument type."""
    raw_instrument_type = str(instrument_type or "").strip().lower()
    if raw_instrument_type in {"fx", "forex"}:
        return resolve_fx_contract(symbol)

    normalized = coerce_instrument_type(instrument_type, default="unknown")
    if normalized == "futures":
        return resolve_futures_contract(symbol)
    if normalized == "fx_artifact":
        return resolve_fx_contract(symbol)
    if normalized == "bond":
        return resolve_bond_contract(symbol, contract_identity=contract_identity)
    if normalized == "option":
        return resolve_option_contract(symbol, contract_identity=contract_identity)
    if normalized in {"equity", "unknown"}:
        raise IBKRContractError(
            f"IBKR resolver not yet implemented for instrument type '{normalized}'"
        )
    raise IBKRContractError(f"Unsupported instrument type '{instrument_type}'")
