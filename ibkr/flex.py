"""IBKR Flex Query client and trade normalization helpers.

Agent orientation:
    Canonical boundary for downloading/parsing Flex reports and converting rows
    into normalized trade/cash payloads used by transaction ingestion.

Upstream reference:
    - IBKR Flex Web Service: https://www.interactivebrokers.com/en/software/am-api/am/flex-web-service.htm
"""

from __future__ import annotations

import os
import importlib
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import certifi
import yaml
from ib_async import FlexReport

from ._logging import trading_logger
from ._types import InstrumentType
from ._vendor import normalize_strike, safe_float

try:
    _ticker_resolver = importlib.import_module("utils.ticker_resolver")
    _resolve_fmp_ticker = getattr(_ticker_resolver, "resolve_fmp_ticker", None)
except Exception:
    _resolve_fmp_ticker = None


_warned_missing_ticker_resolver = False


def resolve_fmp_ticker(
    ticker: str,
    company_name: str | None = None,
    currency: str | None = None,
    exchange_mic: str | None = None,
    **kwargs: Any,
) -> str:
    """Resolve FMP ticker with guarded fallback when monorepo resolver is unavailable."""
    if _resolve_fmp_ticker is not None:
        return _resolve_fmp_ticker(
            ticker=ticker,
            company_name=company_name,
            currency=currency,
            exchange_mic=exchange_mic,
            **kwargs,
        )

    global _warned_missing_ticker_resolver
    if not _warned_missing_ticker_resolver:
        trading_logger.warning(
            "utils.ticker_resolver unavailable; IBKR Flex symbol normalization fallback is active."
        )
        _warned_missing_ticker_resolver = True
    return ticker

# Fix macOS SSL: ib_async's FlexReport uses urllib.request.urlopen which
# relies on the system SSL context. On macOS, Python's bundled OpenSSL
# doesn't trust the system certificate store. Setting SSL_CERT_FILE to
# certifi's CA bundle ensures HTTPS connections to IBKR succeed.
if not os.environ.get("SSL_CERT_FILE"):
    os.environ["SSL_CERT_FILE"] = certifi.where()


@lru_cache(maxsize=1)
def _load_ibkr_exchange_mappings() -> dict[str, Any]:
    path = Path(__file__).resolve().with_name("exchange_mappings.yaml")
    try:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        trading_logger.warning("IBKR exchange_mappings.yaml not found at %s", path)
    except Exception as exc:
        trading_logger.warning("Failed to load IBKR exchange mappings from %s: %s", path, exc)
    return {}


def _parse_flex_date(date_val: Any) -> Optional[datetime]:
    """Parse Flex date (YYYYMMDD, YYYY-MM-DD, or datetime) to datetime."""
    if not date_val:
        return None
    if isinstance(date_val, datetime):
        return date_val
    if hasattr(date_val, "year") and hasattr(date_val, "month"):
        return datetime(date_val.year, date_val.month, date_val.day)
    s = str(date_val).strip().replace("-", "")
    try:
        return datetime.strptime(s[:8], "%Y%m%d")
    except (ValueError, TypeError):
        return None


def _get_attr(obj: Any, *names: str, default: Any = None) -> Any:
    """Read first available attribute/key from object or dict."""
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj.get(name)
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _format_expiry(expiry: Any) -> str:
    """Format expiry into YYMMDD when possible."""
    if isinstance(expiry, datetime):
        return expiry.strftime("%y%m%d")

    raw = str(expiry or "").strip()
    if not raw:
        return ""

    for fmt in ("%Y-%m-%d", "%Y%m%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw[:10], fmt).strftime("%y%m%d")
        except ValueError:
            continue

    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 8:
        try:
            return datetime.strptime(digits[:8], "%Y%m%d").strftime("%y%m%d")
        except ValueError:
            pass
    if len(digits) == 6:
        return digits
    return raw


def _build_option_symbol(
    underlying: Any,
    put_call: Any,
    strike: Any,
    expiry: Any,
) -> str:
    """Build canonical option symbol: UNDERLYING_{C|P}{strike}_{YYMMDD}."""
    underlying_str = str(underlying or "").strip().upper()
    if not underlying_str:
        return ""

    put_call_str = str(put_call or "").strip().upper()
    if put_call_str.startswith("C"):
        option_type = "C"
    elif put_call_str.startswith("P"):
        option_type = "P"
    else:
        return underlying_str

    try:
        strike_str = normalize_strike(strike)
    except Exception:
        return underlying_str

    expiry_str = _format_expiry(expiry)
    if not expiry_str:
        return underlying_str

    return f"{underlying_str}_{option_type}{strike_str}_{expiry_str}"


def _map_trade_type(buy_sell: Any, open_close: Any) -> Optional[str]:
    """Map IBKR Flex buy/sell + open/close values to FIFO trade types."""
    side = str(buy_sell or "").strip().upper()
    oc = str(open_close or "").strip().upper()

    if side not in {"BUY", "SELL"}:
        return None

    if oc:
        if oc.startswith("O"):
            return "BUY" if side == "BUY" else "SHORT"
        if oc.startswith("C"):
            return "COVER" if side == "BUY" else "SELL"

    return "BUY" if side == "BUY" else "SELL"


def _map_instrument_type(asset_category: Any) -> InstrumentType:
    """Map IBKR Flex asset category to internal instrument type."""
    category = str(asset_category or "").strip().upper()
    if category == "OPT":
        return "option"
    if category == "FUT":
        return "futures"
    if category == "BOND":
        return "bond"
    if category == "CASH":
        return "fx_artifact"
    if category in {"STK", "ETF", "EQUITY"}:
        return "equity"
    return "unknown"


def _to_int_or_none(value: Any) -> Optional[int]:
    try:
        return int(float(value))
    except Exception:
        return None


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None or str(value).strip() == "":
        return None
    out = safe_float(value, float("nan"))
    if out != out:  # NaN check
        return None
    return float(out)


def _normalize_contract_expiry(expiry: Any) -> Optional[str]:
    parsed = _parse_flex_date(expiry)
    if parsed is not None:
        return parsed.strftime("%Y%m%d")
    raw = str(expiry or "").strip()
    if not raw:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


def _build_contract_identity(trade: Any) -> Optional[Dict[str, Any]]:
    """Build optional contract identity from Flex fields when available."""
    contract_identity: Dict[str, Any] = {}

    con_id = _to_int_or_none(_get_attr(trade, "conid", "conId"))
    if con_id is not None:
        contract_identity["con_id"] = con_id

    expiry = _normalize_contract_expiry(_get_attr(trade, "expiry", "expirationDate"))
    if expiry:
        contract_identity["expiry"] = expiry

    strike = _to_float_or_none(_get_attr(trade, "strike"))
    if strike is not None:
        contract_identity["strike"] = strike

    right_raw = str(_get_attr(trade, "putCall", "right", default="") or "").strip().upper()
    if right_raw.startswith("C"):
        contract_identity["right"] = "C"
    elif right_raw.startswith("P"):
        contract_identity["right"] = "P"

    multiplier = _to_float_or_none(_get_attr(trade, "multiplier"))
    if multiplier is not None:
        contract_identity["multiplier"] = multiplier

    exchange = str(_get_attr(trade, "exchange", default="") or "").strip().upper()
    if exchange:
        contract_identity["exchange"] = exchange

    return contract_identity or None


def normalize_flex_trades(flex_trades: Iterable[Any]) -> List[Dict[str, Any]]:
    """Convert Flex Trade rows into FIFO transaction dictionaries.

    Contract notes:
    - Output rows are provider-native normalized events consumed by
      ``ibkr.compat`` and transaction adapters.
    - Unmappable rows are skipped with warning rather than raising.
    """
    normalized: List[Dict[str, Any]] = []
    exchange_mappings = _load_ibkr_exchange_mappings()
    ibkr_exchange_to_mic = {
        str(exchange).strip().upper(): str(mic).strip().upper()
        for exchange, mic in (exchange_mappings.get("ibkr_exchange_to_mic", {}) or {}).items()
        if str(exchange).strip() and str(mic).strip()
    }

    for trade in flex_trades:
        trade_type = _map_trade_type(
            _get_attr(trade, "buySell", "side", "tradeType"),
            _get_attr(trade, "openCloseIndicator", "openClose"),
        )
        if trade_type is None:
            trading_logger.warning(
                "Skipping Flex trade with unmappable side/open-close: buySell=%s openClose=%s",
                _get_attr(trade, "buySell", "side", "tradeType"),
                _get_attr(trade, "openCloseIndicator", "openClose"),
            )
            continue

        trade_date = _parse_flex_date(_get_attr(trade, "tradeDate", "dateTime", "date"))
        if trade_date is None:
            trading_logger.warning("Skipping Flex trade with invalid date: %s", trade)
            continue

        asset_category = str(_get_attr(trade, "assetCategory", "assetClass", default="")).upper()
        instrument_type = _map_instrument_type(asset_category)
        contract_identity = _build_contract_identity(trade)
        multiplier = safe_float(_get_attr(trade, "multiplier", default=1.0), 1.0)
        if multiplier <= 0:
            multiplier = 1.0
        is_option = asset_category == "OPT"
        is_futures = asset_category == "FUT"

        symbol = str(_get_attr(trade, "symbol", default="") or "").strip().upper()
        underlying = str(
            _get_attr(trade, "underlyingSymbol", "underlying", default=symbol) or symbol
        ).strip().upper()

        if is_option:
            built = _build_option_symbol(
                underlying=underlying,
                put_call=_get_attr(trade, "putCall", "right"),
                strike=_get_attr(trade, "strike"),
                expiry=_get_attr(trade, "expiry", "expirationDate"),
            )
            symbol = built or symbol or underlying
        elif is_futures:
            symbol = underlying
            raw_underlying = _get_attr(trade, "underlyingSymbol", "underlying")
            if not raw_underlying or str(raw_underlying).strip() == "":
                trading_logger.warning(
                    "FUT trade missing underlyingSymbol; using raw symbol %s "
                    "(may not match FMP mapping)",
                    symbol,
                )
        else:
            symbol = symbol or underlying
            if asset_category == "STK":
                exchange_code = str(_get_attr(trade, "exchange", default="") or "").strip().upper()
                exchange_mic = ibkr_exchange_to_mic.get(exchange_code)
                if not exchange_mic:
                    listing_exchange = str(_get_attr(trade, "listingExchange", default="") or "").strip().upper()
                    exchange_mic = ibkr_exchange_to_mic.get(listing_exchange)
                if exchange_mic:
                    # IBKR can report trailing-dot symbols (e.g., "AT."); strip before suffix resolution.
                    base_symbol = symbol.rstrip(".")
                    if base_symbol:
                        symbol = resolve_fmp_ticker(
                            ticker=base_symbol,
                            company_name=None,
                            currency=str(_get_attr(trade, "currency", default="USD") or "USD").upper(),
                            exchange_mic=exchange_mic,
                        )

        quantity = abs(safe_float(_get_attr(trade, "quantity", "qty"), 0.0))
        if is_futures and multiplier != 1:
            quantity = quantity * multiplier
        if quantity <= 0:
            trading_logger.warning("Skipping Flex trade with non-positive quantity: %s", trade)
            continue

        trade_price = safe_float(_get_attr(trade, "tradePrice", "price"), 0.0)
        price = trade_price * multiplier if is_option and multiplier > 1 else trade_price
        fee = abs(
            safe_float(
                _get_attr(trade, "ibCommission", "commission", "commissionAmount"),
                0.0,
            )
        )

        currency = str(_get_attr(trade, "currency", default="USD") or "USD").upper()
        account_id = str(_get_attr(trade, "accountId", "accountID", default="") or "")
        trade_id = _get_attr(trade, "tradeID", "transactionID", "execID")
        if trade_id is None or str(trade_id).strip() == "":
            trade_id = f"row_{len(normalized)}"

        # Option closing at $0 = expired worthless
        option_expired = is_option and price == 0 and trade_type in ("SELL", "COVER")

        normalized.append(
            {
                "symbol": symbol,
                "type": trade_type,
                "date": trade_date,
                "quantity": quantity,
                "price": price,
                "fee": fee,
                "currency": currency,
                "source": "ibkr_flex",
                "transaction_id": f"ibkr_flex_{trade_id}",
                "is_option": is_option,
                "is_futures": is_futures,
                "instrument_type": instrument_type,
                "contract_identity": contract_identity,
                "option_expired": option_expired,
                "account_id": account_id,
                "_institution": "ibkr",
            }
        )

    return normalized


def _normalize_identifier(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"true", "1", "yes", "y", "t"}


def _parse_cash_amount(value: Any, *, default: float = 0.0) -> float:
    parsed = safe_float(value, default)
    if parsed != parsed:  # NaN check
        return default
    return float(parsed)


def _report_window(report: Any) -> tuple[datetime | None, datetime | None]:
    root = getattr(report, "root", None)
    if root is None:
        return None, None
    for node in root.iter("FlexStatement"):
        if not getattr(node, "attrib", None):
            continue
        start = _parse_flex_date(node.attrib.get("fromDate"))
        end = _parse_flex_date(node.attrib.get("toDate"))
        return start, end
    return None, None


def _report_topics(report: Any) -> set[str]:
    def _topics_from_root() -> set[str]:
        root = getattr(report, "root", None)
        if root is None:
            return set()
        return {
            str(node.tag)
            for node in root.iter()
            if getattr(node, "attrib", None)
        }

    try:
        raw = report.topics()
    except Exception:
        return _topics_from_root()
    if isinstance(raw, set):
        topics = {str(tag) for tag in raw}
        return topics or _topics_from_root()
    if isinstance(raw, list):
        topics = {str(tag) for tag in raw}
        return topics or _topics_from_root()
    return _topics_from_root()


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    payload = getattr(row, "__dict__", None)
    if isinstance(payload, dict):
        return dict(payload)
    try:
        return dict(row)
    except Exception:
        return {}


def _extract_rows(report: Any, topic: str) -> list[dict[str, Any]]:
    try:
        rows = report.extract(topic, parseNumbers=False)
    except TypeError:
        # Older ib_async versions may not expose parseNumbers kwarg.
        rows = report.extract(topic)
    except Exception as exc:
        trading_logger.warning("Failed to parse IBKR Flex %s rows: %s", topic, exc)
        return []
    return [_row_to_dict(row) for row in rows]


def _normalize_flex_currency(value: Any) -> str:
    text = str(value or "").strip().upper()
    if len(text) == 3 and text.isalpha():
        return text
    return "USD"


def _canonical_cash_type(raw_type: str) -> str:
    # Map human-readable Flex XML type strings to canonical enum codes.
    _READABLE_TO_ENUM: dict[str, str] = {
        "DEPOSITS & WITHDRAWALS": "DEPOSITWITHDRAW",
        "DEPOSITS/WITHDRAWALS": "DEPOSITWITHDRAW",
        "BROKER INTEREST PAID": "BROKERINTPAID",
        "BROKER INTEREST RECEIVED": "BROKERINTRCVD",
        "BOND INTEREST PAID": "BONDINTPAID",
        "BOND INTEREST RECEIVED": "BONDINTRCVD",
        "OTHER FEES": "FEES",
        "COMMISSION ADJUSTMENTS": "COMMADJ",
        "ADVISOR FEES": "ADVISORFEES",
        "DIVIDENDS": "DIVIDEND",
        "PAYMENT IN LIEU OF DIVIDENDS": "PAYMENTINLIEU",
        "WITHHOLDING TAX": "WHTAX",
    }
    return _READABLE_TO_ENUM.get(raw_type, raw_type)


def _cash_classification(raw_type: str, amount: float) -> tuple[str, bool, float, bool]:
    canonical = _canonical_cash_type(raw_type)

    if canonical == "DEPOSITWITHDRAW":
        if amount > 0:
            return "contribution", True, abs(amount), True
        if amount < 0:
            return "withdrawal", True, -abs(amount), True
        return "transfer", False, 0.0, False

    fee_types = {
        "BROKERINTPAID",
        "BONDINTPAID",
        "FEES",
        "COMMADJ",
        "ADVISORFEES",
    }
    if canonical in fee_types:
        return "fee", False, -abs(amount), False

    if "TRANSFER" in canonical:
        return "transfer", False, amount, True

    # Dividend/interest/withholding rows are not external flows — skip for flow
    # classification. They are handled separately by the income pipeline.
    return "", False, 0.0, False


def _income_trade_type_for_cash_type(raw_type: str) -> str:
    canonical = _canonical_cash_type(raw_type)
    if canonical in {"DIVIDEND", "PAYMENTINLIEU"}:
        return "DIVIDEND"
    if canonical in {"BROKERINTRCVD", "BONDINTRCVD", "BROKERINTPAID", "BONDINTPAID"}:
        return "INTEREST"
    return ""


def normalize_flex_cash_income_trades(raw_cash_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert raw Flex CashTransaction dividend/interest rows to trade-format records."""
    normalized: list[dict[str, Any]] = []

    for index, row in enumerate(raw_cash_rows):
        trade_type = _income_trade_type_for_cash_type(str(row.get("type") or "").strip().upper())
        if not trade_type:
            continue

        event_dt = _parse_flex_date(row.get("dateTime") or row.get("date") or row.get("reportDate"))
        if event_dt is None:
            continue

        amount = _parse_cash_amount(row.get("amount"), default=0.0)
        if amount == 0.0:
            continue

        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            desc = str(row.get("description") or "").strip()
            if "(" in desc:
                symbol = desc.split("(")[0].strip().upper()
        if not symbol:
            symbol = "UNKNOWN"

        account_id = _normalize_identifier(row.get("accountId") or row.get("accountID"))
        account_name = _normalize_identifier(
            row.get("accountAlias")
            or row.get("acctAlias")
            or row.get("accountName")
        )
        transaction_id = _normalize_identifier(
            row.get("transactionID") or row.get("tradeID") or row.get("id")
        )
        if transaction_id is None:
            transaction_id = (
                f"income:{account_id or account_name or 'unknown'}:{event_dt.isoformat()}:"
                f"{_normalize_flex_currency(row.get('currency'))}:{trade_type}:{abs(amount):.8f}:{index}"
            )

        normalized.append(
            {
                "symbol": symbol,
                "type": trade_type,
                "date": event_dt,
                "amount": float(amount),
                "quantity": 0.0,
                "price": 0.0,
                "fee": 0.0,
                "currency": _normalize_flex_currency(row.get("currency")),
                "source": "ibkr_flex",
                "transaction_id": str(transaction_id),
                "is_option": False,
                "is_futures": False,
                "instrument_type": "fx_artifact",
                "account_id": account_id,
                "account_name": account_name,
                "_institution": "ibkr",
            }
        )

    normalized.sort(
        key=lambda row: (
            _parse_flex_date(row.get("date")) or datetime.min,
            str(row.get("transaction_id") or ""),
            str(row.get("account_id") or ""),
        )
    )
    return normalized


def _transfer_classification(direction: str, amount: float) -> tuple[str, bool, float, bool]:
    if direction == "IN":
        return "contribution", True, abs(amount), True
    if direction == "OUT":
        return "withdrawal", True, -abs(amount), True
    if amount == 0:
        return "", False, 0.0, False
    return "transfer", False, amount, True


def _build_overlap_key(row: dict[str, Any]) -> tuple[str, str, str, str, str, float, str]:
    event_dt = _parse_flex_date(row.get("event_datetime") or row.get("date"))
    event_day = event_dt.date().isoformat() if event_dt is not None else ""
    amount = abs(_parse_cash_amount(row.get("amount"), default=0.0))
    account_identity = (
        _normalize_identifier(row.get("account_id"))
        or _normalize_identifier(row.get("provider_account_ref"))
        or _normalize_identifier(row.get("account_name"))
        or "unknown"
    )
    return (
        "ibkr_flex",
        str(row.get("institution") or "ibkr").strip().lower() or "ibkr",
        account_identity,
        event_day,
        _normalize_flex_currency(row.get("currency")),
        round(amount, 8),
        str(row.get("flow_type") or "").strip().lower(),
    )


def _normalize_cash_transaction_row(raw_row: dict[str, Any], row_index: int) -> dict[str, Any]:
    amount = _parse_cash_amount(raw_row.get("amount"), default=0.0)
    raw_type = str(raw_row.get("type") or "").strip().upper()
    flow_type, is_external, signed_amount, transfer_cash_confirmed = _cash_classification(raw_type, amount)
    if not flow_type or signed_amount == 0.0:
        return {}

    event_dt = _parse_flex_date(raw_row.get("dateTime") or raw_row.get("date") or raw_row.get("reportDate"))
    if event_dt is None:
        return {}

    account_id = _normalize_identifier(raw_row.get("accountId") or raw_row.get("accountID"))
    account_name = _normalize_identifier(raw_row.get("accountAlias") or raw_row.get("accountName"))
    provider_account_ref = _normalize_identifier(raw_row.get("accountAlias"))
    transaction_id = _normalize_identifier(
        raw_row.get("transactionID") or raw_row.get("tradeID") or raw_row.get("id")
    )
    if transaction_id is None:
        transaction_id = (
            f"cash:{account_id or provider_account_ref or account_name or 'unknown'}:"
            f"{event_dt.isoformat()}:{_normalize_flex_currency(raw_row.get('currency'))}:"
            f"{abs(signed_amount):.8f}:{flow_type}:{row_index}"
        )

    return {
        "provider": "ibkr_flex",
        "institution": "ibkr",
        "account_id": account_id,
        "account_name": account_name,
        "provider_account_ref": provider_account_ref,
        "transaction_id": str(transaction_id),
        "event_datetime": event_dt,
        "date": event_dt,
        "currency": _normalize_flex_currency(raw_row.get("currency")),
        "amount": float(signed_amount),
        "flow_type": flow_type,
        "is_external_flow": bool(is_external),
        "transfer_cash_confirmed": bool(transfer_cash_confirmed),
        "raw_type": raw_type,
        "raw_subtype": str(raw_row.get("code") or ""),
        "raw_description": str(raw_row.get("description") or ""),
        "section": "CashTransaction",
    }


def _normalize_transfer_row(raw_row: dict[str, Any], row_index: int) -> dict[str, Any]:
    if not _as_bool(raw_row.get("cashTransfer")):
        return {}

    amount_value = raw_row.get("positionAmount")
    if amount_value in (None, ""):
        amount_value = raw_row.get("amount")
    if amount_value in (None, ""):
        amount_value = raw_row.get("quantity")
    amount = _parse_cash_amount(amount_value, default=0.0)
    if amount == 0.0:
        return {}

    direction = str(raw_row.get("direction") or "").strip().upper()
    flow_type, is_external, signed_amount, transfer_cash_confirmed = _transfer_classification(direction, amount)
    if not flow_type or signed_amount == 0.0:
        return {}

    event_dt = _parse_flex_date(raw_row.get("dateTime") or raw_row.get("date") or raw_row.get("reportDate"))
    if event_dt is None:
        return {}

    account_id = _normalize_identifier(raw_row.get("accountId") or raw_row.get("accountID"))
    account_name = _normalize_identifier(raw_row.get("accountAlias") or raw_row.get("accountName"))
    provider_account_ref = _normalize_identifier(raw_row.get("accountAlias"))
    transaction_id = _normalize_identifier(
        raw_row.get("transactionID") or raw_row.get("tradeID") or raw_row.get("id")
    )
    if transaction_id is None:
        transaction_id = (
            f"transfer:{account_id or provider_account_ref or account_name or 'unknown'}:"
            f"{event_dt.isoformat()}:{_normalize_flex_currency(raw_row.get('currency'))}:"
            f"{abs(signed_amount):.8f}:{flow_type}:{row_index}"
        )

    return {
        "provider": "ibkr_flex",
        "institution": "ibkr",
        "account_id": account_id,
        "account_name": account_name,
        "provider_account_ref": provider_account_ref,
        "transaction_id": str(transaction_id),
        "event_datetime": event_dt,
        "date": event_dt,
        "currency": _normalize_flex_currency(raw_row.get("currency")),
        "amount": float(signed_amount),
        "flow_type": flow_type,
        "is_external_flow": bool(is_external),
        "transfer_cash_confirmed": bool(transfer_cash_confirmed),
        "raw_type": str(raw_row.get("type") or "TRANSFER"),
        "raw_subtype": str(raw_row.get("code") or ""),
        "raw_description": str(raw_row.get("description") or ""),
        "section": "Transfer",
    }


def normalize_flex_cash_rows(
    cash_rows: Iterable[dict[str, Any]],
    transfer_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    primary_overlap_keys: set[tuple[str, str, str, str, str, float, str]] = set()

    for index, row in enumerate(cash_rows):
        normalized_row = _normalize_cash_transaction_row(row, index)
        if not normalized_row:
            continue
        normalized.append(normalized_row)
        primary_overlap_keys.add(_build_overlap_key(normalized_row))

    for index, row in enumerate(transfer_rows):
        normalized_row = _normalize_transfer_row(row, index)
        if not normalized_row:
            continue
        if _build_overlap_key(normalized_row) in primary_overlap_keys:
            continue
        normalized.append(normalized_row)

    normalized.sort(
        key=lambda row: (
            _parse_flex_date(row.get("event_datetime") or row.get("date")) or datetime.min,
            str(row.get("transaction_id") or ""),
            str(row.get("account_id") or ""),
        )
    )
    return normalized


def _load_flex_report(
    *,
    token: str = "",
    query_id: str = "",
    path: Optional[str] = None,
) -> tuple[Any | None, str | None]:
    if path:
        if not Path(path).exists():
            return None, f"IBKR Flex XML file not found: {path}"
        try:
            return FlexReport(path=path), None
        except Exception as exc:
            return None, f"Failed to load IBKR Flex XML from {path}: {exc}"

    if not token or not query_id:
        return None, "IBKR Flex credentials missing; skipping Flex download"

    try:
        return FlexReport(token=token, queryId=query_id), None
    except Exception as exc:
        exc_msg = str(exc)
        if token and token in exc_msg:
            exc_msg = exc_msg.replace(token, "***")
        if query_id and query_id in exc_msg:
            exc_msg = exc_msg.replace(query_id, "***")
        return None, f"IBKR Flex download failed: {exc_msg}"


def fetch_ibkr_flex_payload(
    token: str = "",
    query_id: str = "",
    path: Optional[str] = None,
) -> Dict[str, Any]:
    """Download/load IBKR Flex report and return trades + cash rows + fetch diagnostics."""
    payload: Dict[str, Any] = {
        "trades": [],
        "cash_rows": [],
        "fetch_window_start": None,
        "fetch_window_end": None,
        "pagination_exhausted": False,
        "partial_data": True,
        "fetch_error": None,
        "cash_transaction_section_present": False,
        "transfer_section_present": False,
    }

    report, load_error = _load_flex_report(token=token, query_id=query_id, path=path)
    if load_error is not None:
        trading_logger.warning("%s", load_error)
        payload["fetch_error"] = load_error
        return payload

    window_start, window_end = _report_window(report)
    payload["fetch_window_start"] = window_start
    payload["fetch_window_end"] = window_end

    topics = _report_topics(report)
    cash_topic_present = "CashTransaction" in topics
    transfer_topic_present = "Transfer" in topics
    payload["cash_transaction_section_present"] = cash_topic_present
    payload["transfer_section_present"] = transfer_topic_present

    raw_trades = _extract_rows(report, "Trade")
    if raw_trades:
        payload["trades"] = normalize_flex_trades(raw_trades)

    raw_cash_rows = _extract_rows(report, "CashTransaction")
    raw_transfer_rows = _extract_rows(report, "Transfer")
    payload["cash_rows"] = normalize_flex_cash_rows(raw_cash_rows, raw_transfer_rows)
    payload["trades"].extend(normalize_flex_cash_income_trades(raw_cash_rows))

    partial_messages: list[str] = []
    if not cash_topic_present:
        partial_messages.append("CashTransaction section missing in Flex report")
    if not cash_topic_present and not transfer_topic_present:
        trading_logger.warning(
            "IBKR Flex report missing cash sections (CashTransaction, Transfer); "
            "provider-flow authority remains non-authoritative."
        )

    if partial_messages:
        payload["fetch_error"] = "; ".join(partial_messages)
        payload["partial_data"] = True
        payload["pagination_exhausted"] = False
    else:
        payload["fetch_error"] = None
        payload["partial_data"] = False
        payload["pagination_exhausted"] = True

    return payload


def fetch_ibkr_flex_trades(
    token: str = "",
    query_id: str = "",
    path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Download/load IBKR Flex trades and normalize to FIFO transaction format."""
    if path and not Path(path).exists():
        raise FileNotFoundError(f"IBKR Flex XML file not found: {path}")
    payload = fetch_ibkr_flex_payload(token=token, query_id=query_id, path=path)
    return list(payload.get("trades") or [])
