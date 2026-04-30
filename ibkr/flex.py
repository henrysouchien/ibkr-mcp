"""IBKR Flex Query client and trade normalization helpers.

Agent orientation:
    Canonical boundary for downloading/parsing Flex reports and converting rows
    into normalized trade/cash payloads used by transaction ingestion.

Upstream reference:
    - IBKR Flex Web Service: https://www.interactivebrokers.com/en/software/am-api/am/flex-web-service.htm
"""

from __future__ import annotations

import hashlib
import os
import importlib
import re
import time
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import certifi
import yaml
from app_platform.api_budget import BudgetExceededError
from ib_async import FlexReport

from ._logging import logger
from ._budget import guard_ib_call
from ._types import InstrumentType
from ._vendor import normalize_strike, safe_float

try:
    _ticker_resolver = importlib.import_module("utils.ticker_resolver")
    _resolve_ticker_from_exchange = getattr(_ticker_resolver, "resolve_ticker_from_exchange", None)
except Exception:
    _resolve_ticker_from_exchange = None


_warned_missing_ticker_resolver = False
_FUTURES_MONTH_CODES = set("FGHJKMNQUVXZ")


def resolve_ticker_from_exchange(
    ticker: str,
    company_name: str | None = None,
    currency: str | None = None,
    exchange_mic: str | None = None,
    **kwargs: Any,
) -> str:
    """Resolve FMP ticker with guarded fallback when monorepo resolver is unavailable."""
    if _resolve_ticker_from_exchange is not None:
        return _resolve_ticker_from_exchange(
            ticker=ticker,
            company_name=company_name,
            currency=currency,
            exchange_mic=exchange_mic,
            **kwargs,
        )

    global _warned_missing_ticker_resolver
    if not _warned_missing_ticker_resolver:
        logger.warning(
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
        logger.warning("IBKR exchange_mappings.yaml not found at %s", path)
    except Exception as exc:
        logger.warning("Failed to load IBKR exchange mappings from %s: %s", path, exc)
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
            logger.warning(
                "Skipping Flex trade with unmappable side/open-close: buySell=%s openClose=%s",
                _get_attr(trade, "buySell", "side", "tradeType"),
                _get_attr(trade, "openCloseIndicator", "openClose"),
            )
            continue

        trade_date = _parse_flex_date(_get_attr(trade, "tradeDate", "dateTime", "date"))
        if trade_date is None:
            logger.warning("Skipping Flex trade with invalid date: %s", trade)
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
                logger.warning(
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
                        symbol = resolve_ticker_from_exchange(
                            ticker=base_symbol,
                            company_name=None,
                            currency=str(_get_attr(trade, "currency", default="USD") or "USD").upper(),
                            exchange_mic=exchange_mic,
                        )

        quantity = abs(safe_float(_get_attr(trade, "quantity", "qty"), 0.0))
        if is_futures and multiplier != 1:
            quantity = quantity * multiplier
        if quantity <= 0:
            logger.warning("Skipping Flex trade with non-positive quantity: %s", trade)
            continue

        trade_price = safe_float(_get_attr(trade, "tradePrice", "price"), 0.0)
        price = trade_price * multiplier if is_option and multiplier > 1 else trade_price
        fee = abs(
            safe_float(
                _get_attr(trade, "ibCommission", "commission", "commissionAmount"),
                0.0,
            )
        ) + abs(
            safe_float(
                _get_attr(trade, "taxes"),
                0.0,
            )
        )
        broker_cost_basis = None
        broker_pnl = None
        open_close = str(
            _get_attr(trade, "openCloseIndicator", "openClose", default="") or ""
        ).upper()
        if trade_type in ("SELL", "COVER") and open_close != "O" and not is_futures:
            raw_cost = safe_float(_get_attr(trade, "cost"), 0.0)
            raw_qty = abs(safe_float(_get_attr(trade, "quantity", "qty"), 0.0))
            if abs(raw_cost) > 0 and raw_qty > 0:
                broker_cost_basis = abs(raw_cost) / raw_qty
            raw_pnl = safe_float(_get_attr(trade, "fifoPnlRealized"), 0.0)
            if raw_pnl != 0:
                broker_pnl = float(raw_pnl)

        currency = str(_get_attr(trade, "currency", default="USD") or "USD").upper()
        account_id = str(_get_attr(trade, "accountId", "accountID", default="") or "")
        raw_code = str(_get_attr(trade, "code", "notes", default="") or "").strip()
        code_parts = {p.strip().upper() for p in raw_code.split(";") if p.strip()}

        _has_exercise_token = any(
            tok in ("EX", "AEX", "MEX") or tok.endswith("EX")
            for tok in code_parts
        )
        _has_assignment_token = "A" in code_parts or "GEA" in code_parts
        is_exercise = _has_exercise_token and not _has_assignment_token
        is_assignment = _has_assignment_token
        is_exercise_or_assignment = is_exercise or is_assignment
        trade_id = _get_attr(trade, "tradeID", "transactionID", "execID")
        if trade_id is None or str(trade_id).strip() == "":
            trade_id = f"row_{len(normalized)}"

        # Option closing at $0 = expired worthless
        option_expired = (
            is_option and price == 0
            and trade_type in ("SELL", "COVER")
            and not is_exercise_or_assignment
        )

        normalized.append(
            {
                "symbol": symbol,
                "type": trade_type,
                "date": trade_date,
                "quantity": quantity,
                "price": price,
                "fee": fee,
                "broker_cost_basis": broker_cost_basis,
                "broker_pnl": broker_pnl,
                "currency": currency,
                "source": "ibkr_flex",
                "transaction_id": f"ibkr_flex_{trade_id}",
                "is_option": is_option,
                "is_futures": is_futures,
                "instrument_type": instrument_type,
                "contract_identity": contract_identity,
                "option_expired": option_expired,
                "option_exercised": is_option and is_exercise_or_assignment and trade_type in ("SELL", "COVER"),
                "stock_from_exercise": (
                    not is_option and not is_futures
                    and is_exercise_or_assignment
                    and trade_type in ("BUY", "SELL")
                ),
                "underlying": underlying,
                "exercise_code": raw_code if is_exercise_or_assignment else None,
                "account_id": account_id,
                "_institution": "ibkr",
            }
        )

    return normalized


def normalize_flex_prior_positions(
    rows: list[dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Extract option price history from PriorPeriodPosition rows.

    Returns list of {ticker, date, price, currency} dicts.
    Price is multiplied by multiplier (matching trade price convention
    at flex.py:356).
    """
    result: List[Dict[str, Any]] = []

    for row in rows:
        row_dict = _row_to_dict(row)
        asset_cat = str(row_dict.get("assetCategory", "")).upper()
        if asset_cat != "OPT":
            continue

        underlying = str(
            row_dict.get("underlyingSymbol", "") or ""
        ).strip().upper()
        put_call = row_dict.get("putCall")
        strike = row_dict.get("strike")
        expiry = row_dict.get("expiry")
        date_str = str(row_dict.get("date", "")).strip()
        currency = str(row_dict.get("currency", "USD") or "USD").upper()

        if not underlying or not date_str:
            continue

        raw_price = safe_float(row_dict.get("price"), default=None)
        if raw_price is None:
            continue

        ticker = _build_option_symbol(
            underlying=underlying,
            put_call=put_call,
            strike=strike,
            expiry=expiry,
        )
        if not ticker or ticker == underlying:
            continue

        multiplier = safe_float(row_dict.get("multiplier"), 1.0)
        if multiplier <= 0:
            multiplier = 1.0
        if multiplier > 1:
            price = raw_price * multiplier
        else:
            price = raw_price

        result.append({
            "ticker": ticker,
            "date": date_str,
            "price": price,
            "currency": currency,
        })

    return result


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
        logger.warning("Failed to parse IBKR Flex %s rows: %s", topic, exc)
        return []
    return [_row_to_dict(row) for row in rows]


def _normalize_flex_currency(value: Any) -> str:
    text = str(value or "").strip().upper()
    if len(text) == 3 and text.isalpha():
        return text
    return "USD"


@lru_cache(maxsize=1)
def _load_futures_root_symbols() -> tuple[str, ...]:
    try:
        from brokerage.futures import load_contract_specs

        specs = load_contract_specs()
        roots = sorted(
            (
                str(symbol or "").strip().upper()
                for symbol in (specs or {}).keys()
                if str(symbol or "").strip()
            ),
            key=len,
            reverse=True,
        )
        return tuple(roots)
    except Exception:
        return tuple()


def _strip_futures_contract_month(raw_symbol: Any) -> str:
    symbol = str(raw_symbol or "").strip().upper()
    if not symbol:
        return ""

    compact = "".join(ch for ch in symbol if ch.isalnum()).upper()
    if not compact:
        return symbol

    for root in _load_futures_root_symbols():
        if not compact.startswith(root):
            continue
        suffix = compact[len(root):]
        if len(suffix) >= 2 and suffix[0] in _FUTURES_MONTH_CODES and suffix[1:].isdigit():
            return root

    match = re.match(r"^([A-Z]{1,4})([FGHJKMNQUVXZ]\d{1,4})$", compact)
    if match:
        return str(match.group(1) or "").upper()
    return compact


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


def _is_synthetic_transaction_id(
    transaction_id: Any,
    *,
    synthetic_prefixes: tuple[str, ...],
) -> bool:
    transaction_id_text = str(transaction_id or "").strip()
    if not transaction_id_text:
        return True
    return any(transaction_id_text.startswith(prefix) for prefix in synthetic_prefixes)


def _dedup_account_id(row: dict[str, Any]) -> str:
    """Normalize account_id for dedup keys.

    IBKR Flex reports some rows with ``accountId="-"`` (summary rows) and
    others with the real account ID.  Treat ``"-"`` as empty so these
    collapse during dedup.
    """
    acct = str(row.get("account_id") or "").strip()
    return "" if acct == "-" else acct


def _select_dedup_winner(
    rows: list[dict[str, Any]],
    *,
    synthetic_prefixes: tuple[str, ...],
) -> dict[str, Any]:
    def _dedup_sort_key(row: dict[str, Any]) -> tuple[str, int, str, int, str]:
        transaction_id = str(row.get("transaction_id") or "")
        account_id = _dedup_account_id(row)
        account_name = str(row.get("account_name") or "").strip()
        has_acct = 0 if account_id else 1
        has_name = 0 if account_name else 1
        return (transaction_id, has_acct, account_id, has_name, account_name)

    real_rows = [
        row
        for row in rows
        if not _is_synthetic_transaction_id(
            row.get("transaction_id"),
            synthetic_prefixes=synthetic_prefixes,
        )
    ]
    if real_rows:
        return min(real_rows, key=_dedup_sort_key)
    return min(rows, key=_dedup_sort_key)


def _should_filter_non_detail(raw_rows: Iterable[dict[str, Any]]) -> bool:
    """Return True if we should filter non-DETAIL levelOfDetail rows.

    Only filters when at least one DETAIL row exists - if ALL rows are
    SUMMARY (abnormal Flex config), keep everything to avoid data loss.
    """
    for row in raw_rows:
        lod = str(row.get("levelOfDetail") or "").strip().upper()
        if lod == "DETAIL":
            return True
    return False


def _cross_currency_dedup(
    grouped_rows: list[dict[str, Any]],
    normalized_base_currency: str,
) -> list[dict[str, Any]]:
    """Deduplicate cross-currency rows while preserving distinct events."""
    if len(grouped_rows) <= 1:
        return list(grouped_rows)

    currencies = {_normalize_flex_currency(row.get("currency")) for row in grouped_rows}
    if len(currencies) <= 1:
        return list(grouped_rows)

    base_currency_rows = [
        row
        for row in grouped_rows
        if _normalize_flex_currency(row.get("currency")) == normalized_base_currency
    ]

    base_equivalents: list[tuple[int, float]] = []
    all_have_fx = True
    for index, row in enumerate(grouped_rows):
        fx_rate = safe_float(row.get("fx_rate_to_base"), 0.0)
        if fx_rate == 0.0:
            all_have_fx = False
            break
        raw_amount = _parse_cash_amount(row.get("amount"), default=0.0)
        base_equivalents.append((index, raw_amount * fx_rate))

    if not all_have_fx:
        if base_currency_rows:
            return base_currency_rows
        return list(grouped_rows)

    base_equivalents.sort(key=lambda item: item[1])

    tolerance = 0.10
    clusters: list[list[int]] = []
    cluster_amounts: list[float] = []
    for index, base_amount in base_equivalents:
        matched = False
        for cluster_index, reference_amount in enumerate(cluster_amounts):
            if abs(base_amount - reference_amount) < tolerance:
                clusters[cluster_index].append(index)
                matched = True
                break
        if not matched:
            clusters.append([index])
            cluster_amounts.append(base_amount)

    if len(clusters) == 1:
        if base_currency_rows:
            return base_currency_rows
        return list(grouped_rows)

    deduped: list[dict[str, Any]] = []
    for cluster in clusters:
        cluster_rows = [grouped_rows[index] for index in cluster]
        base_rows_in_cluster = [
            row
            for row in cluster_rows
            if _normalize_flex_currency(row.get("currency")) == normalized_base_currency
        ]
        if base_rows_in_cluster:
            deduped.extend(base_rows_in_cluster)
            continue
        deduped.extend(cluster_rows)
    return deduped


def normalize_flex_cash_income_trades(
    raw_cash_rows: Iterable[dict[str, Any]],
    base_currency: str = "USD",
) -> list[dict[str, Any]]:
    """Convert raw Flex CashTransaction dividend/interest rows to trade-format records."""
    raw_cash_rows_list = list(raw_cash_rows)
    _filter_lod = _should_filter_non_detail(raw_cash_rows_list)
    normalized: list[dict[str, Any]] = []

    for row in raw_cash_rows_list:
        trade_type = _income_trade_type_for_cash_type(str(row.get("type") or "").strip().upper())
        if not trade_type:
            continue

        lod = str(row.get("levelOfDetail") or "").strip().upper()
        if _filter_lod and lod and lod != "DETAIL":
            continue
        account_id = _normalize_identifier(row.get("accountId") or row.get("accountID"))
        if not lod and account_id == "-":
            logger.warning(
                "CashTransaction income row with accountId='-' missing levelOfDetail; "
                "keeping row but may be a summary duplicate: date=%s type=%s amount=%s",
                row.get("dateTime") or row.get("date"),
                row.get("type"),
                row.get("amount"),
            )

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
            symbol = "MARGIN_INTEREST" if trade_type == "INTEREST" else "UNKNOWN"

        account_name = _normalize_identifier(
            row.get("accountAlias")
            or row.get("acctAlias")
            or row.get("accountName")
        )
        transaction_id = _normalize_identifier(
            row.get("transactionID") or row.get("tradeID") or row.get("id")
        )
        if transaction_id is None:
            content = (
                f"income:{event_dt.date().isoformat()}:{symbol}:"
                f"{_normalize_flex_currency(row.get('currency'))}:{trade_type}:{amount:.8f}"
            )
            transaction_id = f"income:{hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]}"

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
                "instrument_type": "income",
                "account_id": account_id,
                "account_name": account_name,
                "_institution": "ibkr",
                "fx_rate_to_base": safe_float(row.get("fxRateToBase"), 0.0),
            }
        )

    # Pass 1: segment dedup — collapse identical events reported per IBKR segment.
    # Exclude account_id because IBKR reports the same event with different
    # accountId values across segments (e.g. "U2471778" vs "-").
    segment_grouped: dict[tuple[str, str, str, float, str], list[dict[str, Any]]] = {}
    for row in normalized:
        event_dt = _parse_flex_date(row.get("date"))
        segment_key = (
            event_dt.date().isoformat() if event_dt is not None else "",
            str(row.get("symbol") or "").strip().upper(),
            str(row.get("type") or "").strip().upper(),
            round(_parse_cash_amount(row.get("amount"), default=0.0), 8),
            _normalize_flex_currency(row.get("currency")),
        )
        segment_grouped.setdefault(segment_key, []).append(row)

    segment_deduped = [
        _select_dedup_winner(rows, synthetic_prefixes=("income:",))
        for rows in segment_grouped.values()
    ]

    # Pass 2: cross-currency dedup — same event in HKD and USD → keep base currency.
    normalized_base_currency = _normalize_flex_currency(base_currency)
    cross_currency_grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in segment_deduped:
        event_dt = _parse_flex_date(row.get("date"))
        currency_key = (
            event_dt.date().isoformat() if event_dt is not None else "",
            str(row.get("symbol") or "").strip().upper(),
            str(row.get("type") or "").strip().upper(),
        )
        cross_currency_grouped.setdefault(currency_key, []).append(row)

    normalized = []
    for grouped_rows in cross_currency_grouped.values():
        normalized.extend(_cross_currency_dedup(grouped_rows, normalized_base_currency))

    normalized.sort(
        key=lambda row: (
            _parse_flex_date(row.get("date")) or datetime.min,
            str(row.get("transaction_id") or ""),
            str(row.get("account_id") or ""),
        )
    )
    return normalized


def normalize_flex_futures_mtm(
    raw_stmtfunds_rows: Iterable[dict[str, Any]],
    base_currency: str = "USD",
) -> list[dict[str, Any]]:
    """Convert Flex StmtFunds futures cash-settlement rows (Position MTM + trade execution) to normalized events."""
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, float, str, str]] = set()
    row_group_keys: list[tuple[str, str, str]] = []

    for index, row in enumerate(raw_stmtfunds_rows):
        if not isinstance(row, dict):
            continue

        asset_category = str(row.get("assetCategory") or row.get("assetClass") or "").strip().upper()
        if asset_category != "FUT":
            continue

        description = str(row.get("activityDescription") or row.get("description") or "").strip()
        description_upper = description.upper()
        is_position_mtm = "POSITION MTM" in description_upper
        is_trade_execution = description_upper.startswith("BUY ") or description_upper.startswith("SELL ")
        if not is_position_mtm and not is_trade_execution:
            continue

        event_dt = _parse_flex_date(row.get("reportDate") or row.get("dateTime") or row.get("date"))
        if event_dt is None:
            continue

        amount = _parse_cash_amount(row.get("amount"), default=0.0)
        if amount == 0.0:
            continue

        currency = _normalize_flex_currency(row.get("currency"))
        account_id = _normalize_identifier(row.get("accountId") or row.get("accountID"))
        # Skip summary/consolidated rows (accountId="-") — these duplicate the
        # per-account rows and cause double-counting in cash replay.
        if account_id == "-":
            continue
        account_name = _normalize_identifier(
            row.get("accountAlias")
            or row.get("acctAlias")
            or row.get("accountName")
        )
        provider_account_ref = _normalize_identifier(row.get("accountAlias"))
        raw_symbol = str(row.get("symbol") or "").strip().upper()

        dedup_key = (
            account_id or "",
            event_dt.date().isoformat(),
            raw_symbol,
            round(amount, 8),
            currency,
            description[:20],
        )
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        symbol = _strip_futures_contract_month(raw_symbol)
        transaction_id = (
            f"stmtfunds_mtm:{account_id or account_name or 'unknown'}:{event_dt.date().isoformat()}:"
            f"{raw_symbol or symbol or 'UNKNOWN'}:{amount:.8f}:{currency}:{index}"
        )

        normalized.append(
            {
                "provider": "ibkr_flex",
                "institution": "ibkr",
                "_institution": "ibkr",
                "account_id": account_id,
                "account_name": account_name,
                "provider_account_ref": provider_account_ref,
                "date": event_dt,
                "symbol": symbol,
                "description": description,
                "amount": float(amount),
                "currency": currency,
                "transaction_id": transaction_id,
            }
        )
        row_group_keys.append((account_id or "", event_dt.date().isoformat(), raw_symbol))

    normalized_base_currency = _normalize_flex_currency(base_currency)
    grouped_indexes: dict[tuple[str, str, str], list[int]] = {}
    for row_index, group_key in enumerate(row_group_keys):
        grouped_indexes.setdefault(group_key, []).append(row_index)

    keep_indexes: set[int] = set(range(len(normalized)))
    for grouped_row_indexes in grouped_indexes.values():
        if len(grouped_row_indexes) <= 1:
            continue

        currencies = {str(normalized[idx].get("currency") or "").upper() for idx in grouped_row_indexes}
        if len(currencies) <= 1:
            continue

        base_currency_indexes = [
            idx
            for idx in grouped_row_indexes
            if str(normalized[idx].get("currency") or "").upper() == normalized_base_currency
        ]
        if not base_currency_indexes:
            continue

        for idx in grouped_row_indexes:
            if idx not in base_currency_indexes:
                keep_indexes.discard(idx)

    normalized = [row for idx, row in enumerate(normalized) if idx in keep_indexes]

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


def _normalize_cash_transaction_row(
    raw_row: dict[str, Any],
    row_index: int,
    *,
    filter_non_detail: bool = False,
) -> dict[str, Any]:
    amount = _parse_cash_amount(raw_row.get("amount"), default=0.0)
    raw_type = str(raw_row.get("type") or "").strip().upper()
    flow_type, is_external, signed_amount, transfer_cash_confirmed = _cash_classification(raw_type, amount)
    if not flow_type or signed_amount == 0.0:
        return {}

    event_dt = _parse_flex_date(raw_row.get("dateTime") or raw_row.get("date") or raw_row.get("reportDate"))
    if event_dt is None:
        return {}

    account_id = _normalize_identifier(raw_row.get("accountId") or raw_row.get("accountID"))
    lod = str(raw_row.get("levelOfDetail") or "").strip().upper()
    if filter_non_detail and lod and lod != "DETAIL":
        return {}
    if not lod and account_id == "-":
        logger.warning(
            "CashTransaction row with accountId='-' missing levelOfDetail; "
            "keeping row but may be a summary duplicate: date=%s type=%s amount=%s",
            raw_row.get("dateTime") or raw_row.get("date"),
            raw_row.get("type"),
            raw_row.get("amount"),
        )
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
        "fx_rate_to_base": safe_float(raw_row.get("fxRateToBase"), 0.0),
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
    base_currency: str = "USD",
) -> list[dict[str, Any]]:
    cash_rows_list = list(cash_rows)
    _filter_lod = _should_filter_non_detail(cash_rows_list)
    normalized: list[dict[str, Any]] = []
    primary_overlap_keys: set[tuple[str, str, str, str, str, float, str]] = set()

    for index, row in enumerate(cash_rows_list):
        normalized_row = _normalize_cash_transaction_row(row, index, filter_non_detail=_filter_lod)
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

    # Pass 1: segment dedup — exclude account_id (same event, different accountId per segment)
    segment_grouped: dict[tuple[str, str, str, float, str], list[dict[str, Any]]] = {}
    for row in normalized:
        event_dt = _parse_flex_date(row.get("event_datetime") or row.get("date"))
        segment_key = (
            event_dt.date().isoformat() if event_dt is not None else "",
            str(row.get("flow_type") or "").strip().lower(),
            str(row.get("raw_type") or "").strip().upper(),
            round(_parse_cash_amount(row.get("amount"), default=0.0), 8),
            _normalize_flex_currency(row.get("currency")),
        )
        segment_grouped.setdefault(segment_key, []).append(row)

    segment_deduped = [
        _select_dedup_winner(rows, synthetic_prefixes=("cash:", "transfer:"))
        for rows in segment_grouped.values()
    ]

    # Pass 2: cross-currency dedup — same event in HKD and USD → keep base currency
    normalized_base_currency = _normalize_flex_currency(base_currency)
    cross_currency_grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in segment_deduped:
        event_dt = _parse_flex_date(row.get("event_datetime") or row.get("date"))
        currency_key = (
            event_dt.date().isoformat() if event_dt is not None else "",
            str(row.get("flow_type") or "").strip().lower(),
            str(row.get("raw_type") or "").strip().upper(),
        )
        cross_currency_grouped.setdefault(currency_key, []).append(row)

    normalized = []
    for grouped_rows in cross_currency_grouped.values():
        normalized.extend(_cross_currency_dedup(grouped_rows, normalized_base_currency))

    normalized.sort(
        key=lambda row: (
            _parse_flex_date(row.get("event_datetime") or row.get("date")) or datetime.min,
            str(row.get("transaction_id") or ""),
            str(row.get("account_id") or ""),
        )
    )
    return normalized


def _redact_credentials(msg: str, token: str, query_id: str) -> str:
    """Replace token/query_id in error messages with ***."""
    if token and token in msg:
        msg = msg.replace(token, "***")
    if query_id and query_id in msg:
        msg = msg.replace(query_id, "***")
    return msg


def _check_flex_error_response_xml(root: Any) -> tuple[int | None, str | None]:
    """Check raw XML root element for IBKR error response."""
    if root is None:
        return None, None

    error_code_el = root.find("ErrorCode")
    if error_code_el is None:
        error_code_el = root.find(".//ErrorCode")
    if error_code_el is None:
        return None, None

    try:
        code = int(error_code_el.text)
    except (TypeError, ValueError):
        # Malformed ErrorCode (non-integer) - log warning, treat as no error.
        logger.warning(
            "IBKR Flex response contains non-integer ErrorCode: %r",
            getattr(error_code_el, "text", None),
        )
        return None, None

    message_el = root.find("ErrorMessage")
    if message_el is None:
        message_el = root.find(".//ErrorMessage")
    message = message_el.text if message_el is not None else f"IBKR Flex error {code}"
    return code, message


def _check_flex_error_response(report: Any) -> tuple[int | None, str | None]:
    """Check if a FlexReport contains an error response instead of data."""
    root = getattr(report, "root", None)
    return _check_flex_error_response_xml(root)


def _download_flex_report(
    token: str,
    query_id: str,
    *,
    budget_user_id: int | None = None,
    poll_interval: int = 3,
    poll_timeout: int = 60,
) -> tuple[Any | None, str | None]:
    """Download IBKR Flex report with correct Phase 2 polling.

    ib_async's FlexReport.download() has a bug: its Phase 2 poll loop checks
    root[0].tag == "code" but IBKR returns <Status> as root[0], so the loop
    exits after one 1-second poll. We implement both phases ourselves.

    Phase 1: POST token+queryId -> get reference code + URL
    Phase 2: Poll reference URL every poll_interval seconds until data or timeout
    """
    from urllib.parse import urlencode
    from urllib.request import urlopen
    import xml.etree.ElementTree as ET

    # --- Phase 1: Request statement generation ---
    base_url = os.getenv(
        "IB_FLEXREPORT_URL",
        "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?",
    )
    params = urlencode({"t": token, "q": query_id, "v": "3"})
    url = f"{base_url}{params}"

    try:
        resp = guard_ib_call(
            operation="flex_send_request",
            fn=urlopen,
            args=(url,),
            kwargs={"timeout": 30},
            budget_user_id=budget_user_id,
        )
        data = resp.read()
        root = ET.fromstring(data)
    except BudgetExceededError:
        raise
    except Exception as exc:
        exc_msg = _redact_credentials(str(exc), token, query_id)
        return None, f"IBKR Flex Phase 1 request failed: {exc_msg}"

    status_el = root.find("Status")
    if status_el is None or (status_el.text or "").strip() != "Success":
        error_code_el = root.find("ErrorCode")
        error_msg_el = root.find("ErrorMessage")
        code = error_code_el.text if error_code_el is not None else "?"
        msg = error_msg_el.text if error_msg_el is not None else "unknown"
        return None, f"IBKR Flex Phase 1 error {code}: {msg}"

    ref_code_el = root.find("ReferenceCode")
    ref_url_el = root.find("Url")
    if ref_code_el is None or ref_url_el is None:
        return None, "IBKR Flex Phase 1 missing ReferenceCode or Url"

    ref_code = (ref_code_el.text or "").strip()
    ref_url = (ref_url_el.text or "").strip()
    if not ref_code or not ref_url:
        return None, "IBKR Flex Phase 1 returned empty ReferenceCode or Url"

    # --- Phase 2: Poll for statement ---
    logger.info("IBKR Flex statement requested (ref %s), polling...", ref_code)
    elapsed = 0

    while elapsed < poll_timeout:
        time.sleep(poll_interval)
        elapsed += poll_interval

        poll_params = urlencode({"q": ref_code, "t": token, "v": "3"})
        poll_url = f"{ref_url}?{poll_params}"
        try:
            resp = guard_ib_call(
                operation="flex_get_statement",
                fn=urlopen,
                args=(poll_url,),
                kwargs={"timeout": 30},
                budget_user_id=budget_user_id,
            )
            poll_data = resp.read()
            poll_root = ET.fromstring(poll_data)
        except BudgetExceededError:
            raise
        except Exception as exc:
            exc_msg = _redact_credentials(str(exc), token, query_id)
            return None, f"IBKR Flex Phase 2 poll failed: {exc_msg}"

        error_code, error_msg = _check_flex_error_response_xml(poll_root)
        if error_code == 1019:
            logger.info(
                "IBKR Flex still generating (ref %s, %ds elapsed)...",
                ref_code,
                elapsed,
            )
            continue

        if error_code is not None:
            return None, f"IBKR Flex Phase 2 error {error_code}: {error_msg}"

        report = FlexReport()
        report.data = poll_data
        report.root = poll_root
        logger.info(
            "IBKR Flex statement retrieved (ref %s, %ds elapsed, %d bytes)",
            ref_code,
            elapsed,
            len(poll_data),
        )
        return report, None

    return None, (
        f"IBKR Flex statement generation timed out after {poll_timeout}s "
        f"(ref {ref_code})"
    )


def _report_xml_text(report: Any, *, path: str | None = None) -> str | None:
    if path:
        try:
            return Path(path).read_text(encoding="utf-8")
        except Exception:
            return None

    data = getattr(report, "data", None)
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace")
    if isinstance(data, str):
        return data

    root = getattr(report, "root", None)
    if root is None:
        return None

    import xml.etree.ElementTree as ET

    try:
        return ET.tostring(root, encoding="unicode")
    except Exception:
        return None


def _write_report_xml(raw_xml: str | None, save_xml_path: str | None) -> None:
    if not raw_xml or not save_xml_path:
        return

    target = Path(save_xml_path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(raw_xml, encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed to save IBKR Flex XML to %s: %s", save_xml_path, exc)


def _load_flex_report(
    *,
    token: str = "",
    query_id: str = "",
    path: Optional[str] = None,
    save_xml_path: str | None = None,
    budget_user_id: int | None = None,
) -> tuple[Any | None, str | None]:
    # --- Path loading ---
    if path:
        if not Path(path).exists():
            return None, f"IBKR Flex XML file not found: {path}"
        try:
            report = FlexReport(path=path)
        except Exception as exc:
            return None, f"Failed to load IBKR Flex XML from {path}: {exc}"

        # A saved XML file could contain an error response
        error_code, error_msg = _check_flex_error_response(report)
        if error_code is not None:
            return None, f"IBKR Flex error {error_code} in {path}: {error_msg}"
        _write_report_xml(_report_xml_text(report, path=path), save_xml_path)
        return report, None

    # --- Download ---
    if not token or not query_id:
        return None, "IBKR Flex credentials missing; skipping Flex download"
    report, load_error = _download_flex_report(
        token,
        query_id,
        budget_user_id=budget_user_id,
    )
    if report is not None:
        _write_report_xml(_report_xml_text(report), save_xml_path)
    return report, load_error


def fetch_ibkr_flex_payload(
    token: str = "",
    query_id: str = "",
    path: Optional[str] = None,
    *,
    budget_user_id: int | None = None,
    raw_xml: bool = False,
    save_xml_path: str | None = None,
) -> Dict[str, Any]:
    """Download/load IBKR Flex report and return trades + cash rows + MTM + diagnostics."""
    payload: Dict[str, Any] = {
        "trades": [],
        "trades_raw": [],
        "cash_rows": [],
        "futures_mtm": [],
        "option_price_history": [],
        "fetch_window_start": None,
        "fetch_window_end": None,
        "pagination_exhausted": False,
        "partial_data": True,
        "fetch_error": None,
        "cash_transaction_section_present": False,
        "transfer_section_present": False,
        "stmtfunds_section_present": False,
    }
    if raw_xml:
        payload["raw_xml"] = None

    report, load_error = _load_flex_report(
        token=token,
        query_id=query_id,
        path=path,
        save_xml_path=save_xml_path,
        budget_user_id=budget_user_id,
    )
    if load_error is not None:
        logger.warning("%s", load_error)
        payload["fetch_error"] = load_error
        return payload

    if raw_xml:
        payload["raw_xml"] = _report_xml_text(report, path=path)

    window_start, window_end = _report_window(report)
    payload["fetch_window_start"] = window_start
    payload["fetch_window_end"] = window_end

    topics = _report_topics(report)
    cash_topic_present = "CashTransaction" in topics
    transfer_topic_present = "Transfer" in topics
    stmtfunds_topic_present = "StatementOfFundsLine" in topics
    payload["cash_transaction_section_present"] = cash_topic_present
    payload["transfer_section_present"] = transfer_topic_present
    payload["stmtfunds_section_present"] = stmtfunds_topic_present

    raw_trades = _extract_rows(report, "Trade")
    payload["trades_raw"] = list(raw_trades)
    if raw_trades:
        payload["trades"] = normalize_flex_trades(raw_trades)

    raw_cash_rows = _extract_rows(report, "CashTransaction")
    raw_transfer_rows = _extract_rows(report, "Transfer")
    raw_stmtfunds_rows = _extract_rows(report, "StatementOfFundsLine") if stmtfunds_topic_present else []
    raw_prior_positions = _extract_rows(report, "PriorPeriodPosition")
    payload["cash_rows"] = normalize_flex_cash_rows(raw_cash_rows, raw_transfer_rows)
    payload["trades"].extend(normalize_flex_cash_income_trades(raw_cash_rows))
    payload["futures_mtm"] = normalize_flex_futures_mtm(raw_stmtfunds_rows)
    payload["option_price_history"] = normalize_flex_prior_positions(raw_prior_positions)

    partial_messages: list[str] = []
    if not cash_topic_present:
        partial_messages.append("CashTransaction section missing in Flex report")
    if not cash_topic_present and not transfer_topic_present:
        logger.warning(
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


def fetch_flex_report(
    *,
    path: str | None = None,
    token: str | None = None,
    query_id: str | None = None,
    budget_user_id: int | None = None,
    raw_xml: bool = False,
    save_xml_path: str | None = None,
) -> Dict[str, Any]:
    """Canonical public wrapper for fetching parsed IBKR Flex payloads."""

    payload_kwargs: dict[str, Any] = {
        "token": token or "",
        "query_id": query_id or "",
        "path": path,
        "raw_xml": raw_xml,
        "save_xml_path": save_xml_path,
    }
    if budget_user_id is not None:
        payload_kwargs["budget_user_id"] = budget_user_id
    return fetch_ibkr_flex_payload(**payload_kwargs)


def fetch_ibkr_flex_trades(
    token: str = "",
    query_id: str = "",
    path: Optional[str] = None,
    *,
    budget_user_id: int | None = None,
) -> List[Dict[str, Any]]:
    """Download/load IBKR Flex trades and normalize to FIFO transaction format."""
    if path and not Path(path).exists():
        raise FileNotFoundError(f"IBKR Flex XML file not found: {path}")
    payload_kwargs: dict[str, Any] = {
        "token": token,
        "query_id": query_id,
        "path": path,
    }
    if budget_user_id is not None:
        payload_kwargs["budget_user_id"] = budget_user_id
    payload = fetch_ibkr_flex_payload(**payload_kwargs)
    return list(payload.get("trades") or [])


def extract_statement_cash(
    statement_db_path: str,
) -> dict[str, Any] | None:
    """Extract starting/ending cash from materialized IBKR statement SQLite.

    Searches for any table matching cash_report% and extracts the Base
    Currency Summary Starting/Ending Cash rows. When available, also extracts
    statement period start/end dates from a statement% table. Returns None if
    the file doesn't exist, the table is missing, or required rows are absent.
    """
    if not statement_db_path or not Path(statement_db_path).exists():
        return None
    import sqlite3

    conn = sqlite3.connect(statement_db_path)
    conn.row_factory = sqlite3.Row
    try:
        # Find the cash_report table (name varies across statement formats)
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name LIKE 'cash_report%' ORDER BY name"
            ).fetchall()
        ]
        if not tables:
            return None
        # Prefer __all (union) if it exists, else first variant
        table = next((t for t in tables if t.endswith("__all")), tables[0])
        rows = conn.execute(
            f"SELECT currency_summary, total FROM [{table}] "
            "WHERE currency = 'Base Currency Summary' "
            "AND currency_summary IN ('Starting Cash', 'Ending Cash')"
        ).fetchall()
        result: dict[str, Any] = {}
        for row in rows:
            label = str(row["currency_summary"]).strip()
            try:
                value = float(row["total"])
            except (TypeError, ValueError):
                continue
            if label == "Starting Cash":
                result["starting_cash_usd"] = value
            elif label == "Ending Cash":
                result["ending_cash_usd"] = value

        if "starting_cash_usd" not in result or "ending_cash_usd" not in result:
            return None

        try:
            stmt_tables = [
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name LIKE 'statement%' ORDER BY name"
                ).fetchall()
            ]
            if stmt_tables:
                stmt_table = next((t for t in stmt_tables if t.endswith("__all")), stmt_tables[0])
                stmt_rows = conn.execute(
                    f"SELECT field_value FROM [{stmt_table}] "
                    "WHERE field_name = 'Period'"
                ).fetchall()
                if stmt_rows:
                    period_str = str(stmt_rows[0][0]).strip()
                    parts = period_str.split(" - ")
                    if len(parts) == 2:
                        from dateutil.parser import parse as parse_date

                        result["period_start"] = parse_date(parts[0]).strftime("%Y-%m-%d")
                        result["period_end"] = parse_date(parts[1]).strftime("%Y-%m-%d")
        except Exception:
            pass

        result["source"] = "ibkr_statement"
        result["db_path"] = str(statement_db_path)
        return result
    except Exception:
        return None
    finally:
        conn.close()
