"""IBKR market data client.

This client uses a dedicated read-only IB connection and a module-level lock to
preserve event-loop/thread safety for `ib_async` calls.

Agent orientation:
    Source of truth for IBKR historical series fetch behavior, including
    contract resolution, whatToShow fallback chains, and on-disk caching.
"""

from __future__ import annotations

import logging
import os
import threading
import time
import math
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pandas as pd
from ibkr._shared.budget_exceptions import BudgetExceededError

from ._logging import log_event, logger, TimingContext
from ._budget import guard_ib_call
from .config import (
    IBKR_CLIENT_ID,
    IBKR_CONNECT_MAX_ATTEMPTS,
    IBKR_FUTURES_CURVE_TIMEOUT,
    IBKR_GATEWAY_HOST,
    IBKR_GATEWAY_PORT,
    IBKR_MARKET_DATA_RETRY_DELAY,
    IBKR_OPTION_SNAPSHOT_TIMEOUT,
    IBKR_SNAPSHOT_POLL_INTERVAL,
    IBKR_SNAPSHOT_TIMEOUT,
    IBKR_TIMEOUT,
)

from .cache import get_cached, put_cache
from .contract_spec import IBKRContractSpec
from .contracts import resolve_contract, resolve_futures_contract, resolve_option_contract
from .exceptions import (
    IBKRConnectionError,
    IBKRContractError,
    IBKRDataError,
    IBKREntitlementError,
    IBKRNoDataError,
)
from .profiles import InstrumentProfile, get_profile
from .locks import ibkr_shared_lock
from .metadata import fetch_futures_months
from .asyncio_compat import apply_nest_asyncio_if_running_loop

if TYPE_CHECKING:
    from ib_async import Contract

apply_nest_asyncio_if_running_loop()


_ibkr_request_lock = threading.Lock()


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _compute_duration_str(start_dt: datetime, end_dt: datetime) -> str:
    """Compute IBKR duration string rounded up to full years."""
    start_date = pd.Timestamp(start_dt).date()
    end_date = pd.Timestamp(end_dt).date()
    if end_date <= start_date:
        return "1 Y"

    years = end_date.year - start_date.year
    anniversary = (pd.Timestamp(start_date) + pd.DateOffset(years=years)).date()
    if end_date > anniversary:
        years += 1
    years = max(1, years)
    return f"{years} Y"


def _bar_attr(bar: Any, attr: str) -> Any:
    if isinstance(bar, dict):
        return bar.get(attr)
    return getattr(bar, attr, None)


class IBKRMarketDataClient:
    """Client for IBKR historical market data with per-request connection lifecycle.

    Upstream reference:
    - IBKR historical bars: https://interactivebrokers.github.io/tws-api/historical_bars.html
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        client_id: int | None = None,
    ):
        configured_client_id = os.getenv("IBKR_MARKET_DATA_CLIENT_ID")
        market_data_client_id = int(configured_client_id) if configured_client_id else (IBKR_CLIENT_ID + 1)

        self.host = host or IBKR_GATEWAY_HOST
        self.port = int(port or IBKR_GATEWAY_PORT)
        self.client_id = int(client_id if client_id is not None else market_data_client_id)
        self.timeout = int(IBKR_TIMEOUT)

    def _connect_ib(self, *, budget_user_id: int | None = None):
        from .asyncio_compat import ensure_event_loop

        ensure_event_loop()
        from ib_async import IB

        ib = IB()
        try:
            guard_ib_call(
                operation="connect",
                fn=ib.connect,
                kwargs={
                    "host": self.host,
                    "port": self.port,
                    "clientId": self.client_id,
                    "timeout": self.timeout,
                    "readonly": True,
                },
                budget_user_id=budget_user_id,
            )
            return ib
        except BudgetExceededError:
            raise
        except ConnectionRefusedError as exc:
            raise IBKRConnectionError("IB Gateway not running") from exc
        except Exception as exc:
            raise IBKRConnectionError(f"IBKR connect failed: {exc}") from exc

    def _duration_for_request(
        self,
        profile: InstrumentProfile,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> str:
        # Always compute from date range to avoid silent truncation.
        # Futures use now() as end to capture the latest continuous contract data.
        if profile.instrument_type == "futures":
            return _compute_duration_str(start_ts.to_pydatetime(), _utcnow())
        return _compute_duration_str(start_ts.to_pydatetime(), end_ts.to_pydatetime())

    def _coerce_snapshot_contract(self, contract: Any):
        """Accept raw contracts or lightweight boundary contract specs."""
        if isinstance(contract, IBKRContractSpec):
            return self._resolve_spec(contract)

        if isinstance(contract, dict):
            contract = type("IBKRContractSpec", (), contract)()

        if not getattr(contract, "_ibkr_contract_spec", False):
            return contract

        sec_type = str(
            getattr(contract, "secType", None)
            or getattr(contract, "instrument_type", None)
            or ""
        ).strip().upper()
        symbol = str(getattr(contract, "symbol", "") or "").strip().upper()
        exchange = str(getattr(contract, "exchange", "SMART") or "SMART").strip().upper()
        currency = str(getattr(contract, "currency", "USD") or "USD").strip().upper()

        if sec_type == "OPT":
            contract_identity = {
                "symbol": symbol,
                "expiry": getattr(contract, "lastTradeDateOrContractMonth", None),
                "strike": getattr(contract, "strike", None),
                "right": getattr(contract, "right", None),
                "exchange": exchange,
                "currency": currency,
                "multiplier": getattr(contract, "multiplier", None),
                "con_id": getattr(contract, "conId", None),
            }
            return resolve_option_contract(symbol, contract_identity=contract_identity)

        if sec_type == "FUT":
            contract_month = getattr(contract, "lastTradeDateOrContractMonth", None)
            if exchange == "SMART":
                return resolve_futures_contract(symbol, contract_month=contract_month)

            from ib_async import Future

            kwargs: dict[str, Any] = {
                "symbol": symbol,
                "exchange": exchange,
                "currency": currency,
            }
            if contract_month:
                kwargs["lastTradeDateOrContractMonth"] = str(contract_month)
            return Future(**kwargs)

        if sec_type == "STK":
            from ib_async import Stock

            return Stock(symbol, exchange, currency)

        raise IBKRContractError(f"Unsupported snapshot contract spec secType '{sec_type or 'unknown'}'")

    def _resolve_spec(self, contract: IBKRContractSpec):
        sec_type = str(contract.sec_type or "").strip().upper()
        symbol = str(contract.symbol or "").strip().upper()
        exchange = str(contract.exchange or "SMART").strip().upper()
        currency = str(contract.currency or "USD").strip().upper()

        if sec_type == "OPT":
            contract_identity = {
                "symbol": symbol,
                "expiry": contract.expiry,
                "strike": contract.strike,
                "right": contract.right,
                "exchange": exchange,
                "currency": currency,
                "multiplier": contract.multiplier,
                "con_id": contract.con_id,
            }
            return resolve_option_contract(symbol, contract_identity=contract_identity)

        if sec_type == "FUT":
            contract_month = contract.contract_month
            if exchange == "SMART":
                return resolve_futures_contract(symbol, contract_month=contract_month)

            from ib_async import Future

            kwargs: dict[str, Any] = {
                "symbol": symbol,
                "exchange": exchange,
                "currency": currency,
            }
            if contract_month:
                kwargs["lastTradeDateOrContractMonth"] = str(contract_month)
            return Future(**kwargs)

        if sec_type == "STK":
            from ib_async import Stock

            return Stock(symbol, exchange, currency)

        raise IBKRContractError(f"Unsupported snapshot contract spec secType '{sec_type or 'unknown'}'")

    def _qualify_contract(self, ib, contract, *, budget_user_id: int | None = None):
        """Qualify a contract, with CUSIP fallback for bonds.

        IBKR's qualifyContracts() does not support secIdType=CUSIP for bonds.
        When qualification fails for a CUSIP bond, search via reqContractDetails
        to resolve the CUSIP to a conId, then retry qualification.
        """
        # First attempt - may return None or raise for CUSIP bonds
        try:
            qualified = guard_ib_call(
                operation="qualifyContracts",
                fn=ib.qualifyContracts,
                args=(contract,),
                budget_user_id=budget_user_id,
            )
            qualified_contract = next(
                (row for row in (qualified or []) if row is not None and getattr(row, "conId", None)),
                None,
            )
            if qualified_contract is not None:
                return qualified_contract
        except BudgetExceededError:
            raise
        except Exception:
            # qualifyContracts raised (e.g. "no security definition")
            qualified_contract = None

        # CUSIP fallback: search via reqContractDetails
        if getattr(contract, "secIdType", "") == "CUSIP":
            from .metadata import resolve_bond_by_cusip

            if budget_user_id is None:
                resolved_con_id = resolve_bond_by_cusip(
                    ib,
                    getattr(contract, "secId", ""),
                    getattr(contract, "currency", "USD"),
                )
            else:
                resolved_con_id = resolve_bond_by_cusip(
                    ib,
                    getattr(contract, "secId", ""),
                    getattr(contract, "currency", "USD"),
                    budget_user_id=budget_user_id,
                )
            if resolved_con_id:
                from ib_async import Bond

                retry_contract = Bond(conId=resolved_con_id)
                qualified = guard_ib_call(
                    operation="qualifyContracts",
                    fn=ib.qualifyContracts,
                    args=(retry_contract,),
                    budget_user_id=budget_user_id,
                )
                qualified_contract = next(
                    (row for row in (qualified or []) if row is not None and getattr(row, "conId", None)),
                    None,
                )
                if qualified_contract is not None:
                    return qualified_contract

        return None

    def _request_bars(
        self,
        contract: Any,
        *,
        budget_user_id: int | None = None,
        profile: InstrumentProfile,
        what_to_show: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> list[Any]:
        with _ibkr_request_lock:
            last_connect_exc: Exception | None = None
            for attempt in range(1, IBKR_CONNECT_MAX_ATTEMPTS + 1):
                if attempt > 1:
                    time.sleep(IBKR_MARKET_DATA_RETRY_DELAY)
                    log_event(
                        logger, logging.INFO, "bars.retry",
                        attempt=attempt, max=IBKR_CONNECT_MAX_ATTEMPTS,
                    )
                try:
                    if budget_user_id is None:
                        ib = self._connect_ib()
                    else:
                        ib = self._connect_ib(budget_user_id=budget_user_id)
                    last_connect_exc = None
                    break
                except IBKRConnectionError as exc:
                    last_connect_exc = exc
                    if attempt == IBKR_CONNECT_MAX_ATTEMPTS:
                        raise
            try:
                if budget_user_id is None:
                    qualified_contract = self._qualify_contract(ib, contract)
                else:
                    qualified_contract = self._qualify_contract(
                        ib,
                        contract,
                        budget_user_id=budget_user_id,
                    )
                if qualified_contract is None:
                    raise IBKRContractError("Unable to qualify IBKR contract")

                duration_str = self._duration_for_request(profile, start_ts, end_ts)
                bars = guard_ib_call(
                    operation="reqHistoricalData",
                    fn=ib.reqHistoricalData,
                    args=(qualified_contract,),
                    kwargs={
                        "endDateTime": "",
                        "durationStr": duration_str,
                        "barSizeSetting": profile.bar_size,
                        "whatToShow": what_to_show,
                        "useRTH": profile.use_rth,
                        "formatDate": 1,
                    },
                    budget_user_id=budget_user_id,
                )
                if not bars:
                    raise IBKRNoDataError("No historical bars returned")
                return list(bars)
            except IBKRDataError:
                raise
            except BudgetExceededError:
                raise
            except Exception as exc:
                text = str(exc).lower()
                if "entitlement" in text or "market data permissions" in text or "permission" in text:
                    raise IBKREntitlementError(str(exc)) from exc
                if (
                    "no security definition" in text
                    or "unknown contract" in text
                    or "includeexpired" in text
                ):
                    raise IBKRContractError(str(exc)) from exc
                raise IBKRDataError(str(exc)) from exc
            finally:
                try:
                    ib.disconnect()
                except Exception:
                    pass

    def _normalize_bars(
        self,
        symbol: str,
        bars: list[Any],
        *,
        bar_size: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.Series:
        rows: list[tuple[pd.Timestamp, float]] = []
        for bar in bars:
            date_val = _bar_attr(bar, "date")
            close_val = _bar_attr(bar, "close")
            if date_val is None or close_val is None:
                continue
            ts = pd.to_datetime(date_val, errors="coerce")
            if pd.isna(ts):
                continue
            try:
                rows.append((pd.Timestamp(ts), float(close_val)))
            except (TypeError, ValueError):
                continue

        if not rows:
            return pd.Series(dtype=float)

        frame = pd.DataFrame(rows, columns=["date", "value"]).set_index("date").sort_index()
        series = frame["value"].astype(float)
        series = series[~series.index.duplicated(keep="last")]

        if "month" in bar_size.lower():
            series = series.resample("ME").last()

        series = series[(series.index >= start_ts) & (series.index <= end_ts)]
        series = series.dropna()
        series.name = symbol
        return series

    def fetch_series(
        self,
        symbol: str,
        instrument_type: str,
        start_date: Any,
        end_date: Any,
        profile: InstrumentProfile | None = None,
        what_to_show: str | None = None,
        contract_identity: dict[str, Any] | None = None,
        raise_on_transient: bool = False,
        *,
        budget_user_id: int | None = None,
    ) -> pd.Series:
        """Fetch historical IBKR series with profile-based fallback chain.

        Contract notes:
        - Returns empty ``pd.Series`` on recoverable failures (invalid input,
          no profile, contract resolution failure, no data).
        - Uses ``ibkr.profiles`` fallback chain unless ``what_to_show`` override
          is explicitly provided.
        """
        sym = str(symbol or "").strip().upper()
        if not sym:
            return pd.Series(dtype=float)

        try:
            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date)
        except Exception:
            logger.warning("Invalid IBKR date range for %s", sym)
            return pd.Series(dtype=float)
        if start_ts > end_ts:
            logger.warning(
                "Invalid IBKR date range for %s: start %s after end %s",
                sym,
                start_ts.date(),
                end_ts.date(),
            )
            return pd.Series(dtype=float)

        try:
            resolved_profile = profile or get_profile(instrument_type)
        except Exception as exc:
            logger.warning("No IBKR profile for %s (%s): %s", sym, instrument_type, exc)
            return pd.Series(dtype=float)

        chain = [what_to_show.strip().upper()] if what_to_show else list(resolved_profile.what_to_show_chain)
        if not chain:
            return pd.Series(dtype=float)

        try:
            contract = resolve_contract(
                sym,
                resolved_profile.instrument_type,
                contract_identity=contract_identity,
            )
        except IBKRContractError as exc:
            logger.warning("IBKR contract resolution failed for %s: %s", sym, exc)
            return pd.Series(dtype=float)

        for candidate in chain:
            cached = get_cached(
                symbol=sym,
                instrument_type=resolved_profile.instrument_type,
                what_to_show=candidate,
                bar_size=resolved_profile.bar_size,
                use_rth=resolved_profile.use_rth,
                start_date=start_ts,
                end_date=end_ts,
                contract_identity=contract_identity,
            )
            if cached is not None and not cached.empty:
                cached.name = sym
                return cached

        _last_transient: Exception | None = None
        for candidate in chain:
            try:
                request_kwargs = {
                    "profile": resolved_profile,
                    "what_to_show": candidate,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                }
                if budget_user_id is not None:
                    request_kwargs["budget_user_id"] = budget_user_id
                bars = self._request_bars(contract, **request_kwargs)
                series = self._normalize_bars(
                    sym,
                    bars,
                    bar_size=resolved_profile.bar_size,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
                if series.empty:
                    continue
                put_cache(
                    series,
                    symbol=sym,
                    instrument_type=resolved_profile.instrument_type,
                    what_to_show=candidate,
                    bar_size=resolved_profile.bar_size,
                    use_rth=resolved_profile.use_rth,
                    start_date=start_ts,
                    end_date=end_ts,
                    contract_identity=contract_identity,
                )
                return series
            except IBKRNoDataError:
                continue
            except IBKREntitlementError as exc:
                logger.warning("IBKR entitlement issue for %s (%s): %s", sym, candidate, exc)
                _last_transient = exc
                continue
            except IBKRContractError as exc:
                logger.warning("IBKR contract invalid for %s (%s): %s", sym, candidate, exc)
                return pd.Series(dtype=float)
            except IBKRConnectionError as exc:
                logger.info("IB Gateway not running; IBKR fallback unavailable for %s", sym)
                _last_transient = exc
                if not raise_on_transient:
                    return pd.Series(dtype=float)
                continue
            except IBKRDataError as exc:
                logger.warning("IBKR data error for %s (%s): %s", sym, candidate, exc)
                _last_transient = exc
                continue
            except Exception as exc:
                logger.warning("IBKR historical data failed for %s (%s): %s", sym, candidate, exc)
                continue

        if raise_on_transient and _last_transient is not None:
            raise _last_transient
        return pd.Series(dtype=float)

    def fetch_monthly_close_futures(
        self,
        symbol: str,
        start_date: Any,
        end_date: Any,
        *,
        budget_user_id: int | None = None,
    ) -> pd.Series:
        """Convenience wrapper for futures month-end close series."""
        profile = get_profile("futures")
        series = self.fetch_series(
            symbol=symbol,
            instrument_type="futures",
            start_date=start_date,
            end_date=end_date,
            profile=profile,
            budget_user_id=budget_user_id,
        )
        if series.empty:
            return series
        return self._to_monthly_close(
            series=series,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def fetch_daily_close_futures(
        self,
        symbol: str,
        start_date: Any,
        end_date: Any,
        raise_on_transient: bool = False,
        *,
        budget_user_id: int | None = None,
    ) -> pd.Series:
        """Convenience wrapper for futures daily close series."""
        profile = get_profile("futures_daily")
        return self.fetch_series(
            symbol=symbol,
            instrument_type="futures",
            start_date=start_date,
            end_date=end_date,
            profile=profile,
            raise_on_transient=raise_on_transient,
            budget_user_id=budget_user_id,
        )

    def fetch_daily_close_fx(
        self,
        symbol: str,
        start_date: Any,
        end_date: Any,
        raise_on_transient: bool = False,
        *,
        budget_user_id: int | None = None,
    ) -> pd.Series:
        """Daily FX close series (no month-end resample)."""
        profile = get_profile("fx")
        return self.fetch_series(
            symbol=symbol,
            instrument_type="fx",
            start_date=start_date,
            end_date=end_date,
            profile=profile,
            raise_on_transient=raise_on_transient,
            budget_user_id=budget_user_id,
        )

    def fetch_daily_close_bond(
        self,
        symbol: str,
        start_date: Any,
        end_date: Any,
        contract_identity: dict[str, Any] | None = None,
        raise_on_transient: bool = False,
        *,
        budget_user_id: int | None = None,
    ) -> pd.Series:
        """Daily bond close series (no month-end resample)."""
        profile = get_profile("bond")
        return self.fetch_series(
            symbol=symbol,
            instrument_type="bond",
            start_date=start_date,
            end_date=end_date,
            profile=profile,
            contract_identity=contract_identity,
            raise_on_transient=raise_on_transient,
            budget_user_id=budget_user_id,
        )

    def fetch_monthly_close_fx(
        self,
        symbol: str,
        start_date: Any,
        end_date: Any,
        *,
        budget_user_id: int | None = None,
    ) -> pd.Series:
        """Convenience wrapper for FX month-end close series from daily bars."""
        profile = get_profile("fx")
        series = self.fetch_series(
            symbol=symbol,
            instrument_type="fx",
            start_date=start_date,
            end_date=end_date,
            profile=profile,
            budget_user_id=budget_user_id,
        )
        if series.empty:
            return series
        return self._to_monthly_close(
            series=series,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def fetch_monthly_close_bond(
        self,
        symbol: str,
        start_date: Any,
        end_date: Any,
        contract_identity: dict[str, Any] | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> pd.Series:
        """Convenience wrapper for bond month-end close series from daily bars."""
        profile = get_profile("bond")
        series = self.fetch_series(
            symbol=symbol,
            instrument_type="bond",
            start_date=start_date,
            end_date=end_date,
            profile=profile,
            contract_identity=contract_identity,
            budget_user_id=budget_user_id,
        )
        if series.empty:
            return series
        return self._to_monthly_close(
            series=series,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def fetch_monthly_close_option(
        self,
        symbol: str,
        start_date: Any,
        end_date: Any,
        contract_identity: dict[str, Any] | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> pd.Series:
        """Stub option month-end mark series from daily bars."""
        profile = get_profile("option")
        series = self.fetch_series(
            symbol=symbol,
            instrument_type="option",
            start_date=start_date,
            end_date=end_date,
            profile=profile,
            contract_identity=contract_identity,
            budget_user_id=budget_user_id,
        )
        if series.empty:
            return series
        return self._to_monthly_close(
            series=series,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def _to_monthly_close(
        self,
        *,
        series: pd.Series,
        symbol: str,
        start_date: Any,
        end_date: Any,
    ) -> pd.Series:
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        monthly = series.resample("ME").last()
        monthly = monthly[(monthly.index >= start_ts) & (monthly.index <= end_ts)].dropna()
        monthly.name = str(symbol or "").strip().upper()
        return monthly

    @staticmethod
    def _as_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            out = float(value)
        except (TypeError, ValueError):
            return None
        if math.isnan(out) or math.isinf(out):
            return None
        return out

    @classmethod
    def _as_int(cls, value: Any) -> int | None:
        out = cls._as_float(value)
        if out is None:
            return None
        return int(out)

    @classmethod
    def _value_for_option_side(
        cls,
        ticker: Any,
        *,
        right: str,
        call_attr: str,
        put_attr: str,
    ) -> int | None:
        prefer_put = right == "P"
        primary = put_attr if prefer_put else call_attr
        secondary = call_attr if prefer_put else put_attr
        primary_value = cls._as_int(getattr(ticker, primary, None))
        if primary_value is not None:
            return primary_value
        return cls._as_int(getattr(ticker, secondary, None))

    def fetch_snapshot(
        self,
        contracts: list[IBKRContractSpec | Any],
        timeout: float = IBKR_SNAPSHOT_TIMEOUT,
        option_timeout: float = IBKR_OPTION_SNAPSHOT_TIMEOUT,
        *,
        budget_user_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Snapshot current bid/ask/last/volume/greeks for one or more contracts.

        Uses reqMktData(snapshot=True). Returns one dict per contract with:
        - bid, ask, last, mid (computed)
        - volume, open_interest
        - For options: implied_vol, delta, gamma, theta, vega (from modelGreeks)

        *option_timeout* is used instead of *timeout* when the batch contains
        any OPT contracts (IBKR computes model Greeks server-side, which takes
        longer than stock snapshots).
        """
        if not contracts:
            return []

        timeout_seconds = max(0.0, float(timeout))
        option_timeout_seconds = max(0.0, float(option_timeout))
        pre_errors: dict[int, str] = {}
        qualified_by_index: dict[int, Any] = {}
        tickers_by_index: dict[int, Any] = {}

        # IBKR error codes that indicate a market data subscription problem.
        _SUBSCRIPTION_ERROR_CODES = {
            354,    # Requested market data is not subscribed
            10090,  # Market data not subscribed
            10167,  # Delayed market data not subscribed
            10189,  # No market data permissions
        }

        # Track subscription errors by reqId during snapshot polling.
        subscription_errors: dict[int, str] = {}  # reqId → error message

        def _on_error(reqId: int, errorCode: int, errorString: str, *_extra: Any) -> None:
            if errorCode in _SUBSCRIPTION_ERROR_CODES:
                subscription_errors[reqId] = errorString

        with ibkr_shared_lock:
            ib = None
            try:
                if budget_user_id is None:
                    ib = self._connect_ib()
                else:
                    ib = self._connect_ib(budget_user_id=budget_user_id)

                for idx, contract in enumerate(contracts):
                    try:
                        qualified = guard_ib_call(
                            operation="qualifyContracts",
                            fn=ib.qualifyContracts,
                            args=(self._coerce_snapshot_contract(contract),),
                            budget_user_id=budget_user_id,
                        )
                        if not qualified:
                            pre_errors[idx] = "unable to qualify contract"
                            continue
                        qualified_contract = next((row for row in qualified if row is not None), None)
                        if qualified_contract is None:
                            pre_errors[idx] = "unable to qualify contract"
                            continue
                        qualified_by_index[idx] = qualified_contract
                    except BudgetExceededError:
                        raise
                    except Exception as exc:
                        pre_errors[idx] = str(exc) or "qualification failed"

                # Detect if any qualified contract is an option — use longer
                # timeout so IBKR has time to compute model Greeks.
                has_options = any(
                    str(getattr(c, "secType", "") or "").upper() == "OPT"
                    for c in qualified_by_index.values()
                )
                effective_timeout = option_timeout_seconds if has_options else timeout_seconds

                # For options, snapshot=True often returns nan — use streaming
                # mode instead and poll for data, then cancel subscriptions.
                use_streaming = has_options

                # Register error handler to capture subscription failures.
                ib.errorEvent += _on_error

                for idx, qualified_contract in qualified_by_index.items():
                    sec_type = str(getattr(qualified_contract, "secType", "") or "").upper()
                    generic_ticks = "100,101,106" if sec_type == "OPT" else ""
                    try:
                        tickers_by_index[idx] = guard_ib_call(
                            operation="reqMktData",
                            fn=ib.reqMktData,
                            args=(qualified_contract,),
                            kwargs={
                                "genericTickList": generic_ticks,
                                "snapshot": not use_streaming,
                                "regulatorySnapshot": False,
                                "mktDataOptions": [],
                            },
                            budget_user_id=budget_user_id,
                        )
                    except BudgetExceededError:
                        raise
                    except Exception as exc:
                        pre_errors[idx] = str(exc) or "snapshot request failed"

                # Build reverse map: reqId → contract index.
                reqid_to_idx: dict[int, int] = {}
                for idx, ticker in tickers_by_index.items():
                    req_id = getattr(ib.wrapper, "ticker2ReqId", {}).get(ticker)
                    if req_id is not None:
                        reqid_to_idx[req_id] = idx

                if use_streaming and tickers_by_index:
                    # Poll until all tickers have price data + Greeks or timeout.
                    poll_interval = IBKR_SNAPSHOT_POLL_INTERVAL
                    elapsed = 0.0
                    while elapsed < effective_timeout:
                        ib.sleep(poll_interval)
                        elapsed += poll_interval

                        # Propagate subscription errors to pre_errors so we
                        # stop waiting for tickers that will never get data.
                        for req_id, msg in list(subscription_errors.items()):
                            cidx = reqid_to_idx.get(req_id)
                            if cidx is not None and cidx not in pre_errors:
                                pre_errors[cidx] = f"not_subscribed: {msg}"
                                tickers_by_index.pop(cidx, None)
                                log_event(
                                    logger, logging.WARNING,
                                    "snapshot.not_subscribed",
                                    idx=cidx, error_code_msg=msg,
                                )

                        if not tickers_by_index:
                            # All remaining tickers failed with subscription errors.
                            break

                        all_ready = True
                        for idx, ticker in tickers_by_index.items():
                            bid = self._as_float(getattr(ticker, "bid", None))
                            ask = self._as_float(getattr(ticker, "ask", None))
                            last = self._as_float(getattr(ticker, "last", None))
                            if bid is None and ask is None and last is None:
                                all_ready = False
                                break
                            # For options, also wait for modelGreeks.
                            contract = qualified_by_index.get(idx)
                            sec_type = str(getattr(contract, "secType", "") or "").upper()
                            if sec_type == "OPT" and getattr(ticker, "modelGreeks", None) is None:
                                all_ready = False
                                break
                        if all_ready:
                            log_event(
                                logger, logging.DEBUG, "snapshot.ok",
                                count=len(tickers_by_index),
                                elapsed_ms=round(elapsed * 1000, 1),
                            )
                            break
                    else:
                        if tickers_by_index:
                            log_event(
                                logger, logging.WARNING, "snapshot.timeout",
                                count=len(tickers_by_index),
                                elapsed_s=round(elapsed, 1),
                            )
                    # Cancel streaming subscriptions.
                    for ticker in tickers_by_index.values():
                        try:
                            guard_ib_call(
                                operation="cancelMktData",
                                fn=ib.cancelMktData,
                                args=((getattr(ticker, "contract", None) or ticker),),
                                budget_user_id=budget_user_id,
                            )
                        except BudgetExceededError:
                            raise
                        except Exception:
                            pass
                elif effective_timeout > 0:
                    ib.sleep(effective_timeout)

                    # Check for subscription errors after non-streaming sleep too.
                    for req_id, msg in subscription_errors.items():
                        cidx = reqid_to_idx.get(req_id)
                        if cidx is not None and cidx not in pre_errors:
                            pre_errors[cidx] = f"not_subscribed: {msg}"
            except IBKRConnectionError as exc:
                return [{"error": str(exc)} for _ in contracts]
            except BudgetExceededError:
                raise
            except Exception as exc:
                return [{"error": str(exc) or "snapshot request failed"} for _ in contracts]
            finally:
                if ib is not None:
                    try:
                        ib.errorEvent -= _on_error
                    except Exception:
                        pass
                    try:
                        ib.disconnect()
                    except Exception:
                        pass

        output: list[dict[str, Any]] = []
        for idx, contract in enumerate(contracts):
            if idx in pre_errors:
                output.append({"error": pre_errors[idx]})
                continue

            ticker = tickers_by_index.get(idx)
            if ticker is None:
                output.append({"error": "timeout"})
                continue

            bid = self._as_float(getattr(ticker, "bid", None))
            ask = self._as_float(getattr(ticker, "ask", None))
            last = self._as_float(getattr(ticker, "last", None))
            mid = (bid + ask) / 2.0 if bid is not None and ask is not None else None

            contract_for_fields = qualified_by_index.get(idx, contract)
            sec_type = str(getattr(contract_for_fields, "secType", "") or "").upper()
            right = str(getattr(contract_for_fields, "right", "") or "").upper()

            if sec_type == "OPT":
                volume = self._value_for_option_side(
                    ticker,
                    right=right,
                    call_attr="callVolume",
                    put_attr="putVolume",
                )
                open_interest = self._value_for_option_side(
                    ticker,
                    right=right,
                    call_attr="callOpenInterest",
                    put_attr="putOpenInterest",
                )
            else:
                volume = self._as_int(getattr(ticker, "volume", None))
                open_interest = None

            model_greeks = getattr(ticker, "modelGreeks", None)
            implied_vol = self._as_float(getattr(model_greeks, "impliedVol", None))
            if implied_vol is None:
                implied_vol = self._as_float(getattr(ticker, "impliedVolatility", None))
            delta = self._as_float(getattr(model_greeks, "delta", None))
            gamma = self._as_float(getattr(model_greeks, "gamma", None))
            theta = self._as_float(getattr(model_greeks, "theta", None))
            vega = self._as_float(getattr(model_greeks, "vega", None))

            close = self._as_float(getattr(ticker, "close", None))

            has_data = any(
                value is not None
                for value in (bid, ask, last, close, volume, open_interest, implied_vol, delta, gamma, theta, vega)
            )
            received_time = getattr(ticker, "time", None)
            if not has_data and received_time is None:
                output.append({"error": "timeout"})
                continue

            output.append(
                {
                    "bid": bid,
                    "ask": ask,
                    "last": last,
                    "close": close,
                    "mid": mid,
                    "volume": volume,
                    "open_interest": open_interest,
                    "implied_vol": implied_vol,
                    "delta": delta,
                    "gamma": gamma,
                    "theta": theta,
                    "vega": vega,
                }
            )

        return output

    def fetch_futures_curve_snapshot(
        self,
        symbol: str,
        timeout: float = IBKR_FUTURES_CURVE_TIMEOUT,
        *,
        budget_user_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Snapshot last/bid/ask/volume for all active monthly contracts.

        Returns list of dicts with:
        con_id, last_trade_date, last, bid, ask, volume, open_interest
        sorted by last_trade_date ascending.
        """
        from ib_async import Contract

        sym = str(symbol or "").strip().upper()
        if not sym:
            raise IBKRContractError("Symbol is required")

        with ibkr_shared_lock:
            ib = None
            try:
                if budget_user_id is None:
                    ib = self._connect_ib()
                else:
                    ib = self._connect_ib(budget_user_id=budget_user_id)
                logger.info(
                    "futures_curve: connected to IBKR (host=%s port=%s client_id=%s)",
                    self.host, self.port, self.client_id,
                )
                if budget_user_id is None:
                    months = fetch_futures_months(ib, sym)
                else:
                    months = fetch_futures_months(ib, sym, budget_user_id=budget_user_id)
                logger.info(
                    "futures_curve: fetch_futures_months returned %d months for %s",
                    len(months), sym,
                )
            except Exception as exc:
                logger.warning(
                    "futures_curve: fetch_futures_months failed for %s: %s",
                    sym, exc,
                )
                raise
            finally:
                if ib is not None:
                    try:
                        ib.disconnect()
                    except Exception:
                        pass

        if not months:
            return []

        contracts: list[Contract] = []
        months_by_con_id: dict[int, dict[str, Any]] = {}
        for month in months:
            con_id_raw = month.get("con_id")
            if con_id_raw is None:
                continue
            try:
                con_id = int(con_id_raw)
            except (TypeError, ValueError):
                continue

            contracts.append(Contract(conId=con_id))
            months_by_con_id[con_id] = {
                "con_id": con_id,
                "last_trade_date": month.get("last_trade_date"),
            }

        if not contracts:
            logger.warning(
                "futures_curve: %d months found but no valid con_ids for %s",
                len(months), sym,
            )
            return []

        logger.info(
            "futures_curve: fetching snapshots for %d contracts (%s)",
            len(contracts), sym,
        )
        if budget_user_id is None:
            snapshots = self.fetch_snapshot(contracts, timeout=timeout)
        else:
            snapshots = self.fetch_snapshot(
                contracts,
                timeout=timeout,
                budget_user_id=budget_user_id,
            )
        logger.info(
            "futures_curve: fetch_snapshot returned %d results for %s",
            len(snapshots), sym,
        )

        merged: list[dict[str, Any]] = []
        skipped_error = 0
        skipped_no_price = 0
        skipped_no_meta = 0
        for idx, snapshot in enumerate(snapshots):
            if not isinstance(snapshot, dict) or "error" in snapshot:
                skipped_error += 1
                if idx < 3:
                    logger.debug(
                        "futures_curve: snapshot[%d] error: %s", idx, snapshot
                    )
                continue
            if idx >= len(contracts):
                continue

            con_id = int(getattr(contracts[idx], "conId", 0) or 0)
            month_meta = months_by_con_id.get(con_id)
            if month_meta is None:
                skipped_no_meta += 1
                continue

            last = self._as_float(snapshot.get("last"))
            close = self._as_float(snapshot.get("close"))
            price = last if last is not None else close
            if price is None:
                skipped_no_price += 1
                continue

            merged.append(
                {
                    "con_id": con_id,
                    "last_trade_date": month_meta.get("last_trade_date"),
                    "last": price,
                    "bid": self._as_float(snapshot.get("bid")),
                    "ask": self._as_float(snapshot.get("ask")),
                    "close": close,
                    "volume": self._as_int(snapshot.get("volume")),
                    "open_interest": self._as_int(snapshot.get("open_interest")),
                }
            )

        if skipped_error or skipped_no_price or skipped_no_meta:
            logger.info(
                "futures_curve: %s merged=%d skipped_error=%d skipped_no_price=%d skipped_no_meta=%d",
                sym, len(merged), skipped_error, skipped_no_price, skipped_no_meta,
            )

        merged.sort(key=lambda row: str(row.get("last_trade_date") or ""))
        return merged
