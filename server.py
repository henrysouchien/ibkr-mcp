#!/usr/bin/env python3
"""IBKR MCP server exposing IBKR-only tools."""

from __future__ import annotations

# CRITICAL: Redirect stdout to stderr BEFORE any imports
# MCP uses stdout for JSON-RPC - all other output (logs, prints) must go to stderr
import sys

_real_stdout = sys.stdout
sys.stdout = sys.stderr

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Optional

from fastmcp import FastMCP

try:
    import nest_asyncio
    nest_asyncio.apply()
except Exception:
    pass

# Restore stdout for MCP transport.
sys.stdout = _real_stdout

mcp = FastMCP(
    "ibkr-mcp",
    instructions="IBKR market/account/contract tools for Interactive Brokers Gateway",
)


def _with_stderr_stdout(fn, *args, **kwargs):
    saved = sys.stdout
    sys.stdout = sys.stderr
    try:
        return fn(*args, **kwargs)
    finally:
        sys.stdout = saved


def _error_str(exc: Exception) -> str:
    """Format exception with type name fallback for empty messages."""
    msg = str(exc)
    return msg if msg else type(exc).__name__


def parse_list(value: Any, *, coerce=str) -> list | None:
    """Parse MCP list params that may arrive as JSON or comma-separated strings."""
    if value is None:
        return None
    if isinstance(value, list):
        return [coerce(v) for v in value]

    text = str(value).strip()
    if not text:
        return None

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None
    else:
        if isinstance(parsed, list):
            return [coerce(v) for v in parsed]
        raise ValueError(f"Expected JSON array, got {type(parsed).__name__}")

    return [coerce(v.strip()) for v in text.split(",") if v.strip()]


def parse_json_list(value: Any) -> list | None:
    """Parse list-of-dict MCP params that may arrive as JSON strings."""
    if value is None:
        return None
    if isinstance(value, list):
        return value

    text = str(value).strip()
    if not text:
        return None

    parsed = json.loads(text)
    if not isinstance(parsed, list):
        raise ValueError(f"Expected JSON array, got {type(parsed).__name__}")
    return parsed


@mcp.tool()
def get_ibkr_market_data(
    symbols: str,
    instrument_type: Literal["futures", "fx", "bond", "option"],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    what_to_show: Optional[str] = None,
    contract_identity: Optional[dict] = None,
) -> dict:
    """Fetch historical price series from IBKR Gateway.

    symbols accepts a JSON array string or comma-separated symbols.
    Related IBKR tools: get_ibkr_contract resolves contract metadata first;
    get_ibkr_snapshot fetches latest quotes; get_ibkr_account and
    get_ibkr_positions inspect account state.
    """

    def _impl() -> dict:
        from brokerage.ibkr import IBKRClient

        parsed_symbols = parse_list(symbols) or []
        if not parsed_symbols:
            raise ValueError("symbols is required")

        client = IBKRClient()
        end_dt = end_date or datetime.now().strftime("%Y-%m-%d")
        start_dt = start_date or (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")

        results: dict[str, Any] = {}
        for sym in parsed_symbols:
            series = client.fetch_series(
                symbol=sym.upper(),
                instrument_type=instrument_type,
                start_date=start_dt,
                end_date=end_dt,
                what_to_show=what_to_show,
                contract_identity=contract_identity,
            )
            if series.empty:
                results[sym.upper()] = {"bars": 0, "data": {}}
            else:
                results[sym.upper()] = {
                    "bars": len(series),
                    "start": str(series.index.min().date()),
                    "end": str(series.index.max().date()),
                    "data": {str(k.date()): round(v, 6) for k, v in series.items()},
                }

        return {"status": "success", "instrument_type": instrument_type, "results": results}

    try:
        return _with_stderr_stdout(_impl)
    except Exception as exc:
        return {"status": "error", "error": _error_str(exc)}


@mcp.tool()
def get_ibkr_positions(
    include_pnl: bool = False,
    account_id: Optional[str] = None,
) -> dict:
    """Fetch current IBKR positions and optionally account-level PnL.

    Related IBKR tools: get_ibkr_account summarizes account metrics,
    get_ibkr_market_data fetches historical prices, and get_ibkr_contract
    resolves contract metadata.
    """

    def _impl() -> dict:
        from brokerage.ibkr import IBKRClient

        client = IBKRClient()
        positions_df = client.get_positions(account_id=account_id)
        positions = positions_df.to_dict(orient="records") if not positions_df.empty else []

        result: dict[str, Any] = {
            "status": "success",
            "count": len(positions),
            "positions": positions,
        }
        if include_pnl:
            result["pnl"] = client.get_pnl(account_id=account_id)
        return result

    try:
        return _with_stderr_stdout(_impl)
    except Exception as exc:
        return {"status": "error", "error": _error_str(exc)}


@mcp.tool()
def get_ibkr_account(account_id: Optional[str] = None) -> dict:
    """Fetch IBKR account summary metrics.

    Related IBKR tools: get_ibkr_positions lists holdings,
    get_ibkr_contract resolves securities, and get_ibkr_market_data fetches
    historical price series.
    """

    def _impl() -> dict:
        from brokerage.ibkr import IBKRClient

        client = IBKRClient()
        summary = client.get_account_summary(account_id=account_id)
        return {"status": "success", "account_summary": summary}

    try:
        return _with_stderr_stdout(_impl)
    except Exception as exc:
        return {"status": "error", "error": _error_str(exc)}


@mcp.tool()
def get_ibkr_contract(
    symbol: str,
    currency: str,
    sec_type: str = "STK",
    info_type: Literal["details", "option_chain"] = "details",
    exchange: str = "SMART",
) -> dict:
    """Fetch contract details or option chain metadata from IBKR.

    Related IBKR tools: get_ibkr_market_data and get_ibkr_snapshot use
    resolved contract fields for pricing; get_ibkr_option_prices snapshots
    option strikes.
    """

    def _impl() -> dict:
        from brokerage.ibkr import IBKRClient

        client = IBKRClient()
        if info_type == "option_chain":
            chain = client.get_option_chain(
                symbol=symbol.upper(),
                currency=currency,
                sec_type=sec_type,
                exchange=exchange,
            )
            return {"status": "success", "info_type": "option_chain", "chain": chain}

        details = client.get_contract_details(
            symbol=symbol.upper(),
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
        )
        return {"status": "success", "info_type": "details", "contracts": details}

    try:
        return _with_stderr_stdout(_impl)
    except Exception as exc:
        return {"status": "error", "error": _error_str(exc)}


@mcp.tool()
def get_ibkr_option_prices(
    symbol: str,
    expiry: str,
    strikes: str,
    currency: str,
    right: str = "P",
) -> dict:
    """Snapshot bid/ask/greeks for multiple option strikes.

    strikes accepts a JSON array string or comma-separated values.
    Related IBKR tools: get_ibkr_contract discovers option-chain metadata,
    get_ibkr_market_data fetches historical series, and get_ibkr_snapshot
    fetches latest single-contract quotes.
    """

    def _impl() -> dict:
        from brokerage.ibkr import IBKRClient, IBKRContractSpec

        normalized_symbol = str(symbol or "").strip().upper()
        normalized_right = str(right or "").strip().upper()
        if normalized_right not in {"P", "C"}:
            raise ValueError("right must be 'P' or 'C'")

        parsed_strikes = parse_list(strikes, coerce=float) or []
        if not parsed_strikes:
            raise ValueError("strikes is required")

        client = IBKRClient()
        contracts = [
            IBKRContractSpec.option(
                normalized_symbol,
                expiry=expiry,
                strike=float(strike),
                right=normalized_right,
                currency=currency,
            )
            for strike in parsed_strikes
        ]
        snapshots = client.fetch_snapshot(contracts=contracts)

        prices: dict[float, dict[str, Any]] = {}
        for idx, strike in enumerate(parsed_strikes):
            strike_key = float(strike)
            prices[strike_key] = snapshots[idx] if idx < len(snapshots) else {"error": "timeout"}

        return {
            "status": "success",
            "symbol": normalized_symbol,
            "expiry": expiry,
            "right": normalized_right,
            "prices": prices,
        }

    try:
        return _with_stderr_stdout(_impl)
    except Exception as exc:
        return {"status": "error", "error": _error_str(exc)}


@mcp.tool()
def get_ibkr_snapshot(
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
) -> dict:
    """Snapshot latest price for a stock or futures contract.

    Related IBKR tools: get_ibkr_contract resolves contract metadata first,
    get_ibkr_market_data fetches historical prices, get_ibkr_option_prices
    fetches option quotes, and get_ibkr_account checks Gateway account state.
    """

    def _impl() -> dict:
        from brokerage.ibkr import IBKRClient, IBKRContractSpec

        normalized_symbol = str(symbol or "").strip().upper()
        normalized_sec_type = str(sec_type or "").strip().upper()

        if normalized_sec_type == "STK":
            contract = IBKRContractSpec.stock(
                normalized_symbol,
                exchange=exchange,
                currency=currency,
            )
        elif normalized_sec_type == "FUT":
            contract = IBKRContractSpec.future(
                normalized_symbol,
                exchange=exchange,
                currency=currency,
            )
        else:
            raise ValueError(
                "get_ibkr_snapshot supports sec_type 'STK' or 'FUT'; "
                "use get_ibkr_option_prices for options"
            )

        client = IBKRClient()
        snapshots = client.fetch_snapshot(contracts=[contract])
        snapshot = snapshots[0] if snapshots else {"error": "timeout"}
        if "error" in snapshot:
            return {"status": "error", "error": snapshot["error"]}
        return {
            "status": "success",
            "symbol": normalized_symbol,
            "sec_type": normalized_sec_type,
            "snapshot": snapshot,
        }

    try:
        return _with_stderr_stdout(_impl)
    except Exception as exc:
        return {"status": "error", "error": _error_str(exc)}


@mcp.tool()
def get_ibkr_status() -> dict:
    """Return IBKR Gateway connection status for diagnostics.

    Related IBKR tools: get_ibkr_account and get_ibkr_positions validate
    account access; get_ibkr_market_data validates market-data access.
    """

    def _impl() -> dict:
        from brokerage.ibkr import IBKRClient

        client = IBKRClient()
        return {"status": "success", **client.get_connection_status()}

    try:
        return _with_stderr_stdout(_impl)
    except Exception as exc:
        return {"status": "error", "error": _error_str(exc)}


def _kill_previous_instance() -> None:
    """Kill previous ibkr-mcp instance spawned by the same parent session."""
    import os
    import signal
    import tempfile

    server_dir = Path(tempfile.gettempdir()) / "ibkr-mcp"
    server_dir.mkdir(exist_ok=True)
    ppid = os.getppid()
    pid_file = server_dir / f".ibkr_mcp_server_{ppid}.pid"
    if pid_file.exists():
        try:
            old_pid = int(pid_file.read_text().strip())
            if old_pid != os.getpid():
                os.kill(old_pid, signal.SIGTERM)
        except (ValueError, ProcessLookupError, PermissionError):
            pass

    pid_file.write_text(str(os.getpid()))

    for stale in server_dir.glob(".ibkr_mcp_server_*.pid"):
        if stale == pid_file:
            continue
        try:
            session_pid = int(stale.stem.split("_")[-1])
            os.kill(session_pid, 0)
        except (ValueError, ProcessLookupError):
            stale.unlink(missing_ok=True)
        except PermissionError:
            pass


def main() -> None:
    _kill_previous_instance()
    mcp.run()


if __name__ == "__main__":
    main()
