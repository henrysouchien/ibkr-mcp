#!/usr/bin/env python3
"""IBKR MCP server exposing IBKR-only tools."""

from __future__ import annotations

# CRITICAL: Redirect stdout to stderr BEFORE any imports
# MCP uses stdout for JSON-RPC - all other output (logs, prints) must go to stderr
import sys

_real_stdout = sys.stdout
sys.stdout = sys.stderr

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Optional

from dotenv import load_dotenv
from fastmcp import FastMCP

_pkg_dir = Path(__file__).resolve().parent
load_dotenv(_pkg_dir / ".env", override=True)
load_dotenv(_pkg_dir.parent / ".env", override=True)

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


@mcp.tool()
def get_ibkr_market_data(
    symbols: list[str],
    instrument_type: Literal["futures", "fx", "bond", "option"],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    what_to_show: Optional[str] = None,
    contract_identity: Optional[dict] = None,
) -> dict:
    """Fetch historical price series from IBKR Gateway."""

    def _impl() -> dict:
        from .client import IBKRClient

        client = IBKRClient()
        end_dt = end_date or datetime.now().strftime("%Y-%m-%d")
        start_dt = start_date or (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")

        results: dict[str, Any] = {}
        for sym in symbols:
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
        return {"status": "error", "error": str(exc)}


@mcp.tool()
def get_ibkr_positions(
    include_pnl: bool = False,
    account_id: Optional[str] = None,
) -> dict:
    """Fetch current IBKR positions and optionally account-level PnL."""

    def _impl() -> dict:
        from .client import IBKRClient

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
        return {"status": "error", "error": str(exc)}


@mcp.tool()
def get_ibkr_account(account_id: Optional[str] = None) -> dict:
    """Fetch IBKR account summary metrics."""

    def _impl() -> dict:
        from .client import IBKRClient

        client = IBKRClient()
        summary = client.get_account_summary(account_id=account_id)
        return {"status": "success", "account_summary": summary}

    try:
        return _with_stderr_stdout(_impl)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@mcp.tool()
def get_ibkr_contract(
    symbol: str,
    sec_type: str = "STK",
    info_type: Literal["details", "option_chain"] = "details",
    exchange: str = "SMART",
    currency: str = "USD",
) -> dict:
    """Fetch contract details or option chain metadata from IBKR."""

    def _impl() -> dict:
        from .client import IBKRClient

        client = IBKRClient()
        if info_type == "option_chain":
            chain = client.get_option_chain(symbol=symbol.upper(), sec_type=sec_type, exchange=exchange)
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
        return {"status": "error", "error": str(exc)}


@mcp.tool()
def get_ibkr_option_prices(
    symbol: str,
    expiry: str,
    strikes: list[float],
    right: str = "P",
) -> dict:
    """Snapshot bid/ask/greeks for multiple option strikes."""

    def _impl() -> dict:
        from ib_async import Option

        from .client import IBKRClient

        normalized_symbol = str(symbol or "").strip().upper()
        normalized_right = str(right or "").strip().upper()
        if normalized_right not in {"P", "C"}:
            raise ValueError("right must be 'P' or 'C'")

        client = IBKRClient()
        contracts = [
            Option(normalized_symbol, expiry, float(strike), normalized_right, "SMART")
            for strike in strikes
        ]
        snapshots = client.fetch_snapshot(contracts=contracts)

        prices: dict[float, dict[str, Any]] = {}
        for idx, strike in enumerate(strikes):
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
        return {"status": "error", "error": str(exc)}


@mcp.tool()
def get_ibkr_snapshot(
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
) -> dict:
    """Snapshot latest price for any security."""

    def _impl() -> dict:
        from ib_async import Contract, Stock

        from .client import IBKRClient

        normalized_symbol = str(symbol or "").strip().upper()
        normalized_sec_type = str(sec_type or "").strip().upper()

        if normalized_sec_type == "STK":
            contract = Stock(normalized_symbol, exchange, currency)
        else:
            contract = Contract(
                symbol=normalized_symbol,
                secType=normalized_sec_type,
                exchange=exchange,
                currency=currency,
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
        return {"status": "error", "error": str(exc)}


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
