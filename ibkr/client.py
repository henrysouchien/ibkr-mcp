"""Unified IBKR API facade with smart routing.

Agent orientation:
    Canonical object interface for IBKR operations used by higher layers.
    This facade routes account/metadata calls through the persistent connection
    manager and market-data calls through ``IBKRMarketDataClient``.
"""

from __future__ import annotations

import importlib
from dataclasses import asdict
from typing import Any

import pandas as pd

from .config import IBKR_AUTHORIZED_ACCOUNTS
from .account import fetch_account_summary, fetch_pnl, fetch_pnl_single, fetch_positions
from .capabilities import get_capability, list_capabilities
from .connection import IBKRConnectionManager
from .exceptions import IBKRAccountError
from .locks import ibkr_shared_lock
from .market_data import IBKRMarketDataClient
from .metadata import fetch_contract_details, fetch_option_chain


class IBKRClient:
    """Unified IBKR API client.

    - Account and metadata requests use persistent IBKRConnectionManager
    - Market data requests delegate to IBKRMarketDataClient

    Primary flow:
    1) Resolve/ensure IB gateway connection.
    2) Resolve account identity constraints when needed.
    3) Delegate to account, metadata, or market-data helper modules.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        client_id: int | None = None,
    ) -> None:
        self._market_data = IBKRMarketDataClient(host=host, port=port, client_id=client_id)
        self._conn_manager = IBKRConnectionManager()

    def _get_account_ib(self):
        """Get the shared IB connection for account/metadata operations."""
        return self._conn_manager.ensure_connected()

    def _resolve_account_id(self, ib, account_id: str | None = None) -> str:
        """Resolve account_id with authorization filtering and ambiguity checks."""
        accounts = list(ib.managedAccounts() or [])
        authorized_accounts = list(IBKR_AUTHORIZED_ACCOUNTS)
        try:
            settings_module = importlib.import_module("settings")
            configured_accounts = getattr(settings_module, "IBKR_AUTHORIZED_ACCOUNTS", None)
            if isinstance(configured_accounts, list):
                authorized_accounts = [str(account) for account in configured_accounts if str(account).strip()]
        except Exception:
            pass

        if authorized_accounts:
            accounts = [a for a in accounts if a in authorized_accounts]

        if account_id:
            if account_id not in accounts:
                raise IBKRAccountError(f"Account {account_id} not authorized or not found")
            return account_id

        if len(accounts) == 1:
            return accounts[0]
        if len(accounts) == 0:
            raise IBKRAccountError("No IBKR accounts available")

        raise IBKRAccountError(
            f"Multiple accounts available ({', '.join(accounts)}); specify --account"
        )

    def fetch_series(self, **kwargs) -> pd.Series:
        return self._market_data.fetch_series(**kwargs)

    def fetch_monthly_close_futures(self, **kwargs) -> pd.Series:
        return self._market_data.fetch_monthly_close_futures(**kwargs)

    def fetch_daily_close_futures(self, **kwargs) -> pd.Series:
        return self._market_data.fetch_daily_close_futures(**kwargs)

    def fetch_monthly_close_fx(self, **kwargs) -> pd.Series:
        return self._market_data.fetch_monthly_close_fx(**kwargs)

    def fetch_monthly_close_bond(self, **kwargs) -> pd.Series:
        return self._market_data.fetch_monthly_close_bond(**kwargs)

    def fetch_monthly_close_option(self, **kwargs) -> pd.Series:
        return self._market_data.fetch_monthly_close_option(**kwargs)

    def fetch_snapshot(self, **kwargs) -> list[dict[str, Any]]:
        return self._market_data.fetch_snapshot(**kwargs)

    def get_positions(self, account_id: str | None = None) -> pd.DataFrame:
        with ibkr_shared_lock:
            ib = self._get_account_ib()
            resolved_account = self._resolve_account_id(ib, account_id)
            return fetch_positions(ib, account_id=resolved_account)

    def get_account_summary(self, account_id: str | None = None) -> dict[str, float]:
        with ibkr_shared_lock:
            ib = self._get_account_ib()
            resolved_account = self._resolve_account_id(ib, account_id)
            return fetch_account_summary(ib, account_id=resolved_account)

    def get_pnl(self, account_id: str | None = None) -> dict[str, Any]:
        with ibkr_shared_lock:
            ib = self._get_account_ib()
            resolved_account = self._resolve_account_id(ib, account_id)
            return fetch_pnl(ib, account_id=resolved_account)

    def get_pnl_single(self, account_id: str | None, con_id: int) -> dict[str, Any]:
        with ibkr_shared_lock:
            ib = self._get_account_ib()
            resolved_account = self._resolve_account_id(ib, account_id)
            return fetch_pnl_single(ib, account_id=resolved_account, con_id=int(con_id))

    def get_contract_details(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> list[dict[str, Any]]:
        with ibkr_shared_lock:
            ib = self._get_account_ib()
            return fetch_contract_details(
                ib,
                symbol=symbol,
                sec_type=sec_type,
                exchange=exchange,
                currency=currency,
            )

    def get_option_chain(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
    ) -> dict[str, Any]:
        with ibkr_shared_lock:
            ib = self._get_account_ib()
            return fetch_option_chain(ib, symbol=symbol, sec_type=sec_type, exchange=exchange)

    def list_capabilities(self, category: str | None = None) -> list[dict[str, Any]]:
        return [asdict(cap) for cap in list_capabilities(category=category)]

    def describe(self, capability_name: str) -> dict[str, Any]:
        cap = get_capability(capability_name)
        if cap is None:
            raise KeyError(f"Unknown capability: {capability_name}")
        return asdict(cap)
