"""Unified IBKR API facade with smart routing.

Agent orientation:
    Canonical object interface for IBKR operations used by higher layers.
    This facade routes account/metadata calls through the connection manager
    (ephemeral or persistent by mode) and market-data calls through
    ``IBKRMarketDataClient``.
"""

from __future__ import annotations

import importlib
from dataclasses import asdict
from typing import Any

import pandas as pd

from .config import IBKR_AUTHORIZED_ACCOUNTS, IBKR_FUTURES_CURVE_TIMEOUT
from .contract_spec import IBKRContractSpec
from .account import (
    fetch_account_summary,
    fetch_pnl,
    fetch_pnl_single,
    fetch_portfolio_with_cash,
    fetch_positions,
)
from .capabilities import get_capability, list_capabilities
from .connection import IBKRConnectionManager
from .exceptions import IBKRAccountError
from .locks import ibkr_shared_lock
from .market_data import IBKRMarketDataClient
from .metadata import fetch_contract_details, fetch_option_chain


class IBKRClient:
    """Unified IBKR API client.

    - Account and metadata requests use IBKRConnectionManager
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
        *,
        budget_user_id: int | None = None,
    ) -> None:
        self._market_data = IBKRMarketDataClient(host=host, port=port, client_id=client_id)
        self._conn_manager = IBKRConnectionManager()
        self._budget_user_id = budget_user_id

    def _effective_budget_user_id(self, budget_user_id: int | None) -> int | None:
        if budget_user_id is not None:
            return budget_user_id
        return self._budget_user_id

    @staticmethod
    def _budget_kwargs(budget_user_id: int | None) -> dict[str, int]:
        if budget_user_id is None:
            return {}
        return {"budget_user_id": budget_user_id}

    def get_connection_status(self, *, budget_user_id: int | None = None) -> dict[str, Any]:
        """Return diagnostic dict describing current connection state."""
        with ibkr_shared_lock:
            return self._conn_manager.get_connection_status(
                **self._budget_kwargs(self._effective_budget_user_id(budget_user_id))
            )

    def _resolve_account_id(self, ib, account_id: str | None = None) -> str:
        """Resolve account_id with authorization filtering and ambiguity checks."""
        authorized_accounts = list(IBKR_AUTHORIZED_ACCOUNTS)
        try:
            settings_module = importlib.import_module("settings")
            configured_accounts = getattr(settings_module, "IBKR_AUTHORIZED_ACCOUNTS", None)
            if isinstance(configured_accounts, list):
                authorized_accounts = [str(account) for account in configured_accounts if str(account).strip()]
        except Exception:
            pass

        if account_id:
            normalized_account_id = str(account_id).strip()
            if authorized_accounts and normalized_account_id in authorized_accounts:
                return normalized_account_id

        accounts = list(ib.managedAccounts() or [])

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

    def fetch_series(self, *, budget_user_id: int | None = None, **kwargs) -> pd.Series:
        effective_budget_user_id = self._effective_budget_user_id(
            budget_user_id if budget_user_id is not None else kwargs.pop("budget_user_id", None)
        )
        return self._market_data.fetch_series(
            **kwargs,
            **self._budget_kwargs(effective_budget_user_id),
        )

    def fetch_monthly_close_futures(self, *, budget_user_id: int | None = None, **kwargs) -> pd.Series:
        effective_budget_user_id = self._effective_budget_user_id(
            budget_user_id if budget_user_id is not None else kwargs.pop("budget_user_id", None)
        )
        return self._market_data.fetch_monthly_close_futures(
            **kwargs,
            **self._budget_kwargs(effective_budget_user_id),
        )

    def fetch_daily_close_futures(self, *, budget_user_id: int | None = None, **kwargs) -> pd.Series:
        effective_budget_user_id = self._effective_budget_user_id(
            budget_user_id if budget_user_id is not None else kwargs.pop("budget_user_id", None)
        )
        return self._market_data.fetch_daily_close_futures(
            **kwargs,
            **self._budget_kwargs(effective_budget_user_id),
        )

    def fetch_monthly_close_fx(self, *, budget_user_id: int | None = None, **kwargs) -> pd.Series:
        effective_budget_user_id = self._effective_budget_user_id(
            budget_user_id if budget_user_id is not None else kwargs.pop("budget_user_id", None)
        )
        return self._market_data.fetch_monthly_close_fx(
            **kwargs,
            **self._budget_kwargs(effective_budget_user_id),
        )

    def fetch_monthly_close_bond(self, *, budget_user_id: int | None = None, **kwargs) -> pd.Series:
        effective_budget_user_id = self._effective_budget_user_id(
            budget_user_id if budget_user_id is not None else kwargs.pop("budget_user_id", None)
        )
        return self._market_data.fetch_monthly_close_bond(
            **kwargs,
            **self._budget_kwargs(effective_budget_user_id),
        )

    def fetch_monthly_close_option(self, *, budget_user_id: int | None = None, **kwargs) -> pd.Series:
        effective_budget_user_id = self._effective_budget_user_id(
            budget_user_id if budget_user_id is not None else kwargs.pop("budget_user_id", None)
        )
        return self._market_data.fetch_monthly_close_option(
            **kwargs,
            **self._budget_kwargs(effective_budget_user_id),
        )

    def fetch_snapshot(
        self,
        contracts: list[IBKRContractSpec | Any],
        *,
        budget_user_id: int | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        effective_budget_user_id = self._effective_budget_user_id(
            budget_user_id if budget_user_id is not None else kwargs.pop("budget_user_id", None)
        )
        return self._market_data.fetch_snapshot(
            contracts=contracts,
            **kwargs,
            **self._budget_kwargs(effective_budget_user_id),
        )

    def get_futures_curve_snapshot(
        self,
        symbol: str,
        timeout: float = IBKR_FUTURES_CURVE_TIMEOUT,
        *,
        budget_user_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Snapshot prices for all active contract months."""
        return self._market_data.fetch_futures_curve_snapshot(
            symbol,
            timeout=timeout,
            **self._budget_kwargs(self._effective_budget_user_id(budget_user_id)),
        )

    def get_positions(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> pd.DataFrame:
        effective_budget_user_id = self._effective_budget_user_id(budget_user_id)
        with ibkr_shared_lock:
            with self._conn_manager.connection(**self._budget_kwargs(effective_budget_user_id)) as ib:
                resolved_account = self._resolve_account_id(ib, account_id)
                return fetch_positions(
                    ib,
                    account_id=resolved_account,
                    **self._budget_kwargs(effective_budget_user_id),
                )

    def get_portfolio_with_cash(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> tuple[pd.DataFrame, dict[str, float]]:
        """Fetch portfolio items and cash balances in one connection session."""
        effective_budget_user_id = self._effective_budget_user_id(budget_user_id)
        with ibkr_shared_lock:
            with self._conn_manager.connection(**self._budget_kwargs(effective_budget_user_id)) as ib:
                resolved_account = self._resolve_account_id(ib, account_id)
                return fetch_portfolio_with_cash(
                    ib,
                    account_id=resolved_account,
                    **self._budget_kwargs(effective_budget_user_id),
                )

    def get_managed_accounts(self, *, budget_user_id: int | None = None) -> list[str]:
        """Return managed account IDs discovered from Gateway."""
        effective_budget_user_id = self._effective_budget_user_id(budget_user_id)
        with ibkr_shared_lock:
            with self._conn_manager.connection(**self._budget_kwargs(effective_budget_user_id)) as ib:
                return list(ib.managedAccounts() or [])

    def get_account_summary(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
        ) -> dict[str, float]:
        effective_budget_user_id = self._effective_budget_user_id(budget_user_id)
        with ibkr_shared_lock:
            with self._conn_manager.connection(**self._budget_kwargs(effective_budget_user_id)) as ib:
                resolved_account = self._resolve_account_id(ib, account_id)
                return fetch_account_summary(
                    ib,
                    account_id=resolved_account,
                    **self._budget_kwargs(effective_budget_user_id),
                )

    def get_pnl(
        self,
        account_id: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> dict[str, Any]:
        effective_budget_user_id = self._effective_budget_user_id(budget_user_id)
        with ibkr_shared_lock:
            with self._conn_manager.connection(**self._budget_kwargs(effective_budget_user_id)) as ib:
                resolved_account = self._resolve_account_id(ib, account_id)
                return fetch_pnl(
                    ib,
                    account_id=resolved_account,
                    **self._budget_kwargs(effective_budget_user_id),
                )

    def get_pnl_single(
        self,
        account_id: str | None,
        con_id: int,
        *,
        budget_user_id: int | None = None,
    ) -> dict[str, Any]:
        effective_budget_user_id = self._effective_budget_user_id(budget_user_id)
        with ibkr_shared_lock:
            with self._conn_manager.connection(**self._budget_kwargs(effective_budget_user_id)) as ib:
                resolved_account = self._resolve_account_id(ib, account_id)
                return fetch_pnl_single(
                    ib,
                    account_id=resolved_account,
                    con_id=int(con_id),
                    **self._budget_kwargs(effective_budget_user_id),
                )

    def get_contract_details(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        *,
        budget_user_id: int | None = None,
    ) -> list[dict[str, Any]]:
        effective_budget_user_id = self._effective_budget_user_id(budget_user_id)
        with ibkr_shared_lock:
            with self._conn_manager.connection(**self._budget_kwargs(effective_budget_user_id)) as ib:
                return fetch_contract_details(
                    ib,
                    symbol=symbol,
                    sec_type=sec_type,
                    exchange=exchange,
                    currency=currency,
                    **self._budget_kwargs(effective_budget_user_id),
                )

    def get_futures_months(
        self,
        symbol: str,
        *,
        budget_user_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Discover available contract months for a futures root."""
        from .metadata import fetch_futures_months

        effective_budget_user_id = self._effective_budget_user_id(budget_user_id)
        with ibkr_shared_lock:
            with self._conn_manager.connection(**self._budget_kwargs(effective_budget_user_id)) as ib:
                return fetch_futures_months(
                    ib,
                    symbol,
                    **self._budget_kwargs(effective_budget_user_id),
                )

    def get_option_chain(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        *,
        budget_user_id: int | None = None,
    ) -> dict[str, Any]:
        effective_budget_user_id = self._effective_budget_user_id(budget_user_id)
        with ibkr_shared_lock:
            with self._conn_manager.connection(**self._budget_kwargs(effective_budget_user_id)) as ib:
                return fetch_option_chain(
                    ib,
                    symbol=symbol,
                    sec_type=sec_type,
                    exchange=exchange,
                    **self._budget_kwargs(effective_budget_user_id),
                )

    def list_capabilities(
        self,
        category: str | None = None,
        *,
        budget_user_id: int | None = None,
    ) -> list[dict[str, Any]]:
        del budget_user_id
        return [asdict(cap) for cap in list_capabilities(category=category)]

    def describe(self, capability_name: str, *, budget_user_id: int | None = None) -> dict[str, Any]:
        del budget_user_id
        cap = get_capability(capability_name)
        if cap is None:
            raise KeyError(f"Unknown capability: {capability_name}")
        return asdict(cap)


def get_ibkr_client(
    *,
    client_id: int | None = None,
    budget_user_id: int | None = None,
) -> IBKRClient:
    """Return a fresh IBKR facade instance.

    Notes:
    - This factory is intentionally not cached.
    - Passing ``client_id`` only affects the market-data client created inside
      ``IBKRClient`` today.
    - Account and metadata methods still use the default
      ``IBKRConnectionManager()`` singleton, so their client-id behavior is
      unchanged in this PR.
    - Omitting ``client_id`` preserves the current configured defaults inside
      ``IBKRClient``.
    """

    init_kwargs: dict[str, int] = {}
    if client_id is not None:
        init_kwargs["client_id"] = client_id
    if budget_user_id is not None:
        init_kwargs["budget_user_id"] = budget_user_id
    return IBKRClient(**init_kwargs)
