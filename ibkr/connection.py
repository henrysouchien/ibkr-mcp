"""Singleton managing the ib_async connection to IB Gateway.

Agent orientation:
    Persistent connection boundary used by account/metadata operations.
    Market-data paths intentionally use separate per-request lifecycle in
    ``ibkr.market_data``.
"""

from __future__ import annotations

import threading
import time
from typing import List, Optional

from ._logging import portfolio_logger
from .config import (
    IBKR_CLIENT_ID,
    IBKR_GATEWAY_HOST,
    IBKR_GATEWAY_PORT,
    IBKR_READONLY,
    IBKR_TIMEOUT,
)


class IBKRConnectionManager:
    """Manage a single persistent IB Gateway connection."""

    _instance: Optional["IBKRConnectionManager"] = None
    _instance_lock = threading.Lock()

    def __new__(cls):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        self._host = IBKR_GATEWAY_HOST
        self._port = IBKR_GATEWAY_PORT
        self._client_id = IBKR_CLIENT_ID
        self._timeout = IBKR_TIMEOUT
        self._readonly = IBKR_READONLY

        self._ib = None
        self._managed_accounts: List[str] = []
        self._connect_lock = threading.RLock()
        self._reconnect_delay = 5
        self._max_reconnect_attempts = 3
        self._reconnecting = False
        self._manual_disconnect = False

    def connect(self):
        """Connect to IB Gateway. Thread-safe and idempotent."""
        with self._connect_lock:
            if self._ib is not None and self._ib.isConnected():
                return self._ib

            from ib_async import IB

            ib = IB()
            ib.disconnectedEvent += self._on_disconnect

            portfolio_logger.info(
                f"Connecting to IB Gateway at {self._host}:{self._port} "
                f"(clientId={self._client_id}, readonly={self._readonly})"
            )

            try:
                ib.connect(
                    host=self._host,
                    port=self._port,
                    clientId=self._client_id,
                    timeout=self._timeout,
                    readonly=self._readonly,
                )
            except Exception:
                try:
                    ib.disconnectedEvent -= self._on_disconnect
                except Exception:
                    pass
                try:
                    ib.disconnect()
                except Exception:
                    pass
                raise

            self._ib = ib
            self._managed_accounts = list(ib.managedAccounts() or [])
            portfolio_logger.info(f"Connected to IB Gateway. Managed accounts: {self._managed_accounts}")
            return ib

    def disconnect(self) -> None:
        """Disconnect from IB Gateway and suppress auto-reconnect."""
        with self._connect_lock:
            if self._ib is None:
                return

            try:
                self._manual_disconnect = True
                try:
                    self._ib.disconnectedEvent -= self._on_disconnect
                except Exception:
                    pass
                self._ib.disconnect()
            except Exception as e:
                portfolio_logger.warning(f"Error during IB disconnect: {e}")
            finally:
                self._ib = None
                self._managed_accounts = []
                self._manual_disconnect = False

    def ensure_connected(self):
        """Return connected IB instance, reconnecting as needed."""
        if self._ib is not None and self._ib.isConnected():
            return self._ib
        return self.connect()

    def get_ib(self):
        """Alias for ensure_connected()."""
        return self.ensure_connected()

    @property
    def managed_accounts(self) -> list[str]:
        """Return managed account IDs discovered from IB Gateway."""
        return list(self._managed_accounts)

    @property
    def is_connected(self) -> bool:
        return self._ib is not None and self._ib.isConnected()

    def _on_disconnect(self) -> None:
        """Handle unexpected disconnection and schedule reconnect."""
        if self._manual_disconnect:
            return

        portfolio_logger.warning("IB Gateway connection lost. Scheduling reconnect...")
        with self._connect_lock:
            self._ib = None
            self._managed_accounts = []

            if not self._reconnecting:
                self._reconnecting = True
                thread = threading.Thread(target=self._reconnect, daemon=True)
                thread.start()

    def _reconnect(self) -> None:
        """Background reconnect worker with linear backoff."""
        try:
            for attempt in range(1, self._max_reconnect_attempts + 1):
                delay = self._reconnect_delay * attempt
                portfolio_logger.info(
                    f"IB reconnect attempt {attempt}/{self._max_reconnect_attempts} in {delay}s..."
                )
                time.sleep(delay)
                try:
                    self.connect()
                    portfolio_logger.info("IB Gateway reconnected successfully")
                    return
                except Exception as e:
                    portfolio_logger.warning(f"IB reconnect attempt {attempt} failed: {e}")

            portfolio_logger.error(
                f"IB Gateway reconnection failed after {self._max_reconnect_attempts} attempts. "
                "IBKR trades will fail until gateway is restored."
            )
        finally:
            with self._connect_lock:
                self._reconnecting = False
