"""Singleton managing the ib_async connection to IB Gateway.

Agent orientation:
    Connection boundary used by account/metadata operations.
    Market-data paths intentionally use separate per-request lifecycle in
    ``ibkr.market_data``.
"""

from __future__ import annotations

from contextlib import contextmanager
import logging
import threading
import time
from typing import Any, List, Optional

from ._logging import log_event, logger, TimingContext
from .config import (
    IBKR_CLIENT_ID,
    IBKR_CONNECTION_MODE,
    IBKR_CONNECT_MAX_ATTEMPTS,
    IBKR_GATEWAY_HOST,
    IBKR_GATEWAY_PORT,
    IBKR_MAX_RECONNECT_ATTEMPTS,
    IBKR_READONLY,
    IBKR_RECONNECT_DELAY,
    IBKR_TIMEOUT,
)

try:
    import nest_asyncio

    nest_asyncio.apply()
except Exception:
    pass


class IBKRConnectionManager:
    """Manage IB Gateway connection lifecycle for account/metadata operations.

    Default (no args) → singleton using ``IBKR_CLIENT_ID``.
    Pass ``client_id=N`` for an independent instance (e.g. trading adapter
    using ``IBKR_TRADE_CLIENT_ID`` to avoid colliding with ibkr-mcp).
    """

    _instance: Optional["IBKRConnectionManager"] = None
    _instance_lock = threading.Lock()

    def __new__(cls, client_id: Optional[int] = None, **kwargs):
        if client_id is not None:
            # Non-singleton path: dedicated connection with custom client_id.
            obj = super().__new__(cls)
            obj._initialized = False
            return obj
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, client_id: Optional[int] = None, default_max_attempts: Optional[int] = None) -> None:
        if self._initialized:
            return
        self._initialized = True

        self._host = IBKR_GATEWAY_HOST
        self._port = IBKR_GATEWAY_PORT
        self._client_id = client_id if client_id is not None else IBKR_CLIENT_ID
        self._timeout = IBKR_TIMEOUT
        self._readonly = IBKR_READONLY
        self._default_max_attempts = default_max_attempts or IBKR_CONNECT_MAX_ATTEMPTS

        self._ib = None
        self._managed_accounts: List[str] = []
        self._connect_lock = threading.RLock()
        self._reconnect_delay = IBKR_RECONNECT_DELAY
        self._max_reconnect_attempts = IBKR_MAX_RECONNECT_ATTEMPTS
        self._reconnecting = False
        self._manual_disconnect = False

    def _do_connect(self, attach_events: bool = True):
        """Create and connect a fresh IB instance without storing to self._ib."""
        from ib_async import IB

        ib = IB()
        if attach_events:
            ib.disconnectedEvent += self._on_disconnect
        try:
            ib.connect(
                host=self._host,
                port=self._port,
                clientId=self._client_id,
                timeout=self._timeout,
                readonly=self._readonly,
            )
            return ib
        except Exception:
            if attach_events:
                try:
                    ib.disconnectedEvent -= self._on_disconnect
                except Exception:
                    pass
            try:
                ib.disconnect()
            except Exception:
                pass
            raise

    def connect(self, max_attempts: int | None = None):
        """Connect to IB Gateway. Thread-safe and idempotent.

        Retries up to *max_attempts* times (default from config) with linear
        backoff between attempts.
        """
        effective_attempts = max_attempts if max_attempts is not None else self._default_max_attempts
        with self._connect_lock:
            if self._ib is not None and self._ib.isConnected():
                return self._ib

            last_exc: Exception | None = None
            for attempt in range(1, effective_attempts + 1):
                if attempt > 1:
                    delay = self._reconnect_delay * (attempt - 1)
                    log_event(
                        logger, logging.INFO, "connect.retry",
                        f"in {delay}s",
                        attempt=attempt, max=effective_attempts,
                        error=str(last_exc) or type(last_exc).__name__ if last_exc else None,
                    )
                    time.sleep(delay)

                if attempt == 1:
                    log_event(
                        logger, logging.INFO, "connect",
                        host=self._host, port=self._port,
                        client_id=self._client_id, readonly=self._readonly,
                    )

                try:
                    with TimingContext() as tc:
                        ib = self._do_connect(attach_events=True)
                    self._ib = ib
                    self._managed_accounts = list(ib.managedAccounts() or [])
                    log_event(
                        logger, logging.INFO, "connect.ok",
                        client_id=self._client_id,
                        accounts=",".join(self._managed_accounts),
                        elapsed_ms=tc.elapsed_ms,
                    )
                    return ib
                except Exception as exc:
                    last_exc = exc
                    if attempt < effective_attempts:
                        log_event(
                            logger, logging.WARNING, "connect.retry",
                            f"in {self._reconnect_delay * attempt}s",
                            attempt=attempt, max=effective_attempts,
                            error=str(exc) or type(exc).__name__,
                        )

            log_event(
                logger, logging.ERROR, "connect.failed",
                client_id=self._client_id,
                error=str(last_exc) or type(last_exc).__name__ if last_exc else None,
            )
            raise last_exc  # type: ignore[misc]

    @contextmanager
    def connection(self):
        """Yield a connected IB instance based on configured connection mode."""
        if IBKR_CONNECTION_MODE == "persistent":
            yield self.ensure_connected()
            return

        last_exc: Exception | None = None
        ib = None
        for attempt in range(1, self._default_max_attempts + 1):
            if attempt > 1:
                delay = self._reconnect_delay * (attempt - 1)
                log_event(
                    logger, logging.INFO, "connect.retry",
                    f"in {delay}s",
                    attempt=attempt, max=self._default_max_attempts,
                    error=str(last_exc) or type(last_exc).__name__ if last_exc else None,
                )
                time.sleep(delay)
            try:
                with TimingContext() as tc:
                    ib = self._do_connect(attach_events=False)
                log_event(
                    logger, logging.INFO, "connect.ephemeral",
                    client_id=self._client_id,
                    elapsed_ms=tc.elapsed_ms,
                )
                break
            except Exception as exc:
                last_exc = exc
                if ib is not None:
                    try:
                        ib.disconnect()
                    except Exception:
                        pass
                    ib = None
        if ib is None:
            log_event(
                logger, logging.ERROR, "connect.failed",
                client_id=self._client_id,
                mode="ephemeral",
                error=str(last_exc) or type(last_exc).__name__ if last_exc else None,
            )
            if last_exc is not None:
                raise last_exc
            raise RuntimeError("IBKR connect failed")
        try:
            yield ib
        finally:
            try:
                ib.disconnect()
            except Exception:
                pass

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
                logger.warning(f"Error during IB disconnect: {e}")
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

    def get_connection_status(self) -> dict[str, Any]:
        """Return diagnostic dict describing current connection state."""
        if IBKR_CONNECTION_MODE == "ephemeral" and not self.is_connected:
            probe = self.probe_connection()
            return {
                "connected": probe["reachable"],
                "host": self._host,
                "port": self._port,
                "client_id": self._client_id,
                "mode": IBKR_CONNECTION_MODE,
                "readonly": self._readonly,
                "managed_accounts": probe.get("managed_accounts", []),
                "reconnecting": self._reconnecting,
                "error": probe.get("error"),
                "config": {
                    "timeout": self._timeout,
                    "connect_max_attempts": self._default_max_attempts,
                    "reconnect_delay": self._reconnect_delay,
                    "max_reconnect_attempts": self._max_reconnect_attempts,
                },
            }
        return {
            "connected": self.is_connected,
            "host": self._host,
            "port": self._port,
            "client_id": self._client_id,
            "mode": IBKR_CONNECTION_MODE,
            "readonly": self._readonly,
            "managed_accounts": list(self._managed_accounts),
            "reconnecting": self._reconnecting,
            "config": {
                "timeout": self._timeout,
                "connect_max_attempts": self._default_max_attempts,
                "reconnect_delay": self._reconnect_delay,
                "max_reconnect_attempts": self._max_reconnect_attempts,
            },
        }

    def probe_connection(self) -> dict[str, Any]:
        """Probe whether IB Gateway is reachable via connect + disconnect."""
        if self._ib is not None and self._ib.isConnected():
            return {"reachable": True, "managed_accounts": list(self._managed_accounts)}

        ib = None
        try:
            ib = self._do_connect(attach_events=False)
            accounts = list(ib.managedAccounts() or [])
            return {"reachable": True, "managed_accounts": accounts}
        except Exception as e:
            return {"reachable": False, "error": str(e)}
        finally:
            if ib is not None:
                try:
                    ib.disconnect()
                except Exception:
                    pass

    def _on_disconnect(self) -> None:
        """Handle unexpected disconnection and schedule reconnect."""
        if self._manual_disconnect:
            return

        log_event(
            logger, logging.WARNING, "disconnect",
            "Connection lost, scheduling reconnect",
            client_id=self._client_id,
        )
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
                log_event(
                    logger, logging.INFO, "connect.retry",
                    f"Reconnect in {delay}s",
                    attempt=attempt, max=self._max_reconnect_attempts,
                )
                time.sleep(delay)
                try:
                    self.connect(max_attempts=1)
                    log_event(
                        logger, logging.INFO, "reconnect.ok",
                        client_id=self._client_id,
                    )
                    return
                except Exception as e:
                    logger.warning(f"IB reconnect attempt {attempt} failed: {e}")

            log_event(
                logger, logging.ERROR, "reconnect.failed",
                "Trades will fail until gateway is restored",
                attempts=self._max_reconnect_attempts,
            )
        finally:
            with self._connect_lock:
                self._reconnecting = False
