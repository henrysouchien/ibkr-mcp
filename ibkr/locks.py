"""Thread-safety locks shared across IBKR modules."""

from __future__ import annotations

import threading


ibkr_shared_lock = threading.Lock()
"""Serializes calls on the shared IBKRConnectionManager IB instance."""
