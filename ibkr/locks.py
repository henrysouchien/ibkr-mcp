"""Thread-safety locks shared across IBKR modules."""

from __future__ import annotations

import threading


ibkr_shared_lock = threading.Lock()
"""Serializes IBKR account/metadata calls.

In persistent mode: serializes access to the shared IB instance.
In ephemeral mode: prevents concurrent connections on the same client ID.
"""
