"""Lightweight helpers vendored from trading_analysis for ibkr portability."""

from __future__ import annotations

from typing import Any


def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def normalize_strike(strike: Any) -> str:
    """Canonical strike string: 30.0->"30", 2.5->"2p5", 2.50->"2p5"."""
    val = float(strike)
    if val == int(val):
        return str(int(val))
    return f"{val:g}".replace(".", "p")
