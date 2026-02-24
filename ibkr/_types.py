"""Shared instrument metadata types vendored for ibkr package portability."""

from __future__ import annotations

from typing import Any, Literal

InstrumentType = Literal["equity", "option", "futures", "fx", "fx_artifact", "bond", "unknown"]

_VALID_INSTRUMENT_TYPES = {
    "equity",
    "option",
    "futures",
    "fx",
    "fx_artifact",
    "bond",
    "unknown",
}


def coerce_instrument_type(value: Any, default: InstrumentType = "equity") -> InstrumentType:
    """Normalize external instrument strings to the supported instrument type enum."""
    normalized = str(value or "").strip().lower()
    if normalized in _VALID_INSTRUMENT_TYPES:
        return normalized  # type: ignore[return-value]
    return default
