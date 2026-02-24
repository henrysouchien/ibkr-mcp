"""Instrument request profiles for IBKR market data."""

from __future__ import annotations

from dataclasses import dataclass

from ._types import InstrumentType, coerce_instrument_type


@dataclass(frozen=True)
class InstrumentProfile:
    """Declarative fetch profile per instrument type."""

    instrument_type: str
    what_to_show_chain: list[str]
    bar_size: str
    use_rth: bool
    duration: str


_PROFILES: dict[str, InstrumentProfile] = {
    "futures": InstrumentProfile(
        instrument_type="futures",
        what_to_show_chain=["TRADES"],
        bar_size="1 month",
        use_rth=True,
        duration="2 Y",
    ),
    "futures_daily": InstrumentProfile(
        instrument_type="futures",
        what_to_show_chain=["TRADES"],
        bar_size="1 day",
        use_rth=True,
        duration="2 Y",
    ),
    "fx": InstrumentProfile(
        instrument_type="fx",
        what_to_show_chain=["MIDPOINT", "BID", "ASK"],
        bar_size="1 day",
        use_rth=False,
        duration="2 Y",
    ),
    "bond": InstrumentProfile(
        instrument_type="bond",
        what_to_show_chain=["MIDPOINT", "BID", "ASK"],
        bar_size="1 day",
        use_rth=True,
        duration="2 Y",
    ),
    "option": InstrumentProfile(
        instrument_type="option",
        what_to_show_chain=["MIDPOINT", "BID", "ASK"],
        bar_size="1 day",
        use_rth=True,
        duration="2 Y",
    ),
}


def get_profile(instrument_type: str | InstrumentType) -> InstrumentProfile:
    """Return the configured profile for an instrument type."""
    raw_instrument_type = str(instrument_type or "").strip().lower()
    if raw_instrument_type in _PROFILES:
        return _PROFILES[raw_instrument_type]
    if raw_instrument_type in {"fx", "forex"}:
        return _PROFILES["fx"]

    normalized = coerce_instrument_type(instrument_type, default="unknown")
    if normalized == "fx_artifact":
        normalized = "fx"
    profile = _PROFILES.get(normalized)
    if profile is None:
        raise KeyError(f"No IBKR profile configured for instrument type '{instrument_type}'")
    return profile


def get_profiles() -> dict[str, InstrumentProfile]:
    """Return all configured profiles."""
    return dict(_PROFILES)
