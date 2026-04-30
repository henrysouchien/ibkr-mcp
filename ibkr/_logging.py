"""IBKR package logging."""

from __future__ import annotations

import logging
import sys
import time

logger = logging.getLogger("ibkr")

# Ensure at least stderr output when no root handlers are configured
# (standalone/CLI usage outside the monorepo).
if not logging.root.handlers and not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


def log_event(logger: logging.Logger, level: int, event: str, msg: str = "", **fields) -> None:
    """Log with structured key=value fields.

    Output: [ibkr.connect] Connected to gateway client_id=20 elapsed_ms=142
    """
    parts = [f"[ibkr.{event}]"]
    if msg:
        parts.append(msg)
    for key, value in fields.items():
        if value is not None:
            parts.append(f"{key}={value}")
    logger.log(level, " ".join(parts))


class TimingContext:
    """Context manager for elapsed time measurement."""

    def __init__(self, name: str | None = None):
        self.name = name

    def __enter__(self):
        self.start = time.monotonic()
        return self

    def __exit__(self, *args):
        self.elapsed_ms = round((time.monotonic() - self.start) * 1000, 1)
        if self.name:
            try:
                from app_platform.logging.core import log_timing_event

                log_timing_event("dependency", self.name, self.elapsed_ms)
            except Exception:
                pass
