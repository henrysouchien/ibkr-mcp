"""IBKR package-local logging shims with monorepo fallback behavior."""

from __future__ import annotations

import logging
import sys


def _make_fallback_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"ibkr.{name}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


try:
    from utils.logging import portfolio_logger, trading_logger
except Exception:
    portfolio_logger = _make_fallback_logger("portfolio")
    trading_logger = _make_fallback_logger("trading")
