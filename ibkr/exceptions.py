"""IBKR market data exceptions."""

from __future__ import annotations


class IBKRDataError(Exception):
    """Base exception for IBKR market data errors."""

    pass


class IBKRConnectionError(IBKRDataError):
    """Raised when IBKR Gateway/TWS connection fails."""

    pass


class IBKRContractError(IBKRDataError):
    """Raised when an IBKR contract cannot be resolved or qualified."""

    pass


class IBKRNoDataError(IBKRDataError):
    """Raised when IBKR returns no data for a valid request."""

    pass


class IBKREntitlementError(IBKRDataError):
    """Raised when account lacks required market data permissions."""

    pass


class IBKRAccountError(IBKRDataError):
    """Raised when account data request fails (auth, ambiguity, unavailable account)."""

    pass


class IBKRTimeoutError(IBKRDataError):
    """Raised when IBKR request times out waiting for callback data."""

    pass
