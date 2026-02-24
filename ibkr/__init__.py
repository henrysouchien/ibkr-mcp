"""IBKR provider package.

Agent orientation:
    Public package entrypoint for IBKR integration.
    For market data/account operations start with ``ibkr.client.IBKRClient``;
    for compatibility wrappers used by other modules start with ``ibkr.compat``.
"""

from .client import IBKRClient
from .market_data import IBKRMarketDataClient
from .compat import (
    fetch_ibkr_bond_monthly_close,
    fetch_ibkr_daily_close_futures,
    fetch_ibkr_flex_trades,
    fetch_ibkr_fx_monthly_close,
    fetch_ibkr_monthly_close,
    fetch_ibkr_option_monthly_mark,
    get_futures_currency,
    get_ibkr_futures_exchanges,
    get_ibkr_futures_fmp_map,
)
from .exceptions import (
    IBKRConnectionError,
    IBKRContractError,
    IBKRDataError,
    IBKREntitlementError,
    IBKRAccountError,
    IBKRNoDataError,
    IBKRTimeoutError,
)
from .profiles import InstrumentProfile, get_profile, get_profiles

__all__ = [
    "IBKRClient",
    "IBKRMarketDataClient",
    "fetch_ibkr_monthly_close",
    "fetch_ibkr_daily_close_futures",
    "fetch_ibkr_fx_monthly_close",
    "fetch_ibkr_bond_monthly_close",
    "fetch_ibkr_option_monthly_mark",
    "fetch_ibkr_flex_trades",
    "get_ibkr_futures_fmp_map",
    "get_ibkr_futures_exchanges",
    "get_futures_currency",
    "InstrumentProfile",
    "get_profile",
    "get_profiles",
    "IBKRDataError",
    "IBKRConnectionError",
    "IBKRContractError",
    "IBKRNoDataError",
    "IBKREntitlementError",
    "IBKRAccountError",
    "IBKRTimeoutError",
]
