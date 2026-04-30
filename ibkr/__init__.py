"""IBKR provider package public boundary."""

from importlib import import_module

_LAZY_EXPORTS = {
    "IBKRClient": ("ibkr.client", "IBKRClient"),
    "IBKRContractSpec": ("ibkr.contract_spec", "IBKRContractSpec"),
    "get_ibkr_client": ("ibkr.client", "get_ibkr_client"),
    "probe_ibkr_connection": ("ibkr.connection", "probe_ibkr_connection"),
    "get_ibkr_connection_status": ("ibkr.connection", "get_ibkr_connection_status"),
    "fetch_flex_report": ("ibkr.flex", "fetch_flex_report"),
    "fetch_ibkr_monthly_close": ("ibkr.compat", "fetch_ibkr_monthly_close"),
    "fetch_ibkr_daily_close_futures": ("ibkr.compat", "fetch_ibkr_daily_close_futures"),
    "fetch_ibkr_daily_close_fx": ("ibkr.compat", "fetch_ibkr_daily_close_fx"),
    "fetch_ibkr_daily_close_bond": ("ibkr.compat", "fetch_ibkr_daily_close_bond"),
    "fetch_ibkr_fx_monthly_close": ("ibkr.compat", "fetch_ibkr_fx_monthly_close"),
    "fetch_ibkr_bond_monthly_close": ("ibkr.compat", "fetch_ibkr_bond_monthly_close"),
    "fetch_ibkr_option_monthly_mark": ("ibkr.compat", "fetch_ibkr_option_monthly_mark"),
    "fetch_ibkr_flex_trades": ("ibkr.compat", "fetch_ibkr_flex_trades"),
    "get_ibkr_futures_fmp_map": ("ibkr.compat", "get_ibkr_futures_fmp_map"),
    "get_ibkr_futures_exchanges": ("ibkr.compat", "get_ibkr_futures_exchanges"),
    "get_futures_currency": ("ibkr.compat", "get_futures_currency"),
}


def __getattr__(name: str):
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module 'ibkr' has no attribute {name!r}")
    module_name, attr_name = _LAZY_EXPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value

__all__ = [
    "IBKRClient",
    "IBKRContractSpec",
    "get_ibkr_client",
    "probe_ibkr_connection",
    "get_ibkr_connection_status",
    "fetch_flex_report",
    "fetch_ibkr_monthly_close",
    "fetch_ibkr_daily_close_futures",
    "fetch_ibkr_daily_close_fx",
    "fetch_ibkr_daily_close_bond",
    "fetch_ibkr_fx_monthly_close",
    "fetch_ibkr_bond_monthly_close",
    "fetch_ibkr_option_monthly_mark",
    "fetch_ibkr_flex_trades",
    "get_ibkr_futures_fmp_map",
    "get_ibkr_futures_exchanges",
    "get_futures_currency",
]
