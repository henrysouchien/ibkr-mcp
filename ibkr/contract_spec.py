"""Vendor-neutral contract specs for boundary-crossing IBKR snapshot calls."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Union

SecType = Literal["STK", "FUT", "OPT"]
OptionRight = Literal["C", "P"]


@dataclass(frozen=True)
class IBKRContractSpec:
    """Vendor-neutral description of a contract for boundary-crossing calls."""

    sec_type: SecType
    symbol: str
    exchange: str = "SMART"
    currency: str = "USD"
    expiry: Optional[str] = None
    strike: Optional[float] = None
    right: Optional[OptionRight] = None
    multiplier: Optional[Union[str, int, float]] = None
    contract_month: Optional[str] = None
    con_id: Optional[int] = None

    @classmethod
    def stock(
        cls,
        symbol: str,
        *,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> "IBKRContractSpec":
        return cls(sec_type="STK", symbol=symbol, exchange=exchange, currency=currency)

    @classmethod
    def option(
        cls,
        symbol: str,
        *,
        expiry: str,
        strike: float,
        right: OptionRight,
        exchange: str = "SMART",
        currency: str = "USD",
        multiplier: Optional[Union[str, int, float]] = None,
    ) -> "IBKRContractSpec":
        return cls(
            sec_type="OPT",
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            right=right,
            exchange=exchange,
            currency=currency,
            multiplier=multiplier,
        )

    @classmethod
    def option_by_con_id(
        cls,
        symbol: str,
        *,
        con_id: int,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> "IBKRContractSpec":
        return cls(
            sec_type="OPT",
            symbol=symbol,
            con_id=con_id,
            exchange=exchange,
            currency=currency,
        )

    @classmethod
    def future(
        cls,
        symbol: str,
        *,
        contract_month: Optional[str] = None,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> "IBKRContractSpec":
        return cls(
            sec_type="FUT",
            symbol=symbol,
            contract_month=contract_month,
            exchange=exchange,
            currency=currency,
        )
