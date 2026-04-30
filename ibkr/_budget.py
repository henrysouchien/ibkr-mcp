"""Internal helpers for IBKR API budget guard wrapping."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from app_platform.api_budget import guard_call
from config.api_budget_costs import COST_PER_CALL


def ibkr_cost_per_call(operation: str) -> Any:
    return COST_PER_CALL.get(("ibkr", operation), 0)


def guard_ib_call(
    *,
    operation: str,
    fn: Callable[..., Any],
    args: tuple[Any, ...] = (),
    kwargs: Mapping[str, Any] | None = None,
    budget_user_id: int | None = None,
) -> Any:
    return guard_call(
        provider="ibkr",
        operation=operation,
        budget_user_id=budget_user_id,
        cost_per_call=ibkr_cost_per_call(operation),
        fn=fn,
        args=args,
        kwargs=dict(kwargs) if kwargs is not None else None,
    )


__all__ = ["guard_ib_call", "ibkr_cost_per_call"]
