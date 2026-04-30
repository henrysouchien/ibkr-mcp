from __future__ import annotations

import asyncio
import inspect


def ensure_event_loop() -> None:
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


def _has_running_loop() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _ensure_loop_factory_compatible_asyncio_run() -> None:
    if not getattr(asyncio, "_nest_patched", False):
        return

    run = asyncio.run
    try:
        parameters = inspect.signature(run).parameters
    except (TypeError, ValueError):
        parameters = {}
    if "loop_factory" in parameters:
        return

    def _compat_run(main, *, debug=None, loop_factory=None):
        if loop_factory is not None:
            from asyncio.runners import Runner

            with Runner(debug=debug, loop_factory=loop_factory) as runner:
                return runner.run(main)
        if debug is None:
            return run(main)
        return run(main, debug=debug)

    asyncio.run = _compat_run


def apply_nest_asyncio_if_running_loop() -> bool:
    try:
        import nest_asyncio
    except Exception:
        return False

    applied = False
    if _has_running_loop():
        try:
            nest_asyncio.apply()
        except Exception:
            applied = False
        else:
            applied = True

    _ensure_loop_factory_compatible_asyncio_run()
    return applied
