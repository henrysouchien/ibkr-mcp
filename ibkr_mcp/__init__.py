"""Import alias for ibkr package.

Avoids collision when a project has its own local ibkr/ subpackage
that shadows the top-level ibkr distribution.
"""

from pathlib import Path
import sys

_PACKAGE_ROOT = str(Path(__file__).resolve().parents[1])
if sys.path[0] != _PACKAGE_ROOT:
    try:
        sys.path.remove(_PACKAGE_ROOT)
    except ValueError:
        pass
    sys.path.insert(0, _PACKAGE_ROOT)

from ibkr.client import IBKRClient
from ibkr.exceptions import IBKRContractError, IBKRDataError

__all__ = ["IBKRClient", "IBKRContractError", "IBKRDataError"]
