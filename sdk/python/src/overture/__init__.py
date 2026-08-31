"""Overture — safe execution boundary (Action -> Run -> Proof).

Canonical import is `overture`. Legacy `igris` import remains for backward compat.
This package re-exports the igris SDK surface under the overture name.

Usage:
    from overture import Overture, Igris
    # or legacy:
    from igris import Igris
"""

from __future__ import annotations

# Re-export everything from igris for backward compat
# The overture name is canonical going forward; igris is alias.
from igris import *  # noqa: F403,F401
from igris import (
    ActionContract,
    ActionTarget,
    ContractBinding,
    DurableRun,
    DurableRunStatus,
    EvidenceLinkResult,
    Igris,
    IgrisDurableClient,
    __version__,
)

# Alias: Overture is the canonical facade name going forward
Overture = Igris

__all__ = [
    "Igris",
    "Overture",
    "IgrisDurableClient",
    "ActionContract",
    "ActionTarget",
    "ContractBinding",
    "DurableRun",
    "DurableRunStatus",
    "EvidenceLinkResult",
    "__version__",
]
