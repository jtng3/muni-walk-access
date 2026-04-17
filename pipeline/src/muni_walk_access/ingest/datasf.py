"""Compatibility shim — the canonical module is ``ingest.sources.datasf``.

Story 5.3 T3 moved the DataSF SODA fetcher to
:mod:`muni_walk_access.ingest.sources.datasf`. This file keeps existing
imports (`from muni_walk_access.ingest.datasf import X`) working during
the migration window. T7 deletes this file once the 9 call sites that
still import from here have moved to the new path.

Implementation: we rebind ``sys.modules[__name__]`` to the canonical
module. Subsequent attribute access (reads *and* writes, including test
state mutation like ``datasf_mod._upstream_fallback = False``) routes
through the real module. Simpler and more correct than ``from ... import
*`` + per-name wrappers, which would silently desynchronize module-level
globals because ``from`` binds a new name in the importing module.
"""

import sys

from muni_walk_access.ingest.sources import datasf as _canonical

sys.modules[__name__] = _canonical
