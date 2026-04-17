"""Shared pytest bootstrap for src-layout imports."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
_SESSION_LOOP: asyncio.AbstractEventLoop | None = None

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def pytest_sessionstart(session):
    del session
    global _SESSION_LOOP
    _SESSION_LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(_SESSION_LOOP)


def pytest_sessionfinish(session, exitstatus):
    del session, exitstatus
    global _SESSION_LOOP
    loop = _SESSION_LOOP
    if loop is not None and not loop.is_closed():
        loop.close()
    _SESSION_LOOP = None
