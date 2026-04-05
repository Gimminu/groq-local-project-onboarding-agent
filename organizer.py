#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
from pathlib import Path

from index_organizer import main


def _ensure_supported_python() -> None:
    if sys.version_info >= (3, 10):
        return
    preferred = Path("/opt/homebrew/bin/python3.13")
    if preferred.exists() and Path(sys.executable) != preferred:
        os.execv(str(preferred), [str(preferred), __file__, *sys.argv[1:]])
    raise SystemExit("Folder Organizer V2 requires Python 3.10+.")


if __name__ == "__main__":
    _ensure_supported_python()
    raise SystemExit(main())
