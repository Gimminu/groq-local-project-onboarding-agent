"""Canonical organizer package.

This module provides a stable import path (`app.organizer`) while
keeping compatibility with existing `app.index_v2` internals.
"""

from app.index_v2 import IndexOrganizerService, load_index_config

__all__ = ["IndexOrganizerService", "load_index_config"]
