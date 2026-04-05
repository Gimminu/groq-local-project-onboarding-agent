from __future__ import annotations

# Backward-compatibility re-exports for legacy test and CLI import paths.
from app.legacy.organizer_launchd import (
    DEFAULT_LAUNCHD_LABEL,
    STANDARD_SCOPE,
    build_launch_agent_plist,
    install_launch_agents,
    install_standard_launch_agents,
    uninstall_launch_agents,
)

__all__ = [
    "DEFAULT_LAUNCHD_LABEL",
    "STANDARD_SCOPE",
    "build_launch_agent_plist",
    "install_launch_agents",
    "install_standard_launch_agents",
    "uninstall_launch_agents",
]
