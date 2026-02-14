from __future__ import annotations

from enum import Enum


class ExecutionMode(str, Enum):
    LIVE_TRADING = "live_trading"
    MONITOR_ONLY = "monitor_only"


execution_mode = ExecutionMode.LIVE_TRADING


def set_execution_mode(mode: ExecutionMode) -> None:
    global execution_mode
    execution_mode = mode


def get_execution_mode() -> ExecutionMode:
    return execution_mode
