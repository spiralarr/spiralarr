"""Task and DAG state definitions."""

from typing import Set


class State:
    """Task and DAG state constants."""

    # Task states
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

    # DAG states
    DAG_RUNNING = "running"
    DAG_SUCCESS = "success"
    DAG_FAILED = "failed"

    @classmethod
    @property
    def terminal(cls) -> Set[str]:
        """States that indicate a task has finished."""
        return {cls.SUCCESS, cls.FAILED, cls.SKIPPED}

    @classmethod
    @property
    def running(cls) -> Set[str]:
        """States that indicate a task is currently executing."""
        return {cls.RUNNING}

    @classmethod
    @property
    def finished(cls) -> Set[str]:
        """States that indicate a task is no longer active."""
        return cls.terminal | cls.running
