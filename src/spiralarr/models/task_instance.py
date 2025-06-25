"""Task instance model for tracking individual task executions."""

import datetime as dt
from typing import TYPE_CHECKING, Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from spiralarr.models.base import Base
from spiralarr.utils.state import State

if TYPE_CHECKING:
    from spiralarr.models.dag_run import DagRun
    from spiralarr.models.task_instance_result import TaskInstanceResult


class TaskInstance(Base):
    """
    Represents a specific run of a task. The scheduler and executor update
    the state of this object in the database.
    """

    __tablename__ = "task_instance"

    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[str] = mapped_column(String(255))
    dag_run_id: Mapped[int] = mapped_column(
        ForeignKey("dag_run.id", ondelete="CASCADE")
    )

    state: Mapped[str] = mapped_column(String(20), default=State.SCHEDULED, index=True)
    start_date: Mapped[Optional[dt.datetime]] = mapped_column(DateTime)
    end_date: Mapped[Optional[dt.datetime]] = mapped_column(DateTime)
    duration: Mapped[Optional[int]] = mapped_column(Integer)  # In seconds

    # NEW: Timestamp for when the task was sent to the executor.
    # Used to detect "zombie" tasks that get stuck in a QUEUED state.
    queued_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime)

    # A simple way to link to logs if stored on disk
    log_filepath: Mapped[Optional[str]] = mapped_column(String(2000))

    dag_run: Mapped["DagRun"] = relationship(
        back_populates="task_instances",
        lazy="selectin",  # Async-safe loading
    )
    result: Mapped[Optional["TaskInstanceResult"]] = relationship(
        back_populates="task_instance", lazy="selectin"
    )

    __table_args__ = (
        UniqueConstraint(
            "task_id", "dag_run_id", name="uq_task_instance_task_id_dag_run_id"
        ),
    )

    def __repr__(self):
        return f"<TaskInstance {self.dag_id}.{self.task_id} state={self.state}>"

    @property
    def dag_id(self):
        # This assumes dag_run is eagerly loaded or accessed within a session
        return self.dag_run.dag_id if self.dag_run else None

    def get_key(self):
        """Returns a tuple key for this task instance"""
        if not self.dag_run:
            # Fallback for cases where dag_run relationship isn't loaded
            # This can happen in tests or when TaskInstance is created without proper relationships  # noqa: E501
            return (f"dag_run_id_{self.dag_run_id}", self.task_id, "unknown_run_id")
        return (self.dag_run.dag_id, self.task_id, self.dag_run.run_id)

    def set_state(self, state: str, log_transition: bool = True):
        """
        Sets the state of the task instance.

        REVISED: This method NO LONGER commits to the database.
        The calling code (Scheduler, Task Runner) is responsible for session
        management and committing the transaction. This allows for atomic
        updates of multiple objects.

        Args:
            state: The new state to set
            log_transition: Whether to log the state transition
        """
        if str(self.state) == str(state):
            return

        # Validate state transition
        if not self._is_valid_state_transition(str(self.state), state):
            raise ValueError(
                f"Invalid state transition from {self.state} to {state} "
                f"for task {self.get_key()}"
            )

        old_state = self.state
        self.state = state
        # Use UTC timezone-naive datetime for consistency (AI/LLM-first design)
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

        # Update timestamps based on state
        if state == State.QUEUED:
            self.queued_at = now
        elif state in State.running:
            self.start_date = now
        elif state in State.terminal:
            self.end_date = now
            if self.start_date is not None:
                self.duration = int((self.end_date - self.start_date).total_seconds())

        # Log the transition
        if log_transition:
            print(f"Task {self.get_key()} state: {old_state} → {state}")

    def _is_valid_state_transition(self, from_state: str, to_state: str) -> bool:
        """
        Validate if a state transition is allowed.

        Args:
            from_state: Current state
            to_state: Target state

        Returns:
            True if transition is valid, False otherwise
        """
        # Define valid state transitions
        # Note: SCHEDULED → RUNNING is allowed for direct task execution (AI/LLM usage)
        valid_transitions = {
            State.SCHEDULED: [State.QUEUED, State.RUNNING, State.FAILED],
            State.QUEUED: [State.RUNNING, State.FAILED],
            State.RUNNING: [State.SUCCESS, State.FAILED],
            State.SUCCESS: [],  # Terminal state
            State.FAILED: [State.QUEUED],  # Can retry
        }

        return to_state in valid_transitions.get(from_state, [])

    def is_terminal(self) -> bool:
        """Check if the task is in a terminal state."""
        return self.state in State.terminal

    def is_running(self) -> bool:
        """Check if the task is currently running."""
        return self.state in State.running

    def can_retry(self) -> bool:
        """Check if the task can be retried (is in FAILED state)."""
        return bool(self.state == State.FAILED)

    def get_execution_time(self) -> float:
        """
        Get the execution time in seconds.

        Returns:
            Execution time in seconds, or 0 if not available
        """
        if self.duration is not None:
            # Type ignore for SQLAlchemy column conversion
            return float(self.duration)  # type: ignore
        elif self.start_date is not None and self.end_date is not None:
            return (self.end_date - self.start_date).total_seconds()
        elif self.start_date is not None and self.state in State.running:
            current_time = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
            return (current_time - self.start_date).total_seconds()
        return 0.0

    def get_state_summary(self) -> str:
        """Get a summary string of the task state."""
        summary = f"State: {self.state}"
        if self.start_date is not None:
            summary += f", Started: {self.start_date}"
        if self.end_date is not None:
            summary += f", Ended: {self.end_date}"
        if self.duration is not None:
            summary += f", Duration: {self.duration}s"
        return summary
