"""Task instance result model for structured outcome tracking.

This model implements Tier 1 of the logging strategy - structured, actionable metadata
that serves as the primary source of truth for LLM agents to understand task outcomes
without parsing verbose log files.
"""

import datetime as dt
from typing import TYPE_CHECKING, Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from spiralarr.models.base import Base

if TYPE_CHECKING:
    from spiralarr.models.task_instance import TaskInstance


class TaskInstanceResult(Base):
    """
    Structured metadata for task execution outcomes.

    This table stores the definitive outcome of every task execution in a format
    optimized for LLM consumption. Instead of parsing logs to determine status,
    agents can query this table for immediate, structured insights.

    Key Design Principles:
    - Signal-to-noise optimization: Only essential outcome data
    - LLM-friendly: Structured fields for easy API consumption
    - Actionable: Includes failure context for debugging
    - Compact: Single record per task execution
    """

    __tablename__ = "task_instance_result"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign key to TaskInstance
    task_instance_id: Mapped[int] = mapped_column(
        ForeignKey("task_instance.id", ondelete="CASCADE"),
        unique=True,  # One result per task instance
    )

    # Core Status Fields
    status: Mapped[str] = mapped_column(String(20), index=True)  # SUCCESS, FAILED
    start_time: Mapped[dt.datetime] = mapped_column(DateTime)
    end_time: Mapped[dt.datetime] = mapped_column(DateTime)
    duration_seconds: Mapped[int] = mapped_column(Integer)  # Computed for easy querying

    # Success Information
    return_value: Mapped[Optional[str]] = mapped_column(
        Text
    )  # JSON string of operator return value

    # Failure Information
    error_type: Mapped[Optional[str]] = mapped_column(
        String(255)
    )  # e.g., "ValueError", "FileNotFoundError"
    error_message: Mapped[Optional[str]] = mapped_column(
        Text
    )  # First line of exception message
    exit_code: Mapped[Optional[int]] = mapped_column(
        Integer
    )  # For shell commands (BashOperator)

    # Smart Summary - Last few lines before failure
    log_excerpt: Mapped[Optional[str]] = mapped_column(
        Text
    )  # Critical context without full log parsing

    # Metadata
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime, default=lambda: dt.datetime.now(dt.timezone.utc)
    )

    # Relationships
    task_instance: Mapped["TaskInstance"] = relationship(
        back_populates="result", lazy="selectin"
    )

    def __repr__(self):
        return (
            f"<TaskInstanceResult {self.task_instance_id} "
            f"status={self.status} duration={self.duration_seconds}s>"
        )

    @property
    def is_success(self) -> bool:
        """Check if task execution was successful."""
        return bool(self.status == "SUCCESS")

    @property
    def is_failure(self) -> bool:
        """Check if task execution failed."""
        return bool(self.status == "FAILED")

    def get_summary(self) -> dict:
        """
        Get a compact summary for LLM consumption.

        Returns:
            Dictionary with essential outcome information
        """
        summary = {
            "status": self.status,
            "duration_seconds": self.duration_seconds,
            "start_time": (
                self.start_time.isoformat() if self.start_time is not None else None
            ),
            "end_time": (
                self.end_time.isoformat() if self.end_time is not None else None
            ),
        }

        if self.is_success and self.return_value is not None:
            summary["return_value"] = self.return_value

        if self.is_failure:
            summary["error"] = {
                "type": self.error_type,
                "message": self.error_message,
                "exit_code": self.exit_code,
            }
            if self.log_excerpt is not None:
                summary["log_excerpt"] = self.log_excerpt

        return summary

    def get_failure_context(self) -> Optional[dict]:
        """
        Get detailed failure context for debugging.

        Returns:
            Dictionary with failure details or None if task succeeded
        """
        if not self.is_failure:
            return None

        return {
            "error_type": self.error_type,
            "error_message": self.error_message,
            "exit_code": self.exit_code,
            "log_excerpt": self.log_excerpt,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def create_success_result(
        cls,
        task_instance_id: int,
        start_time: dt.datetime,
        end_time: dt.datetime,
        return_value: Optional[str] = None,
    ) -> "TaskInstanceResult":
        """
        Create a success result record.

        Args:
            task_instance_id: ID of the associated TaskInstance
            start_time: Task execution start time
            end_time: Task execution end time
            return_value: JSON string of operator return value

        Returns:
            TaskInstanceResult instance for success
        """
        duration = int((end_time - start_time).total_seconds())

        return cls(
            task_instance_id=task_instance_id,
            status="SUCCESS",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            return_value=return_value,
        )

    @classmethod
    def create_failure_result(
        cls,
        task_instance_id: int,
        start_time: dt.datetime,
        end_time: dt.datetime,
        error_type: str,
        error_message: str,
        exit_code: Optional[int] = None,
        log_excerpt: Optional[str] = None,
    ) -> "TaskInstanceResult":
        """
        Create a failure result record.

        Args:
            task_instance_id: ID of the associated TaskInstance
            start_time: Task execution start time
            end_time: Task execution end time
            error_type: Type of error (e.g., "ValueError")
            error_message: First line of exception message
            exit_code: Exit code for shell commands
            log_excerpt: Last few lines before failure

        Returns:
            TaskInstanceResult instance for failure
        """
        duration = int((end_time - start_time).total_seconds())

        return cls(
            task_instance_id=task_instance_id,
            status="FAILED",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            error_type=error_type,
            error_message=error_message,
            exit_code=exit_code,
            log_excerpt=log_excerpt,
        )
