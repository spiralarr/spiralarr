"""
Task log handler with semantic prefixes for LLM-optimized log parsing.

This module implements file-based logging with structured prefixes that allow
LLM agents to distinguish between system logs and user-generated output.
"""

from contextlib import contextmanager
from io import StringIO
import logging
import sys
from typing import Optional, TextIO

from spiralarr.models.task_instance import TaskInstance
from spiralarr.utils.log_template import LogPathManager


class SemanticPrefix:
    """Semantic prefixes for different types of log content."""

    STDOUT = "[STDOUT]"
    STDERR = "[STDERR]"
    METRIC = "[METRIC]"
    OUTPUT = "[OUTPUT]"
    CHECKPOINT = "[CHECKPOINT]"
    TRACEBACK = "[TRACEBACK]"
    SYSTEM = "[SYSTEM]"


class TaskLogHandler(logging.Handler):
    """
    File-based log handler with semantic prefixes for task execution.

    This handler writes logs to files using Airflow-compatible naming conventions
    and adds semantic prefixes to distinguish between different types of content.
    """

    def __init__(
        self,
        base_log_folder: str = "logs",
        filename_template: Optional[str] = None,
        level: int = logging.INFO,
    ):
        """
        Initialize the task log handler.

        Args:
            base_log_folder: Base directory for log files
            filename_template: Template for log file naming
            level: Logging level
        """
        super().__init__(level)
        self.log_manager = LogPathManager(base_log_folder, filename_template)
        self.current_task_instance: Optional[TaskInstance] = None
        self.current_try_number: int = 1
        self.file_handler: Optional[logging.FileHandler] = None

        # Set up formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.setFormatter(formatter)

    def set_context(self, task_instance: TaskInstance, try_number: int = 1) -> None:
        """
        Set the task context for logging.

        Args:
            task_instance: TaskInstance to log for
            try_number: Attempt number
        """
        self.current_task_instance = task_instance
        self.current_try_number = try_number

        # Close existing handler if any
        if self.file_handler:
            self.file_handler.close()
            self.file_handler = None

        # Create new file handler for this task
        if task_instance:
            log_path = self.log_manager.ensure_log_dir(task_instance, try_number)
            self.file_handler = logging.FileHandler(log_path, mode="a")
            self.file_handler.setFormatter(self.formatter)

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the task log file.

        Args:
            record: Log record to emit
        """
        if self.file_handler:
            self.file_handler.emit(record)

    def log_with_prefix(
        self, prefix: str, message: str, level: int = logging.INFO
    ) -> None:
        """
        Log a message with a semantic prefix.

        Args:
            prefix: Semantic prefix (e.g., SemanticPrefix.STDOUT)
            message: Message to log
            level: Log level
        """
        prefixed_message = f"{prefix} {message}"
        record = logging.LogRecord(
            name="spiralarr.task",
            level=level,
            pathname="",
            lineno=0,
            msg=prefixed_message,
            args=(),
            exc_info=None,
        )
        self.emit(record)

    def log_stdout(self, message: str) -> None:
        """Log stdout content with semantic prefix."""
        self.log_with_prefix(SemanticPrefix.STDOUT, message)

    def log_stderr(self, message: str) -> None:
        """Log stderr content with semantic prefix."""
        self.log_with_prefix(SemanticPrefix.STDERR, message, logging.WARNING)

    def log_metric(self, name: str, value: any) -> None:
        """Log a metric with semantic prefix."""
        metric_data = f'{{"name": "{name}", "value": {value}}}'
        self.log_with_prefix(SemanticPrefix.METRIC, metric_data)

    def log_output(self, name: str, value: str) -> None:
        """Log an output artifact with semantic prefix."""
        output_data = f'{{"name": "{name}", "value": "{value}"}}'
        self.log_with_prefix(SemanticPrefix.OUTPUT, output_data)

    def log_checkpoint(self, message: str) -> None:
        """Log a checkpoint with semantic prefix."""
        self.log_with_prefix(SemanticPrefix.CHECKPOINT, message)

    def log_traceback(self, traceback_str: str) -> None:
        """Log a traceback with semantic prefix."""
        self.log_with_prefix(SemanticPrefix.TRACEBACK, traceback_str, logging.ERROR)

    def log_system(self, message: str) -> None:
        """Log a system message with semantic prefix."""
        self.log_with_prefix(SemanticPrefix.SYSTEM, message)

    def close(self) -> None:
        """Close the log handler and any open file handlers."""
        if self.file_handler:
            self.file_handler.close()
            self.file_handler = None
        super().close()


class OutputCapture:
    """
    Context manager for capturing stdout/stderr with semantic prefixes.

    This class captures output from user code and redirects it to the task
    log handler with appropriate semantic prefixes.
    """

    def __init__(self, log_handler: TaskLogHandler):
        """
        Initialize output capture.

        Args:
            log_handler: TaskLogHandler to send captured output to
        """
        self.log_handler = log_handler
        self.original_stdout: Optional[TextIO] = None
        self.original_stderr: Optional[TextIO] = None
        self.stdout_capture = StringIO()
        self.stderr_capture = StringIO()

    def __enter__(self):
        """Start capturing stdout/stderr."""
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr

        # Replace stdout/stderr with our capture objects
        sys.stdout = self.stdout_capture
        sys.stderr = self.stderr_capture

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop capturing and log the captured output."""
        # Restore original stdout/stderr
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr

        # Log captured stdout
        stdout_content = self.stdout_capture.getvalue()
        if stdout_content.strip():
            for line in stdout_content.splitlines():
                if line.strip():  # Skip empty lines
                    self.log_handler.log_stdout(line)

        # Log captured stderr
        stderr_content = self.stderr_capture.getvalue()
        if stderr_content.strip():
            for line in stderr_content.splitlines():
                if line.strip():  # Skip empty lines
                    self.log_handler.log_stderr(line)


@contextmanager
def capture_task_output(task_instance: TaskInstance, try_number: int = 1):
    """
    Context manager for capturing task output with semantic logging.

    Args:
        task_instance: TaskInstance to log for
        try_number: Attempt number

    Yields:
        TaskLogHandler configured for the task

    Example:
        with capture_task_output(task_instance) as log_handler:
            log_handler.log_checkpoint("Starting data processing")
            print("This will be logged with [STDOUT] prefix")
            log_handler.log_metric("rows_processed", 1000)
    """
    log_handler = TaskLogHandler()
    log_handler.set_context(task_instance, try_number)

    try:
        with OutputCapture(log_handler):
            yield log_handler
    finally:
        log_handler.close()


# Global task log handler for use throughout the application
_global_task_log_handler: Optional[TaskLogHandler] = None


def get_task_logger() -> Optional[TaskLogHandler]:
    """Get the current global task log handler."""
    return _global_task_log_handler


def set_task_logger(task_instance: TaskInstance, try_number: int = 1) -> TaskLogHandler:
    """
    Set up global task logger for the current task.

    Args:
        task_instance: TaskInstance to log for
        try_number: Attempt number

    Returns:
        Configured TaskLogHandler
    """
    global _global_task_log_handler

    if _global_task_log_handler:
        _global_task_log_handler.close()

    _global_task_log_handler = TaskLogHandler()
    _global_task_log_handler.set_context(task_instance, try_number)
    return _global_task_log_handler


def clear_task_logger() -> None:
    """Clear the global task logger."""
    global _global_task_log_handler

    if _global_task_log_handler:
        _global_task_log_handler.close()
        _global_task_log_handler = None


# Convenience functions for semantic logging
def log_metric(name: str, value: any) -> None:
    """Log a metric using the global task logger."""
    if _global_task_log_handler:
        _global_task_log_handler.log_metric(name, value)


def log_output(name: str, value: str) -> None:
    """Log an output using the global task logger."""
    if _global_task_log_handler:
        _global_task_log_handler.log_output(name, value)


def log_checkpoint(message: str) -> None:
    """Log a checkpoint using the global task logger."""
    if _global_task_log_handler:
        _global_task_log_handler.log_checkpoint(message)
