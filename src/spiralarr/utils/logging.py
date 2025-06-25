"""
Core logging configuration and utilities for Spiralarr.

This module provides the foundation for structured, semantic logging that
optimizes signal-to-noise ratio for LLM consumption.
"""

import logging
import sys

from spiralarr.utils.log_context import (
    log_checkpoint,
    log_metric,
    log_output,
)


class SpiralarrLogger:
    """
    Centralized logger for Spiralarr components with semantic capabilities.

    This logger provides both traditional logging and semantic logging features
    for optimal LLM consumption.
    """

    def __init__(self, name: str, level: int = logging.INFO):
        """
        Initialize Spiralarr logger.

        Args:
            name: Logger name (typically module name)
            level: Logging level
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Ensure we have a console handler for component logs
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def debug(self, message: str) -> None:
        """Log debug message."""
        self.logger.debug(message)

    def info(self, message: str) -> None:
        """Log info message."""
        self.logger.info(message)

    def warning(self, message: str) -> None:
        """Log warning message."""
        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Log error message."""
        self.logger.error(message)

    def critical(self, message: str) -> None:
        """Log critical message."""
        self.logger.critical(message)

    # Semantic logging methods
    def checkpoint(self, message: str) -> None:
        """Log a checkpoint for task progress tracking."""
        log_checkpoint(message)
        self.info(f"CHECKPOINT: {message}")

    def metric(self, name: str, value: any) -> None:
        """Log a metric for performance monitoring."""
        log_metric(name, value)
        self.info(f"METRIC: {name}={value}")

    def output(self, name: str, value: str) -> None:
        """Log an output artifact."""
        log_output(name, value)
        self.info(f"OUTPUT: {name}={value}")


# Component loggers
scheduler_logger = SpiralarrLogger("spiralarr.scheduler")
executor_logger = SpiralarrLogger("spiralarr.executor")
task_runner_logger = SpiralarrLogger("spiralarr.task_runner")
api_logger = SpiralarrLogger("spiralarr.api")


def get_logger(name: str) -> SpiralarrLogger:
    """
    Get a Spiralarr logger for a specific component.

    Args:
        name: Logger name

    Returns:
        SpiralarrLogger instance
    """
    return SpiralarrLogger(name)


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure global logging settings for Spiralarr.

    Args:
        level: Global logging level
    """
    # Configure root logger
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Set specific levels for different components
    logging.getLogger("spiralarr.scheduler").setLevel(level)
    logging.getLogger("spiralarr.executor").setLevel(level)
    logging.getLogger("spiralarr.task_runner").setLevel(level)
    logging.getLogger("spiralarr.api").setLevel(level)

    # Reduce noise from external libraries
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("alembic").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)


# Convenience functions for semantic logging
def log_task_start(dag_id: str, task_id: str, run_id: str) -> None:
    """Log task start with structured information."""
    task_runner_logger.checkpoint(f"Starting task {dag_id}.{task_id} (run: {run_id})")


def log_task_success(dag_id: str, task_id: str, run_id: str, duration: float) -> None:
    """Log task success with metrics."""
    task_runner_logger.checkpoint(f"Task {dag_id}.{task_id} completed successfully")
    task_runner_logger.metric("task_duration_seconds", duration)


def log_task_failure(dag_id: str, task_id: str, run_id: str, error: str) -> None:
    """Log task failure with error context."""
    task_runner_logger.error(f"Task {dag_id}.{task_id} failed: {error}")


def log_dag_run_start(dag_id: str, run_id: str, task_count: int) -> None:
    """Log DAG run start with metrics."""
    scheduler_logger.checkpoint(f"Starting DAG run {dag_id}.{run_id}")
    scheduler_logger.metric("dag_task_count", task_count)


def log_dag_run_complete(
    dag_id: str, run_id: str, duration: float, success_count: int, failure_count: int
) -> None:
    """Log DAG run completion with comprehensive metrics."""
    scheduler_logger.checkpoint(f"DAG run {dag_id}.{run_id} completed")
    scheduler_logger.metric("dag_run_duration_seconds", duration)
    scheduler_logger.metric("dag_successful_tasks", success_count)
    scheduler_logger.metric("dag_failed_tasks", failure_count)
    scheduler_logger.metric(
        "dag_success_rate",
        success_count / (success_count + failure_count) * 100
        if (success_count + failure_count) > 0
        else 0,
    )


def log_executor_stats(running_tasks: int, queued_tasks: int, capacity: int) -> None:
    """Log executor statistics."""
    executor_logger.metric("executor_running_tasks", running_tasks)
    executor_logger.metric("executor_queued_tasks", queued_tasks)
    executor_logger.metric("executor_capacity", capacity)
    executor_logger.metric(
        "executor_utilization", (running_tasks / capacity * 100) if capacity > 0 else 0
    )


def log_api_request(
    method: str, path: str, status_code: int, duration_ms: float
) -> None:
    """Log API request with performance metrics."""
    api_logger.info(f"{method} {path} - {status_code}")
    api_logger.metric("api_request_duration_ms", duration_ms)
    api_logger.metric("api_status_code", status_code)


# Error logging utilities
def log_database_error(operation: str, error: str) -> None:
    """Log database operation errors."""
    scheduler_logger.error(f"Database error during {operation}: {error}")


def log_dag_parsing_error(dag_file: str, error: str) -> None:
    """Log DAG parsing errors."""
    scheduler_logger.error(f"Failed to parse DAG file {dag_file}: {error}")


def log_task_timeout(dag_id: str, task_id: str, timeout_seconds: int) -> None:
    """Log task timeout events."""
    task_runner_logger.error(
        f"Task {dag_id}.{task_id} timed out after {timeout_seconds} seconds"
    )
    task_runner_logger.metric("task_timeout_seconds", timeout_seconds)


# System health logging
def log_system_health(
    active_dags: int,
    running_tasks: int,
    queued_tasks: int,
    failed_tasks_24h: int,
    avg_task_duration: float,
) -> None:
    """Log comprehensive system health metrics."""
    scheduler_logger.info("System health check")
    scheduler_logger.metric("system_active_dags", active_dags)
    scheduler_logger.metric("system_running_tasks", running_tasks)
    scheduler_logger.metric("system_queued_tasks", queued_tasks)
    scheduler_logger.metric("system_failed_tasks_24h", failed_tasks_24h)
    scheduler_logger.metric("system_avg_task_duration", avg_task_duration)


# Initialize logging on module import
configure_logging()
