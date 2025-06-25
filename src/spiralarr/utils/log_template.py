"""
Log template system for Airflow-compatible log file naming.

This module provides template rendering for log file paths, supporting both
Python string formatting and Jinja2 templates for maximum compatibility.
"""

import os
from typing import Optional

import jinja2

from spiralarr.models.task_instance import TaskInstance


def parse_template_string(
    template_string: str,
) -> tuple[Optional[str], Optional[jinja2.Template]]:
    """
    Parse template string to determine if it's Jinja2 or Python string format.

    Args:
        template_string: Template string to parse

    Returns:
        Tuple of (python_template, jinja_template) where one is None
    """
    if "{{" in template_string:  # Jinja2 mode
        return None, jinja2.Template(template_string)
    else:  # Python string format mode
        return template_string, None


def render_log_filename(
    task_instance: TaskInstance, try_number: int, filename_template: str
) -> str:
    """
    Render log filename using task instance context and try number.

    Supports both Python string formatting and Jinja2 templates for compatibility
    with Airflow's log template system.

    Args:
        task_instance: TaskInstance object
        try_number: Attempt number for this task execution
        filename_template: Template string for filename

    Returns:
        Rendered filename path

    Examples:
        Python format: "{dag_id}/{run_id}/{task_id}/{try_number}.log"
    """
    python_template, jinja_template = parse_template_string(filename_template)

    if jinja_template:
        # Jinja2 template rendering
        context = {
            "ti": task_instance,
            "dag_id": task_instance.dag_id,
            "task_id": task_instance.task_id,
            "run_id": task_instance.dag_run.run_id
            if task_instance.dag_run
            else "unknown",
            "try_number": try_number,
            "map_index": -1,  # Spiralarr doesn't support mapped tasks yet
        }
        return jinja_template.render(context)

    else:
        # Python string formatting
        return python_template.format(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.dag_run.run_id if task_instance.dag_run else "unknown",
            try_number=try_number,
            map_index=-1,  # Spiralarr doesn't support mapped tasks yet
        )


def get_log_file_path(
    base_log_folder: str,
    task_instance: TaskInstance,
    try_number: int = 1,
    filename_template: Optional[str] = None,
) -> str:
    """
    Get the complete log file path for a task instance.

    Args:
        base_log_folder: Base directory for log files
        task_instance: TaskInstance object
        try_number: Attempt number (default: 1)
        filename_template: Custom template (uses default if None)

    Returns:
        Complete file path for the log file
    """
    if filename_template is None:
        # Default Airflow-compatible template
        filename_template = (
            "dag_id={dag_id}/run_id={run_id}/task_id={task_id}/"
            "map_index={map_index}/attempt={try_number}.log"
        )

    relative_path = render_log_filename(task_instance, try_number, filename_template)
    return os.path.join(base_log_folder, relative_path)


def ensure_log_directory(log_file_path: str) -> None:
    """
    Ensure the directory for the log file exists.

    Args:
        log_file_path: Complete path to the log file
    """
    log_dir = os.path.dirname(log_file_path)
    os.makedirs(log_dir, exist_ok=True)


class LogPathManager:
    """
    Manager for log file paths with configurable templates.

    This class provides a centralized way to manage log file paths
    with support for different templates and base directories.
    """

    def __init__(
        self, base_log_folder: str = "logs", filename_template: Optional[str] = None
    ):
        """
        Initialize log path manager.

        Args:
            base_log_folder: Base directory for log files
            filename_template: Template for log filenames
        """
        self.base_log_folder = base_log_folder
        self.filename_template = filename_template or (
            "dag_id={dag_id}/run_id={run_id}/task_id={task_id}/"
            "map_index={map_index}/attempt={try_number}.log"
        )

    def get_log_path(self, task_instance: TaskInstance, try_number: int = 1) -> str:
        """
        Get log file path for a task instance.

        Args:
            task_instance: TaskInstance object
            try_number: Attempt number

        Returns:
            Complete log file path
        """
        return get_log_file_path(
            self.base_log_folder, task_instance, try_number, self.filename_template
        )

    def ensure_log_dir(self, task_instance: TaskInstance, try_number: int = 1) -> str:
        """
        Ensure log directory exists and return the log file path.

        Args:
            task_instance: TaskInstance object
            try_number: Attempt number

        Returns:
            Complete log file path with directory created
        """
        log_path = self.get_log_path(task_instance, try_number)
        ensure_log_directory(log_path)
        return log_path

    def get_log_dir(self, task_instance: TaskInstance, try_number: int = 1) -> str:
        """
        Get the directory containing the log file.

        Args:
            task_instance: TaskInstance object
            try_number: Attempt number

        Returns:
            Directory path containing the log file
        """
        log_path = self.get_log_path(task_instance, try_number)
        return os.path.dirname(log_path)


# Default log path manager instance
default_log_manager = LogPathManager()


def get_task_log_path(task_instance: TaskInstance, try_number: int = 1) -> str:
    """
    Convenience function to get log path using default manager.

    Args:
        task_instance: TaskInstance object
        try_number: Attempt number

    Returns:
        Complete log file path
    """
    return default_log_manager.get_log_path(task_instance, try_number)


def ensure_task_log_dir(task_instance: TaskInstance, try_number: int = 1) -> str:
    """
    Convenience function to ensure log directory exists using default manager.

    Args:
        task_instance: TaskInstance object
        try_number: Attempt number

    Returns:
        Complete log file path with directory created
    """
    return default_log_manager.ensure_log_dir(task_instance, try_number)


# Template examples for reference
TEMPLATE_EXAMPLES = {
    "airflow_default": (
        "dag_id={dag_id}/run_id={run_id}/task_id={task_id}/"
        "map_index={map_index}/attempt={try_number}.log"
    ),
    "airflow_jinja": (
        "{{ ti.dag_id }}/{{ ti.run_id }}/{{ ti.task_id }}/"
        "{{ map_index }}/{{ try_number }}.log"
    ),
    "simple": "{dag_id}/{task_id}/{try_number}.log",
    "dated": "{dag_id}/{task_id}/{execution_date}/{try_number}.log",
    "hierarchical": "{dag_id}/runs/{run_id}/tasks/{task_id}/attempts/{try_number}.log",
}
