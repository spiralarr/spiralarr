"""Context utilities for task execution in spiralarr."""

from typing import Any, Dict, Optional

from spiralarr.models.dag_run import DagRun
from spiralarr.models.task_instance import TaskInstance


class ExecutionContext:
    """
    Execution context for spiralarr tasks.

    This class provides a structured way to build and access execution context
    that gets passed to operators during task execution.
    """

    # Core context keys that should always be present
    CORE_KEYS = {
        "dag_id",
        "task_id",
        "run_id",
        "execution_date",
        "task_instance",
        "dag_run",
    }

    # Optional context keys that may be present
    OPTIONAL_KEYS = {
        "logical_date",  # Same as execution_date, for Airflow compatibility
        "ds",  # execution_date as YYYY-MM-DD string
        "ds_nodash",  # execution_date as YYYYMMDD string
        "params",  # DAG/task parameters
        "conf",  # Configuration values
        "var",  # Variable accessor (future)
        "conn",  # Connection accessor (future)
    }

    @classmethod
    def build_context(
        cls,
        task_instance: TaskInstance,
        dag_run: Optional[DagRun] = None,
        params: Optional[Dict[str, Any]] = None,
        **extra_context: Any,
    ) -> Dict[str, Any]:
        """
        Build execution context for a task instance.

        Args:
            task_instance: The TaskInstance being executed
            dag_run: The DagRun (will use task_instance.dag_run if not provided)
            params: Additional parameters to include in context
            **extra_context: Additional context variables

        Returns:
            Dictionary containing execution context
        """
        if dag_run is None:
            dag_run = task_instance.dag_run

        execution_date = dag_run.execution_date

        # Build core context
        context = {
            # Core identifiers
            "dag_id": dag_run.dag_id,
            "task_id": task_instance.task_id,
            "run_id": dag_run.run_id,
            # Execution timing
            "execution_date": execution_date,
            "logical_date": execution_date,  # Airflow 2.x compatibility
            # Date strings for convenience
            "ds": execution_date.strftime("%Y-%m-%d"),
            "ds_nodash": execution_date.strftime("%Y%m%d"),
            # Object references
            "task_instance": task_instance,
            "dag_run": dag_run,
            # Parameters and configuration
            "params": params or {},
            "conf": {},  # TODO: Add configuration access in future phases
            # Future accessors (placeholders for now)
            "var": None,  # TODO: Variable accessor
            "conn": None,  # TODO: Connection accessor
        }

        # Add any extra context provided
        context.update(extra_context)

        return context

    @classmethod
    def validate_context(cls, context: Dict[str, Any]) -> bool:
        """
        Validate that context contains all required keys.

        Args:
            context: Context dictionary to validate

        Returns:
            True if context is valid, False otherwise
        """
        missing_keys = cls.CORE_KEYS - set(context.keys())
        if missing_keys:
            return False
        return True

    @classmethod
    def get_context_summary(cls, context: Dict[str, Any]) -> str:
        """
        Get a summary string of the context for logging.

        Args:
            context: Context dictionary

        Returns:
            Summary string
        """
        return (
            f"Context(dag_id={context.get('dag_id')}, "
            f"task_id={context.get('task_id')}, "
            f"run_id={context.get('run_id')}, "
            f"execution_date={context.get('execution_date')})"
        )


def build_task_context(
    task_instance: TaskInstance, dag_run: Optional[DagRun] = None, **kwargs: Any
) -> Dict[str, Any]:
    """
    Convenience function to build task execution context.

    This is the main function that should be used by the task runner
    to build context for operator execution.

    Args:
        task_instance: The TaskInstance being executed
        dag_run: The DagRun (optional, will use task_instance.dag_run)
        **kwargs: Additional context variables

    Returns:
        Dictionary containing execution context
    """
    return ExecutionContext.build_context(
        task_instance=task_instance, dag_run=dag_run, **kwargs
    )


def validate_task_context(context: Dict[str, Any]) -> None:
    """
    Validate task execution context and raise exception if invalid.

    Args:
        context: Context dictionary to validate

    Raises:
        ValueError: If context is missing required keys
    """
    if not ExecutionContext.validate_context(context):
        missing_keys = ExecutionContext.CORE_KEYS - set(context.keys())
        raise ValueError(
            f"Invalid execution context. Missing required keys: {missing_keys}"
        )
