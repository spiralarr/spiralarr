"""Base operator class for all spiralarr operators."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from spiralarr.utils.context import ExecutionContext


class BaseOperator(ABC):
    """
    Base class for all operators in spiralarr.

    Operators define what actually gets executed when a task runs.
    """

    def __init__(
        self,
        task_id: str,
        dag_id: Optional[str] = None,
        depends_on_past: bool = False,
        retries: int = 0,
        retry_delay_seconds: int = 300,
        **kwargs,
    ):
        # Validate task_id
        if not task_id or not isinstance(task_id, str):
            raise ValueError("task_id must be a non-empty string")

        # Validate retries
        if retries < 0:
            raise ValueError("retries must be non-negative")

        # Validate retry_delay_seconds
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be non-negative")

        self.task_id = task_id
        self.dag_id = dag_id
        self.depends_on_past = depends_on_past
        self.retries = retries
        self.retry_delay_seconds = retry_delay_seconds

        # Store additional kwargs for subclasses
        self.kwargs = kwargs

        # Operator metadata
        self._operator_name = self.__class__.__name__

    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute the operator's main logic.

        Args:
            context: Dictionary containing execution context information
                    like dag_run, task_instance, execution_date, etc.

        Returns:
            Any value that should be stored as the task's return value
        """
        pass

    def pre_execute(self, context: Dict[str, Any]) -> None:
        """Hook that runs before execute(). Override in subclasses if needed."""
        # Validate context has required keys
        if not ExecutionContext.validate_context(context):
            missing_keys = ExecutionContext.CORE_KEYS - set(context.keys())
            raise ValueError(f"Invalid execution context. Missing keys: {missing_keys}")

    def post_execute(self, context: Dict[str, Any], result: Any) -> None:
        """Hook that runs after execute(). Override in subclasses if needed."""
        # Log execution completion
        print(f"Task {self.task_id} completed successfully")

    def get_task_key(self) -> str:
        """Get a unique key for this task."""
        if self.dag_id:
            return f"{self.dag_id}.{self.task_id}"
        return self.task_id

    def validate_parameters(self) -> None:
        """Validate operator parameters.

        Override in subclasses for custom validation.
        """
        pass

    @property
    def operator_name(self) -> str:
        """Get the operator class name."""
        return self._operator_name

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.task_id}>"
