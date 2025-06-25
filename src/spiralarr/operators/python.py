"""Python operator for executing Python functions with semantic logging."""

from contextlib import redirect_stderr, redirect_stdout
import inspect
from io import StringIO
import traceback
from typing import Any, Callable, Dict, List, Optional

from spiralarr.operators.base import BaseOperator
from spiralarr.utils.log_context import get_task_logger


class PythonOperator(BaseOperator):
    """
    Operator for executing Python functions.

    Example:
        def my_function():
            return "Hello World"

        task = PythonOperator(
            task_id="my_python_task",
            python_callable=my_function
        )
    """

    def __init__(
        self,
        python_callable: Callable,
        op_args: Optional[List[Any]] = None,
        op_kwargs: Optional[Dict[str, Any]] = None,
        provide_context: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # Validate python_callable
        if not callable(python_callable):
            raise ValueError("python_callable must be a callable object")

        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context

        # Inspect the callable to understand its signature
        self._callable_signature = inspect.signature(python_callable)
        self._callable_accepts_context = self._check_if_accepts_context()

    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute the Python callable with semantic output capture."""
        task_logger = get_task_logger()

        # Capture stdout and stderr
        stdout_capture = StringIO()
        stderr_capture = StringIO()

        try:
            if task_logger:
                task_logger.log_checkpoint(
                    f"Starting Python callable: {self.python_callable.__name__}"
                )

            # Execute with output capture
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                # Determine how to call the function based on its signature
                if self.provide_context or self._callable_accepts_context:
                    # Pass context as keyword argument
                    kwargs = self.op_kwargs.copy()
                    if "context" not in kwargs:
                        kwargs["context"] = context
                    result = self.python_callable(*self.op_args, **kwargs)
                else:
                    # Call without context
                    result = self.python_callable(*self.op_args, **self.op_kwargs)

            # Log captured output with semantic prefixes
            self._log_captured_output(stdout_capture, stderr_capture, task_logger)

            if task_logger:
                task_logger.log_checkpoint(
                    f"Completed Python callable: {self.python_callable.__name__}"
                )
                if result is not None:
                    task_logger.log_output("return_value", str(result))

            return result

        except Exception as e:
            # Log captured output even on failure
            self._log_captured_output(stdout_capture, stderr_capture, task_logger)

            # Log the exception with traceback
            if task_logger:
                tb_str = traceback.format_exc()
                task_logger.log_traceback(tb_str)

            raise RuntimeError(
                f"Python callable '{self.python_callable.__name__}' failed: {str(e)}"
            )

    def _check_if_accepts_context(self) -> bool:
        """Check if the callable accepts a 'context' parameter."""
        try:
            params = self._callable_signature.parameters
            return "context" in params or any(
                param.kind == param.VAR_KEYWORD for param in params.values()
            )
        except Exception:
            # If we can't inspect the signature, assume it doesn't accept context
            return False

    def _log_captured_output(
        self, stdout_capture: StringIO, stderr_capture: StringIO, task_logger
    ) -> None:
        """Log captured stdout and stderr with semantic prefixes."""
        if not task_logger:
            return

        # Log captured stdout
        stdout_content = stdout_capture.getvalue()
        if stdout_content.strip():
            for line in stdout_content.splitlines():
                if line.strip():  # Skip empty lines
                    task_logger.log_stdout(line)

        # Log captured stderr
        stderr_content = stderr_capture.getvalue()
        if stderr_content.strip():
            for line in stderr_content.splitlines():
                if line.strip():  # Skip empty lines
                    task_logger.log_stderr(line)

    def validate_parameters(self) -> None:
        """Validate operator parameters."""
        super().validate_parameters()

        # Validate that op_args is a list
        if not isinstance(self.op_args, list):
            raise ValueError("op_args must be a list")

        # Validate that op_kwargs is a dict
        if not isinstance(self.op_kwargs, dict):
            raise ValueError("op_kwargs must be a dictionary")

    def __repr__(self):
        return (
            f"<PythonOperator {self.task_id} callable={self.python_callable.__name__}>"
        )
