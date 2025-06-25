"""Bash operator for executing shell commands with semantic logging."""

import os
import subprocess
from typing import Any, Dict, Optional

from spiralarr.operators.base import BaseOperator
from spiralarr.utils.log_context import get_task_logger


class BashOperator(BaseOperator):
    """
    Operator for executing bash commands.

    Example:
        task = BashOperator(
            task_id="my_bash_task",
            bash_command="echo 'Hello World'"
        )
    """

    def __init__(
        self,
        bash_command: str,
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        timeout: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # Validate bash_command
        if not bash_command or not isinstance(bash_command, str):
            raise ValueError("bash_command must be a non-empty string")

        self.bash_command = bash_command.strip()
        self.env = env or {}
        self.cwd = cwd
        self.timeout = timeout

    def execute(self, context: Dict[str, Any]) -> str:
        """Execute the bash command with semantic logging."""
        task_logger = get_task_logger()

        # Template the command with context variables
        templated_command = self._template_command(context)

        # Prepare environment variables
        env = os.environ.copy()
        env.update(self.env)

        # Add some useful context variables to environment
        env.update(
            {
                "SPIRALARR_DAG_ID": context.get("dag_id", ""),
                "SPIRALARR_TASK_ID": context.get("task_id", ""),
                "SPIRALARR_RUN_ID": context.get("run_id", ""),
                "SPIRALARR_EXECUTION_DATE": str(context.get("execution_date", "")),
            }
        )

        if task_logger:
            task_logger.log_checkpoint(f"Executing bash command: {templated_command}")

        try:
            result = subprocess.run(
                templated_command,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
                env=env,
                cwd=self.cwd,
                timeout=self.timeout,
            )

            # Log the output with semantic prefixes
            if result.stdout and task_logger:
                for line in result.stdout.splitlines():
                    if line.strip():
                        task_logger.log_stdout(line)

            if result.stderr and task_logger:
                for line in result.stderr.splitlines():
                    if line.strip():
                        task_logger.log_stderr(line)

            if task_logger:
                task_logger.log_checkpoint("Bash command completed successfully")
                task_logger.log_output("command_output", result.stdout.strip())

            return result.stdout.strip()

        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"Bash command timed out after {self.timeout} seconds. "
                f"Command: {templated_command}"
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Bash command failed with return code {e.returncode}. "
                f"Command: {templated_command}, "
                f"stdout: {e.stdout}, stderr: {e.stderr}"
            )

    def _template_command(self, context: Dict[str, Any]) -> str:
        """
        Simple templating of bash command with context variables.

        Replaces {{ variable_name }} with values from context.
        """
        command = self.bash_command

        # Simple template replacement for common variables
        replacements = {
            "{{ dag_id }}": context.get("dag_id", ""),
            "{{ task_id }}": context.get("task_id", ""),
            "{{ run_id }}": context.get("run_id", ""),
            "{{ execution_date }}": str(context.get("execution_date", "")),
            "{{ ds }}": context.get("ds", ""),
            "{{ ds_nodash }}": context.get("ds_nodash", ""),
        }

        for template, value in replacements.items():
            command = command.replace(template, value)

        return command

    def validate_parameters(self) -> None:
        """Validate operator parameters."""
        super().validate_parameters()

        # Validate timeout
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be positive")

        # Validate cwd exists if specified
        if self.cwd and not os.path.isdir(self.cwd):
            raise ValueError(f"Working directory does not exist: {self.cwd}")

    def __repr__(self):
        return f"<BashOperator {self.task_id} command='{self.bash_command[:50]}...'>"
