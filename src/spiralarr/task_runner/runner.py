"""
High-performance task runner with database self-updates.

PERFORMANCE BENEFITS:
- Task runners update their own state in database
- Immediate state consistency without scheduler polling
- Reduced scheduler workload
- Proper timestamp tracking
- Structured outcome tracking for LLM consumption
"""

from datetime import datetime
import json
import traceback

from spiralarr.models.base import get_session
from spiralarr.models.task_instance import TaskInstance
from spiralarr.models.task_instance_result import TaskInstanceResult
from spiralarr.operators.bash import BashOperator
from spiralarr.operators.python import PythonOperator
from spiralarr.utils.context import build_task_context, validate_task_context
from spiralarr.utils.dag_loader import load_task_operator
from spiralarr.utils.log_context import clear_task_logger, set_task_logger
from spiralarr.utils.logging import (
    log_task_failure,
    log_task_start,
    log_task_success,
    task_runner_logger,
)
from spiralarr.utils.state import State


class TaskRunner:
    """
    High-Performance Task Runner - the 3rd component of the execution system.

    PERFORMANCE FEATURES:
    - Database self-updates for immediate state consistency
    - Proper timestamp tracking (start_date, end_date)
    - Reduced scheduler workload through autonomous state management
    - Immediate visibility of task state changes

    Responsible for:
    1. Loading the specific task object from DAG file
    2. Self-updating TaskInstance state to RUNNING with timestamp
    3. Executing the task's operator
    4. Self-updating TaskInstance state to SUCCESS/FAILED with timestamp
    """

    def run_task(self, dag_id: str, task_id: str, run_id: str) -> bool:
        """
        Run a specific task instance.

        Returns:
            True if task succeeded, False if failed
        """
        session = get_session()
        try:
            # Find the task instance
            task_instance = (
                session.query(TaskInstance)
                .join(TaskInstance.dag_run)
                .filter(
                    TaskInstance.task_id == task_id,
                    TaskInstance.dag_run.has(dag_id=dag_id, run_id=run_id),
                )
                .first()
            )

            if not task_instance:
                task_runner_logger.error(
                    f"Task instance not found: {dag_id}.{task_id}.{run_id}"
                )
                return False

            # Set up semantic logging for this task
            set_task_logger(task_instance, 1)

            # Log task start
            log_task_start(dag_id, task_id, run_id)

            # DATABASE SELF-UPDATE: Update state to RUNNING with timestamp
            task_instance.set_state(State.RUNNING)
            task_instance.start_date = datetime.utcnow()
            session.commit()
            task_runner_logger.info(
                f"Task {dag_id}.{task_id}.{run_id} state updated to RUNNING"
            )

            # Create execution context
            context = build_task_context(task_instance)
            validate_task_context(context)

            # Load the actual operator from DAG file
            operator = load_task_operator(dag_id, task_id)
            if not operator:
                # Fallback to dummy operator for backward compatibility
                task_runner_logger.warning(
                    f"Could not load operator for {dag_id}.{task_id}, "
                    f"using dummy operator"
                )
                operator = self._create_dummy_operator(task_id)

            # Execute the task with proper lifecycle management
            try:
                result = self._execute_task_with_lifecycle(
                    operator, context, task_instance, session
                )
                return result

            except Exception as e:
                # Task failed with unexpected error
                log_task_failure(dag_id, task_id, run_id, str(e))
                try:
                    # DATABASE SELF-UPDATE: Unexpected failure
                    task_instance.set_state(State.FAILED)
                    task_instance.end_date = datetime.utcnow()

                    # Create structured failure result for unexpected errors
                    error_type = type(e).__name__
                    error_message = str(e).split("\n")[0]
                    tb_lines = traceback.format_exc().split("\n")
                    log_excerpt = (
                        "\n".join(tb_lines[-6:])
                        if len(tb_lines) > 6
                        else traceback.format_exc()
                    )

                    task_result = TaskInstanceResult.create_failure_result(
                        task_instance_id=task_instance.id,
                        start_time=task_instance.start_date,
                        end_time=task_instance.end_date,
                        error_type=error_type,
                        error_message=error_message,
                        log_excerpt=log_excerpt,
                    )
                    session.add(task_result)
                    session.commit()
                    task_runner_logger.info(
                        f"Task {dag_id}.{task_id} state updated to FAILED"
                    )
                except Exception as commit_error:
                    task_runner_logger.error(
                        f"Failed to update task state after error: {commit_error}"
                    )
                    session.rollback()
                return False

        except Exception as e:
            task_runner_logger.error(f"Task runner error: {e}")
            session.rollback()
            return False

        finally:
            clear_task_logger()
            session.close()

    def _execute_task_with_lifecycle(
        self, operator, context, task_instance, session
    ) -> bool:
        """
        Execute a task with proper lifecycle management.

        This includes pre-execute hooks, execution, post-execute hooks,
        and proper state management.
        """
        dag_id = context["dag_id"]
        task_id = context["task_id"]

        try:
            # Pre-execution phase
            task_runner_logger.info(
                f"Starting pre-execution for task {dag_id}.{task_id}"
            )
            operator.pre_execute(context)

            # Validate operator parameters
            operator.validate_parameters()

            # Main execution phase
            task_runner_logger.info(f"Executing task {dag_id}.{task_id}")
            result = operator.execute(context)

            # Post-execution phase
            task_runner_logger.info(
                f"Starting post-execution for task {dag_id}.{task_id}"
            )
            operator.post_execute(context, result)

            # DATABASE SELF-UPDATE: Task succeeded
            task_instance.set_state(State.SUCCESS)
            task_instance.end_date = datetime.utcnow()

            # Create structured result record for LLM consumption
            return_value_json = None
            if result is not None:
                try:
                    return_value_json = (
                        json.dumps(result) if not isinstance(result, str) else result
                    )
                except (TypeError, ValueError):
                    return_value_json = str(result)

            task_result = TaskInstanceResult.create_success_result(
                task_instance_id=task_instance.id,
                start_time=task_instance.start_date,
                end_time=task_instance.end_date,
                return_value=return_value_json,
            )
            session.add(task_result)
            session.commit()

            execution_time = (
                task_instance.end_date - task_instance.start_date
            ).total_seconds()

            # Log success with structured logging
            log_task_success(dag_id, task_id, context["run_id"], execution_time)

            if result is not None:
                task_runner_logger.info(f"Task result: {result}")

            return True

        except Exception as e:
            # Task failed during execution
            log_task_failure(dag_id, task_id, context["run_id"], str(e))

            try:
                # DATABASE SELF-UPDATE: Task failed
                task_instance.set_state(State.FAILED)
                task_instance.end_date = datetime.utcnow()

                # Create structured failure result for LLM consumption
                error_type = type(e).__name__
                error_message = str(e).split("\n")[0]  # First line only

                # Capture last few lines of traceback as log excerpt
                tb_lines = traceback.format_exc().split("\n")
                log_excerpt = (
                    "\n".join(tb_lines[-6:])
                    if len(tb_lines) > 6
                    else traceback.format_exc()
                )

                # Extract exit code for bash operators
                exit_code = None
                if hasattr(e, "returncode"):
                    exit_code = e.returncode
                elif (
                    isinstance(operator, BashOperator)
                    and hasattr(e, "args")
                    and len(e.args) > 0
                ):
                    # Try to extract exit code from error message
                    try:
                        if "exit code" in str(e).lower():
                            import re

                            match = re.search(r"exit code (\d+)", str(e), re.IGNORECASE)
                            if match:
                                exit_code = int(match.group(1))
                    except (ValueError, AttributeError):
                        pass

                task_result = TaskInstanceResult.create_failure_result(
                    task_instance_id=task_instance.id,
                    start_time=task_instance.start_date,
                    end_time=task_instance.end_date,
                    error_type=error_type,
                    error_message=error_message,
                    exit_code=exit_code,
                    log_excerpt=log_excerpt,
                )
                session.add(task_result)
                session.commit()

                if task_instance.start_date:
                    execution_time = (
                        task_instance.end_date - task_instance.start_date
                    ).total_seconds()
                    task_runner_logger.info(
                        f"Task {dag_id}.{task_id} failed after {execution_time:.2f}s"
                    )
                else:
                    task_runner_logger.info(f"Task {dag_id}.{task_id} failed")
            except Exception as state_error:
                task_runner_logger.error(f"Failed to update task state: {state_error}")
                session.rollback()

            return False

    def _create_dummy_operator(self, task_id: str):
        """Create a dummy operator for Phase 1 testing."""
        # Simple demo operators for different task types
        if "bash" in task_id.lower():
            return BashOperator(
                task_id=task_id, bash_command="echo 'Hello from bash task'"
            )
        elif "python" in task_id.lower():

            def dummy_python_task():
                return f"Hello from python task {task_id}"

            return PythonOperator(task_id=task_id, python_callable=dummy_python_task)
        else:
            # Default to a simple python operator
            def default_task():
                return f"Task {task_id} executed successfully"

            return PythonOperator(task_id=task_id, python_callable=default_task)
