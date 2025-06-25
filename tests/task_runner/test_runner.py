"""Unit tests for spiralarr task runner."""

import datetime as dt
from unittest.mock import MagicMock, call, patch

from spiralarr.operators.bash import BashOperator
from spiralarr.operators.python import PythonOperator
from spiralarr.task_runner.runner import TaskRunner
from spiralarr.utils.state import State


class TestTaskRunner:
    """Test cases for TaskRunner."""

    @patch("spiralarr.task_runner.runner.get_session")
    @patch("spiralarr.utils.dag_loader.load_task_operator")
    def test_run_task_success(self, mock_load_operator, mock_get_session):
        """Test successful task execution."""
        # Mock database session
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        # Mock DAG run and task instance
        mock_dag_run = MagicMock()
        mock_dag_run.dag_id = "test_dag"
        mock_dag_run.run_id = "test_run"
        mock_dag_run.execution_date = dt.datetime.now(dt.timezone.utc)

        mock_task_instance = MagicMock()
        mock_task_instance.task_id = "test_task"
        mock_task_instance.dag_run = mock_dag_run
        mock_task_instance.set_state = MagicMock()

        # Mock query result
        mock_query = MagicMock()
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = mock_task_instance
        mock_session.query.return_value = mock_query

        # Mock operator
        def test_func():
            return "success"

        mock_operator = PythonOperator(task_id="test_task", python_callable=test_func)
        mock_load_operator.return_value = mock_operator

        # Run the task
        runner = TaskRunner()
        result = runner.run_task("test_dag", "test_task", "test_run")

        # Verify success
        assert result is True

        # Verify state transitions
        mock_task_instance.set_state.assert_has_calls(
            [call(State.RUNNING), call(State.SUCCESS)]
        )

        # Verify session commits
        assert mock_session.commit.call_count == 2
        mock_session.close.assert_called_once()

    @patch("spiralarr.task_runner.runner.get_session")
    @patch("spiralarr.utils.dag_loader.load_task_operator")
    def test_run_task_not_found(self, mock_load_operator, mock_get_session):
        """Test handling of task instance not found."""
        # Mock database session
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        # Mock query returning None (task not found)
        mock_query = MagicMock()
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query

        # Run the task
        runner = TaskRunner()
        result = runner.run_task("test_dag", "test_task", "test_run")

        # Verify failure
        assert result is False
        mock_session.close.assert_called_once()

    def test_run_task_operator_failure_direct(self):
        """Test handling of operator execution failure using direct lifecycle test."""
        # Mock session
        mock_session = MagicMock()

        # Mock task instance
        mock_task_instance = MagicMock()
        mock_task_instance.set_state = MagicMock()

        # Mock operator that fails during execution
        mock_operator = MagicMock()
        mock_operator.pre_execute = MagicMock()
        mock_operator.validate_parameters = MagicMock()
        mock_operator.execute = MagicMock(side_effect=ValueError("Test error"))

        # Mock context
        context = {"dag_id": "test_dag", "task_id": "test_task", "run_id": "test_run"}

        # Test the lifecycle directly
        runner = TaskRunner()
        result = runner._execute_task_with_lifecycle(
            mock_operator, context, mock_task_instance, mock_session
        )

        # Verify failure
        assert result is False

        # Verify lifecycle calls
        mock_operator.pre_execute.assert_called_once_with(context)
        mock_operator.validate_parameters.assert_called_once()
        mock_operator.execute.assert_called_once_with(context)

        # post_execute should not be called on failure
        mock_operator.post_execute.assert_not_called()

        # Verify state update
        mock_task_instance.set_state.assert_called_once_with(State.FAILED)
        mock_session.commit.assert_called_once()

    @patch("spiralarr.task_runner.runner.get_session")
    @patch("spiralarr.utils.dag_loader.load_task_operator")
    def test_run_task_no_operator_fallback(self, mock_load_operator, mock_get_session):
        """Test fallback to dummy operator when DAG loading fails."""
        # Mock database session
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        # Mock DAG run and task instance
        mock_dag_run = MagicMock()
        mock_dag_run.dag_id = "test_dag"
        mock_dag_run.run_id = "test_run"
        mock_dag_run.execution_date = dt.datetime.now(dt.timezone.utc)

        mock_task_instance = MagicMock()
        mock_task_instance.task_id = "python_task"  # Will trigger python dummy
        mock_task_instance.dag_run = mock_dag_run
        mock_task_instance.set_state = MagicMock()

        # Mock query result
        mock_query = MagicMock()
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = mock_task_instance
        mock_session.query.return_value = mock_query

        # Mock operator loading failure
        mock_load_operator.return_value = None

        # Run the task
        runner = TaskRunner()
        result = runner.run_task("test_dag", "python_task", "test_run")

        # Should still succeed with dummy operator
        assert result is True

        # Verify state transitions
        mock_task_instance.set_state.assert_has_calls(
            [call(State.RUNNING), call(State.SUCCESS)]
        )

    @patch("spiralarr.task_runner.runner.get_session")
    def test_run_task_database_error(self, mock_get_session):
        """Test handling of database errors."""
        # Mock database session that raises an error
        mock_session = MagicMock()
        mock_session.query.side_effect = Exception("Database error")
        mock_get_session.return_value = mock_session

        # Run the task
        runner = TaskRunner()
        result = runner.run_task("test_dag", "test_task", "test_run")

        # Verify failure
        assert result is False
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()

    def test_create_dummy_operator_bash(self):
        """Test creation of dummy bash operator."""
        runner = TaskRunner()
        operator = runner._create_dummy_operator("bash_task")

        assert isinstance(operator, BashOperator)
        assert operator.task_id == "bash_task"
        assert "echo" in operator.bash_command

    def test_create_dummy_operator_python(self):
        """Test creation of dummy python operator."""
        runner = TaskRunner()
        operator = runner._create_dummy_operator("python_task")

        assert isinstance(operator, PythonOperator)
        assert operator.task_id == "python_task"
        assert callable(operator.python_callable)

    def test_create_dummy_operator_default(self):
        """Test creation of default dummy operator."""
        runner = TaskRunner()
        operator = runner._create_dummy_operator("generic_task")

        assert isinstance(operator, PythonOperator)
        assert operator.task_id == "generic_task"
        assert callable(operator.python_callable)

    @patch("spiralarr.task_runner.runner.get_session")
    @patch("spiralarr.utils.dag_loader.load_task_operator")
    def test_execute_task_with_lifecycle_success(
        self, mock_load_operator, mock_get_session
    ):
        """Test the task execution lifecycle with success."""
        # Mock session
        mock_session = MagicMock()

        # Mock task instance
        mock_task_instance = MagicMock()
        mock_task_instance.set_state = MagicMock()
        # Mock datetime fields to avoid format string errors (use utcnow())
        mock_task_instance.start_date = dt.datetime.utcnow()
        mock_task_instance.end_date = dt.datetime.utcnow()

        # Mock operator
        mock_operator = MagicMock()
        mock_operator.pre_execute = MagicMock()
        mock_operator.execute = MagicMock(return_value="test_result")
        mock_operator.post_execute = MagicMock()
        mock_operator.validate_parameters = MagicMock()

        # Mock context
        context = {"dag_id": "test_dag", "task_id": "test_task", "run_id": "test_run"}

        # Test the lifecycle
        runner = TaskRunner()
        result = runner._execute_task_with_lifecycle(
            mock_operator, context, mock_task_instance, mock_session
        )

        # Verify success
        assert result is True

        # Verify lifecycle calls
        mock_operator.pre_execute.assert_called_once_with(context)
        mock_operator.validate_parameters.assert_called_once()
        mock_operator.execute.assert_called_once_with(context)
        mock_operator.post_execute.assert_called_once_with(context, "test_result")

        # Verify state update
        mock_task_instance.set_state.assert_called_once_with(State.SUCCESS)
        mock_session.commit.assert_called_once()

    @patch("spiralarr.task_runner.runner.get_session")
    def test_execute_task_with_lifecycle_failure(self, mock_get_session):
        """Test the task execution lifecycle with failure."""
        # Mock session
        mock_session = MagicMock()

        # Mock task instance
        mock_task_instance = MagicMock()
        mock_task_instance.set_state = MagicMock()

        # Mock operator that fails during execution
        mock_operator = MagicMock()
        mock_operator.pre_execute = MagicMock()
        mock_operator.validate_parameters = MagicMock()
        mock_operator.execute = MagicMock(side_effect=ValueError("Test error"))

        # Mock context
        context = {"dag_id": "test_dag", "task_id": "test_task", "run_id": "test_run"}

        # Test the lifecycle
        runner = TaskRunner()
        result = runner._execute_task_with_lifecycle(
            mock_operator, context, mock_task_instance, mock_session
        )

        # Verify failure
        assert result is False

        # Verify lifecycle calls
        mock_operator.pre_execute.assert_called_once_with(context)
        mock_operator.validate_parameters.assert_called_once()
        mock_operator.execute.assert_called_once_with(context)

        # post_execute should not be called on failure
        mock_operator.post_execute.assert_not_called()

        # Verify state update
        mock_task_instance.set_state.assert_called_once_with(State.FAILED)
        mock_session.commit.assert_called_once()

    @patch("spiralarr.task_runner.runner.get_session")
    def test_execute_task_with_lifecycle_state_update_failure(self, mock_get_session):
        """Test handling of state update failure during task execution."""
        # Mock session that fails on commit
        mock_session = MagicMock()
        mock_session.commit.side_effect = Exception("Commit failed")

        # Mock task instance
        mock_task_instance = MagicMock()
        mock_task_instance.set_state = MagicMock()

        # Mock successful operator
        mock_operator = MagicMock()
        mock_operator.pre_execute = MagicMock()
        mock_operator.validate_parameters = MagicMock()
        mock_operator.execute = MagicMock(return_value="success")
        mock_operator.post_execute = MagicMock()

        # Mock context
        context = {"dag_id": "test_dag", "task_id": "test_task", "run_id": "test_run"}

        # Test the lifecycle
        runner = TaskRunner()
        result = runner._execute_task_with_lifecycle(
            mock_operator, context, mock_task_instance, mock_session
        )

        # Should return False since commit failed
        assert result is False

        # Verify rollback was called
        mock_session.rollback.assert_called_once()
