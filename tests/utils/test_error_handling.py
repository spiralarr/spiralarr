"""Tests for error handling and edge cases in spiralarr."""

import os
import subprocess
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from spiralarr.executors.local import LocalExecutor
from spiralarr.executors.mockable_executor import MockableLocalExecutor
from spiralarr.models.task_instance import TaskInstance
from spiralarr.operators.bash import BashOperator
from spiralarr.operators.python import PythonOperator
from spiralarr.task_runner.runner import TaskRunner
from spiralarr.utils.dag_loader import DagLoader
from spiralarr.utils.state import State


class TestOperatorErrorHandling:
    """Test error handling in operators."""

    def test_python_operator_function_exception(self):
        """Test PythonOperator handling of function exceptions."""

        def failing_function():
            raise ValueError("Test error message")

        operator = PythonOperator(
            task_id="failing_task", python_callable=failing_function
        )

        context = {"dag_id": "test", "task_id": "failing_task"}

        with pytest.raises(
            RuntimeError, match="Python callable 'failing_function' failed"
        ):
            operator.execute(context)

    def test_python_operator_function_with_args_exception(self):
        """Test PythonOperator with arguments that cause exceptions."""

        def divide_function(a, b):
            return a / b

        operator = PythonOperator(
            task_id="divide_task",
            python_callable=divide_function,
            op_args=[10, 0],  # Division by zero
        )

        context = {"dag_id": "test", "task_id": "divide_task"}

        with pytest.raises(RuntimeError):
            operator.execute(context)

    def test_python_operator_invalid_callable_type(self):
        """Test PythonOperator with invalid callable type."""
        with pytest.raises(
            ValueError, match="python_callable must be a callable object"
        ):
            PythonOperator(task_id="test", python_callable="not_a_function")

    def test_python_operator_invalid_args_type(self):
        """Test PythonOperator with invalid op_args type."""

        def test_func():
            pass

        operator = PythonOperator(task_id="test", python_callable=test_func)
        operator.op_args = "not_a_list"

        with pytest.raises(ValueError, match="op_args must be a list"):
            operator.validate_parameters()

    def test_python_operator_invalid_kwargs_type(self):
        """Test PythonOperator with invalid op_kwargs type."""

        def test_func():
            pass

        operator = PythonOperator(task_id="test", python_callable=test_func)
        operator.op_kwargs = "not_a_dict"

        with pytest.raises(ValueError, match="op_kwargs must be a dictionary"):
            operator.validate_parameters()

    @patch("subprocess.run")
    def test_bash_operator_command_failure(self, mock_run):
        """Test BashOperator handling of command failures."""
        mock_run.side_effect = subprocess.CalledProcessError(returncode=1, cmd="false")

        operator = BashOperator(task_id="failing_bash", bash_command="false")
        context = {"dag_id": "test", "task_id": "failing_bash", "run_id": "test"}

        with pytest.raises(
            RuntimeError, match="Bash command failed with return code 1"
        ):
            operator.execute(context)

    @patch("subprocess.run")
    def test_bash_operator_timeout(self, mock_run):
        """Test BashOperator handling of command timeouts."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="sleep 10", timeout=5)

        operator = BashOperator(
            task_id="timeout_bash", bash_command="sleep 10", timeout=5
        )
        context = {"dag_id": "test", "task_id": "timeout_bash"}

        with pytest.raises(
            RuntimeError, match="Bash command timed out after 5 seconds"
        ):
            operator.execute(context)

    def test_bash_operator_empty_command(self):
        """Test BashOperator with empty command."""
        with pytest.raises(ValueError, match="bash_command must be a non-empty string"):
            BashOperator(task_id="test", bash_command="")

    def test_bash_operator_invalid_timeout(self):
        """Test BashOperator with invalid timeout."""
        operator = BashOperator(task_id="test", bash_command="echo test")
        operator.timeout = -1

        with pytest.raises(ValueError, match="timeout must be positive"):
            operator.validate_parameters()

    def test_bash_operator_invalid_working_directory(self):
        """Test BashOperator with invalid working directory."""
        operator = BashOperator(
            task_id="test", bash_command="echo test", cwd="/nonexistent/directory"
        )

        with pytest.raises(ValueError, match="Working directory does not exist"):
            operator.validate_parameters()


class TestExecutorErrorHandling:
    """Test error handling in executors."""

    @patch("subprocess.Popen")
    def test_executor_subprocess_creation_failure(self, mock_popen):
        """Test executor handling of subprocess creation failure."""
        mock_popen.side_effect = OSError("Failed to create process")

        executor = LocalExecutor()
        task_key = ("dag", "task", "run")

        try:
            # Should not raise exception, but handle gracefully
            executor.queue_task(task_key)

            # Task should not be in processes
            assert task_key not in executor.processes
            assert task_key not in executor.process_start_times
        finally:
            executor.shutdown()

    @patch("subprocess.Popen")
    def test_executor_process_communication_failure(self, mock_popen):
        """Test executor handling of process communication failure."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.poll.return_value = 1  # Failed
        mock_process.returncode = 1
        mock_process.communicate.side_effect = OSError("Communication failed")
        mock_popen.return_value = mock_process

        executor = MockableLocalExecutor()
        task_key = ("dag", "task", "run")

        try:
            executor.queue_task(task_key)

            # Should handle communication failure gracefully
            finished_tasks = executor.check_for_finished_tasks()

            # Task should still be marked as finished
            assert task_key in finished_tasks
            assert task_key not in executor.processes
        finally:
            executor.shutdown()

    def test_executor_shutdown_with_unresponsive_processes(self):
        """Test executor shutdown with unresponsive processes."""
        executor = LocalExecutor()

        with patch("subprocess.Popen") as mock_popen:
            # Create mock processes that don't respond to termination
            mock_processes = []
            for i in range(2):
                mock_process = MagicMock()
                mock_process.pid = 1000 + i
                mock_process.poll.return_value = None  # Still running
                mock_process.wait.side_effect = subprocess.TimeoutExpired("cmd", 10)
                mock_processes.append(mock_process)

            mock_popen.side_effect = mock_processes

            # Queue tasks
            for i in range(2):
                task_key = ("dag", f"task{i}", "run")
                executor.queue_task(task_key)

            # Shutdown should force kill unresponsive processes
            executor.shutdown()

            # All processes should be terminated and killed
            for mock_process in mock_processes:
                mock_process.terminate.assert_called_once()
                mock_process.kill.assert_called_once()

    def test_executor_duplicate_task_queuing(self):
        """Test executor handling of duplicate task queuing."""
        executor = LocalExecutor()

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.pid = 12345
            mock_popen.return_value = mock_process

            task_key = ("dag", "task", "run")

            try:
                # Queue task first time
                executor.queue_task(task_key)
                assert task_key in executor.processes

                # Queue same task again - should be ignored
                executor.queue_task(task_key)

                # Should still only have one process
                assert len(executor.processes) == 1
                assert mock_popen.call_count == 1
            finally:
                executor.shutdown()


class TestTaskRunnerErrorHandling:
    """Test error handling in task runner."""

    @patch("spiralarr.task_runner.runner.get_session")
    def test_task_runner_database_connection_failure(self, mock_get_session):
        """Test task runner handling of database connection failure."""
        mock_session = MagicMock()
        mock_session.query.side_effect = Exception("Database connection failed")
        mock_get_session.return_value = mock_session

        runner = TaskRunner()
        result = runner.run_task("test_dag", "test_task", "test_run")

        # Should return False and handle gracefully
        assert result is False
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()

    @patch("spiralarr.task_runner.runner.get_session")
    def test_task_runner_task_not_found(self, mock_get_session):
        """Test task runner handling of task not found."""
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None  # Task not found
        mock_session.query.return_value = mock_query
        mock_get_session.return_value = mock_session

        runner = TaskRunner()
        result = runner.run_task("nonexistent_dag", "nonexistent_task", "test_run")

        # Should return False
        assert result is False
        mock_session.close.assert_called_once()

    @patch("spiralarr.task_runner.runner.get_session")
    def test_task_runner_state_update_failure(self, mock_get_session):
        """Test task runner handling of state update failure."""
        mock_session = MagicMock()
        mock_session.commit.side_effect = Exception("Commit failed")

        # Mock task instance
        mock_task_instance = MagicMock()
        mock_task_instance.set_state = MagicMock()

        # Mock query
        mock_query = MagicMock()
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = mock_task_instance
        mock_session.query.return_value = mock_query
        mock_get_session.return_value = mock_session

        runner = TaskRunner()
        result = runner.run_task("test_dag", "test_task", "test_run")

        # Should handle commit failure gracefully
        assert result is False
        mock_session.rollback.assert_called()
        mock_session.close.assert_called_once()


class TestDagLoaderErrorHandling:
    """Test error handling in DAG loader."""

    def test_dag_loader_nonexistent_dags_folder(self):
        """Test DAG loader with nonexistent dags folder."""
        loader = DagLoader(dags_folder="/nonexistent/folder")

        # Should handle gracefully and return None
        dag_config = loader.load_dag_config("test_dag")
        assert dag_config is None

    def test_dag_loader_invalid_python_file(self):
        """Test DAG loader with invalid Python file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create invalid Python file
            invalid_file = os.path.join(temp_dir, "invalid_dag.py")
            with open(invalid_file, "w") as f:
                f.write("invalid python syntax !!!")

            loader = DagLoader(dags_folder=temp_dir)
            dag_config = loader.load_dag_config("test_dag")

            # Should handle syntax error gracefully
            assert dag_config is None

    def test_dag_loader_missing_dag_config(self):
        """Test DAG loader with Python file missing DAG config."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create valid Python file without DAG config
            valid_file = os.path.join(temp_dir, "no_config_dag.py")
            with open(valid_file, "w") as f:
                f.write("# Valid Python file but no DAG config\nprint('hello')")

            loader = DagLoader(dags_folder=temp_dir)
            dag_config = loader.load_dag_config("test_dag")

            # Should return None
            assert dag_config is None

    def test_dag_loader_create_task_operator_missing_dag(self):
        """Test creating task operator for missing DAG."""
        loader = DagLoader()
        operator = loader.create_task_operator("nonexistent_dag", "test_task")

        # Should return None
        assert operator is None

    def test_dag_loader_create_task_operator_missing_task(self):
        """Test creating task operator for missing task."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create DAG file with config but missing the requested task
            dag_file = os.path.join(temp_dir, "test_dag.py")
            with open(dag_file, "w") as f:
                f.write("""
test_dag_config = {
    'dag_id': 'test_dag',
    'tasks': [
        {
            'task_id': 'other_task',
            'operator': 'BashOperator',
            'bash_command': 'echo hello'
        }
    ]
}
""")

            loader = DagLoader(dags_folder=temp_dir)
            operator = loader.create_task_operator("test_dag", "missing_task")

            # Should return None
            assert operator is None

    def test_dag_loader_unknown_operator_type(self):
        """Test creating task operator with unknown operator type."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create DAG file with unknown operator
            dag_file = os.path.join(temp_dir, "test_dag.py")
            with open(dag_file, "w") as f:
                f.write("""
test_dag_config = {
    'dag_id': 'test_dag',
    'tasks': [
        {
            'task_id': 'unknown_task',
            'operator': 'UnknownOperator',
            'some_param': 'value'
        }
    ]
}
""")

            loader = DagLoader(dags_folder=temp_dir)
            operator = loader.create_task_operator("test_dag", "unknown_task")

            # Should return None
            assert operator is None


class TestStateTransitionValidation:
    """Test state transition validation."""

    def test_invalid_state_transitions(self):
        """Test invalid state transitions are rejected."""
        task_instance = TaskInstance(task_id="test", dag_run_id=1)

        # Set initial state
        task_instance.state = State.SUCCESS

        # Try invalid transition from SUCCESS to RUNNING
        with pytest.raises(ValueError, match="Invalid state transition"):
            task_instance.set_state(State.RUNNING)

    def test_valid_state_transitions(self):
        """Test valid state transitions are allowed."""
        task_instance = TaskInstance(task_id="test", dag_run_id=1)

        # Valid transition sequence
        task_instance.state = State.SCHEDULED
        task_instance.set_state(State.QUEUED)  # Should work

        task_instance.set_state(State.RUNNING)  # Should work

        task_instance.set_state(State.SUCCESS)  # Should work

    def test_retry_state_transition(self):
        """Test retry state transitions."""
        task_instance = TaskInstance(task_id="test", dag_run_id=1)

        # Failed task can be retried
        task_instance.state = State.FAILED
        task_instance.set_state(State.QUEUED)  # Should work for retry
