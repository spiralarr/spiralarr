"""Unit tests for spiralarr executors."""

import subprocess
import time
from unittest.mock import MagicMock, patch

from spiralarr.executors.local import LocalExecutor
from spiralarr.executors.mockable_executor import MockableLocalExecutor


class TestBaseExecutor:
    """Test cases for BaseExecutor."""

    def test_init(self):
        """Test BaseExecutor initialization."""
        # We can't instantiate BaseExecutor directly since it's abstract
        # But we can test through LocalExecutor
        executor = LocalExecutor()

        assert hasattr(executor, "running_tasks")
        assert hasattr(executor, "queued_tasks")
        assert isinstance(executor.running_tasks, dict)
        assert isinstance(executor.queued_tasks, list)

    def test_get_running_task_count(self):
        """Test get_running_task_count method."""
        executor = LocalExecutor()

        # Initially should be 0
        assert executor.get_running_task_count() == 0

        # Add some mock processes (LocalExecutor uses processes, not running_tasks)
        executor.running_tasks[("dag1", "task1", "run1")] = MagicMock()
        executor.running_tasks[("dag1", "task2", "run1")] = MagicMock()

        assert executor.get_running_task_count() == 2

    def test_get_queued_task_count(self):
        """Test get_queued_task_count method."""
        executor = LocalExecutor()

        # Initially should be 0
        assert executor.get_queued_task_count() == 0

        # Add some queued tasks
        executor.queued_tasks.append(("dag1", "task1", "run1"))
        executor.queued_tasks.append(("dag1", "task2", "run1"))

        assert executor.get_queued_task_count() == 2


class TestLocalExecutor:
    """Test cases for LocalExecutor."""

    def test_init(self):
        """Test LocalExecutor initialization."""
        executor = LocalExecutor()

        assert hasattr(executor, "processes")
        assert hasattr(executor, "process_start_times")
        assert hasattr(executor, "max_parallelism")
        assert hasattr(executor, "task_timeout")
        assert isinstance(executor.processes, dict)
        assert isinstance(executor.process_start_times, dict)
        assert executor.max_parallelism > 0
        assert executor.task_timeout > 0

    @patch("subprocess.Popen")
    def test_queue_task_success(self, mock_popen):
        """Test successful task queuing."""
        # Mock successful process creation
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        executor = LocalExecutor()
        task_key = ("test_dag", "test_task", "test_run")

        executor.queue_task(task_key)

        # Verify process was created and stored
        assert task_key in executor.processes
        assert executor.processes[task_key] == mock_process
        assert task_key in executor.process_start_times

        # Verify subprocess.Popen was called with correct arguments
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        assert "run-task" in call_args
        assert "test_dag" in call_args
        assert "test_task" in call_args
        assert "test_run" in call_args

    @patch("subprocess.Popen")
    def test_queue_task_already_running(self, mock_popen):
        """Test queuing a task that's already running."""
        executor = LocalExecutor()
        task_key = ("test_dag", "test_task", "test_run")

        # Add task to running processes
        executor.processes[task_key] = MagicMock()

        executor.queue_task(task_key)

        # Should not create new process
        mock_popen.assert_not_called()

    @patch("subprocess.Popen")
    def test_queue_task_at_capacity(self, mock_popen):
        """Test queuing tasks when at max capacity."""
        executor = LocalExecutor()
        executor.max_parallelism = 2

        # Fill up to capacity
        for i in range(2):
            task_key = ("dag", f"task{i}", "run")
            executor.processes[task_key] = MagicMock()

        # Try to queue another task
        new_task_key = ("dag", "new_task", "run")
        executor.queue_task(new_task_key)

        # Should be queued, not started
        assert new_task_key in executor.queued_tasks
        assert new_task_key not in executor.processes
        mock_popen.assert_not_called()

    @patch("subprocess.Popen")
    def test_queue_task_subprocess_failure(self, mock_popen):
        """Test handling of subprocess creation failure."""
        # Mock subprocess failure
        mock_popen.side_effect = OSError("Failed to start process")

        executor = LocalExecutor()
        task_key = ("test_dag", "test_task", "test_run")

        # Should not raise exception, but handle gracefully
        executor.queue_task(task_key)

        # Task should not be in processes
        assert task_key not in executor.processes
        assert task_key not in executor.process_start_times

    def test_check_for_finished_tasks_success(self):
        """Test checking for successfully finished tasks."""
        executor = MockableLocalExecutor()
        task_key = ("test_dag", "test_task", "test_run")

        # Mock finished process
        mock_process = MagicMock()
        mock_process.poll.return_value = 0  # Finished with success
        mock_process.returncode = 0
        mock_process.communicate.return_value = ("Task completed", "")

        executor.processes[task_key] = mock_process
        executor.process_start_times[task_key] = time.time() - 10  # 10 seconds ago

        finished_tasks = executor.check_for_finished_tasks()

        assert task_key in finished_tasks
        assert task_key not in executor.processes
        assert task_key not in executor.process_start_times

    def test_check_for_finished_tasks_failure(self):
        """Test checking for failed tasks."""
        executor = MockableLocalExecutor()
        task_key = ("test_dag", "test_task", "test_run")

        # Mock failed process
        mock_process = MagicMock()
        mock_process.poll.return_value = 1  # Finished with failure
        mock_process.returncode = 1
        mock_process.communicate.return_value = ("", "Task failed")

        executor.processes[task_key] = mock_process
        executor.process_start_times[task_key] = time.time() - 10

        finished_tasks = executor.check_for_finished_tasks()

        assert task_key in finished_tasks
        assert task_key not in executor.processes
        assert task_key not in executor.process_start_times

    @patch("time.time")
    def test_check_for_finished_tasks_timeout(self, mock_time):
        """Test handling of timed out tasks."""
        executor = MockableLocalExecutor()
        executor.task_timeout = 60  # 1 minute timeout
        task_key = ("test_dag", "test_task", "test_run")

        # Mock process that's still running initially, then times out
        mock_process = MagicMock()
        mock_process.poll.return_value = -1  # Simulate timeout termination
        mock_process.returncode = -1
        mock_process.wait.return_value = None

        executor.processes[task_key] = mock_process

        # Mock time to simulate timeout
        start_time = 1000.0
        current_time = start_time + 120.0  # 2 minutes later
        executor.process_start_times[task_key] = start_time
        mock_time.return_value = current_time

        finished_tasks = executor.check_for_finished_tasks()

        # Task should be marked as finished (TestLocalExecutor handles synchronously)
        assert task_key in finished_tasks
        assert task_key not in executor.processes

    @patch("subprocess.Popen")
    def test_check_for_finished_tasks_starts_queued(self, mock_popen):
        """Test that finished tasks trigger queued tasks to start."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        executor = MockableLocalExecutor()
        executor.max_parallelism = 1

        # Add a running task
        running_task = ("dag", "running_task", "run")
        finished_process = MagicMock()
        finished_process.poll.return_value = 0
        finished_process.returncode = 0
        finished_process.communicate.return_value = ("", "")
        executor.processes[running_task] = finished_process
        executor.process_start_times[running_task] = time.time() - 10

        # Add a queued task
        queued_task = ("dag", "queued_task", "run")
        executor.queued_tasks.append(queued_task)

        finished_tasks = executor.check_for_finished_tasks()

        # Running task should be finished
        assert running_task in finished_tasks
        assert running_task not in executor.processes

        # Queued task should now be running
        assert queued_task in executor.processes
        assert queued_task not in executor.queued_tasks
        mock_popen.assert_called_once()

    def test_shutdown_graceful(self):
        """Test graceful shutdown of executor."""
        executor = LocalExecutor()

        # Add some mock processes
        task1 = ("dag", "task1", "run")
        task2 = ("dag", "task2", "run")

        mock_process1 = MagicMock()
        mock_process1.poll.return_value = None  # Still running
        mock_process1.wait.return_value = None  # Terminates gracefully

        mock_process2 = MagicMock()
        mock_process2.poll.return_value = None  # Still running
        mock_process2.wait.side_effect = subprocess.TimeoutExpired("cmd", 10)

        executor.processes[task1] = mock_process1
        executor.processes[task2] = mock_process2
        executor.process_start_times[task1] = time.time()
        executor.process_start_times[task2] = time.time()

        # Add some queued tasks
        executor.queued_tasks.append(("dag", "queued1", "run"))
        executor.queued_tasks.append(("dag", "queued2", "run"))

        executor.shutdown()

        # All processes should be terminated
        mock_process1.terminate.assert_called_once()
        mock_process2.terminate.assert_called_once()
        mock_process2.kill.assert_called_once()  # Force killed after timeout

        # All data should be cleared
        assert len(executor.processes) == 0
        assert len(executor.process_start_times) == 0
        assert len(executor.queued_tasks) == 0

    def test_shutdown_empty(self):
        """Test shutdown with no running processes."""
        executor = LocalExecutor()

        # Should not raise any exceptions
        executor.shutdown()

        assert len(executor.processes) == 0
        assert len(executor.queued_tasks) == 0

    def test_get_executor_stats(self):
        """Test getting executor statistics."""
        executor = LocalExecutor()

        # Add some mock data
        executor.processes[("dag", "task1", "run")] = MagicMock()
        executor.processes[("dag", "task2", "run")] = MagicMock()
        executor.queued_tasks.append(("dag", "task3", "run"))

        stats = executor.get_executor_stats()

        assert stats["running_tasks"] == 2
        assert stats["queued_tasks"] == 1
        assert stats["max_parallelism"] == executor.max_parallelism
