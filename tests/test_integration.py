"""Integration tests for spiralarr Phase 1 & 2."""

import datetime as dt
from unittest.mock import MagicMock, patch

from spiralarr.executors.mockable_executor import MockableLocalExecutor
from spiralarr.models.base import get_session
from spiralarr.models.dag import DagModel
from spiralarr.models.dag_run import DagRun
from spiralarr.models.task_instance import TaskInstance
from spiralarr.task_runner.runner import TaskRunner
from spiralarr.utils.state import State


class TestPhase1Integration:
    """Integration tests for Phase 1 functionality."""

    def test_complete_task_execution_flow(self, temp_db):
        """Test the complete task execution flow."""
        session = get_session()

        try:
            # Step 1: Create test data
            dag = DagModel(
                dag_id="integration_test_dag",
                description="Integration test DAG",
                schedule_interval="0 0 * * *",
                start_date=dt.datetime(2024, 1, 1),
                is_active="True",
                fileloc="./tests/test_dag.py",
            )
            session.add(dag)
            session.commit()

            dag_run = DagRun(
                dag_id="integration_test_dag",
                run_id="integration_test_run",
                execution_date=dt.datetime.now(dt.timezone.utc),
                state=State.DAG_RUNNING,
                run_type="manual",
            )
            session.add(dag_run)
            session.commit()

            task_instance = TaskInstance(
                task_id="bash_task", dag_run_id=dag_run.id, state=State.SCHEDULED
            )
            session.add(task_instance)
            session.commit()

            # Step 2: Test task runner
            task_runner = TaskRunner()
            success = task_runner.run_task(
                "integration_test_dag", "bash_task", "integration_test_run"
            )

            assert success is True

            # Step 3: Verify task state was updated
            session.refresh(task_instance)
            assert task_instance.state == State.SUCCESS
            assert task_instance.start_date is not None
            assert task_instance.end_date is not None

        finally:
            session.close()

    def test_task_failure_handling(self, temp_db):
        """Test task failure handling."""
        session = get_session()

        try:
            # Create test data with a task that will fail
            dag = DagModel(dag_id="fail_test_dag", is_active="True")
            session.add(dag)
            session.commit()

            dag_run = DagRun(
                dag_id="fail_test_dag",
                run_id="fail_test_run",
                execution_date=dt.datetime.now(dt.timezone.utc),
            )
            session.add(dag_run)
            session.commit()

            # Create a task that should fail (no matching operator pattern)
            task_instance = TaskInstance(
                task_id="unknown_task_type",
                dag_run_id=dag_run.id,
                state=State.SCHEDULED,
            )
            session.add(task_instance)
            session.commit()

            # Test task runner with failing task
            task_runner = TaskRunner()
            success = task_runner.run_task(
                "fail_test_dag", "unknown_task_type", "fail_test_run"
            )

            # Should still return True as the task runner handles the execution
            # The actual task result is stored in the database
            assert success is True

        finally:
            session.close()


class TestPhase2Integration:
    """Integration tests for Phase 2 execution system."""

    def test_executor_task_runner_integration(self):
        """Test integration between LocalExecutor and TaskRunner."""
        executor = MockableLocalExecutor()

        try:
            # Mock subprocess to avoid actual CLI execution
            with patch("subprocess.Popen") as mock_popen:
                mock_process = MagicMock()
                mock_process.pid = 12345
                mock_process.poll.return_value = None  # Still running initially
                mock_popen.return_value = mock_process

                # Queue a task
                task_key = ("test_dag", "test_task", "test_run")
                executor.queue_task(task_key)

                # Verify task was queued
                assert task_key in executor.processes
                assert len(executor.processes) == 1

                # Simulate task completion
                mock_process.poll.return_value = 0  # Completed successfully
                mock_process.returncode = 0
                mock_process.communicate.return_value = ("Task completed", "")

                # Check for finished tasks
                finished_tasks = executor.check_for_finished_tasks()

                # Verify task was marked as finished
                assert task_key in finished_tasks
                assert task_key not in executor.processes

        finally:
            executor.shutdown()

    def test_executor_capacity_management(self):
        """Test executor capacity and queuing behavior."""
        executor = MockableLocalExecutor()
        executor.max_parallelism = 2  # Limit to 2 concurrent tasks

        try:
            with patch("subprocess.Popen") as mock_popen:
                mock_processes = []

                def create_mock_process(*args, **kwargs):
                    mock_process = MagicMock()
                    mock_process.pid = len(mock_processes) + 1000
                    mock_process.poll.return_value = None  # Still running
                    mock_processes.append(mock_process)
                    return mock_process

                mock_popen.side_effect = create_mock_process

                # Queue 4 tasks (more than capacity)
                tasks = [
                    ("dag", "task1", "run"),
                    ("dag", "task2", "run"),
                    ("dag", "task3", "run"),
                    ("dag", "task4", "run"),
                ]

                for task_key in tasks:
                    executor.queue_task(task_key)

                # Only 2 should be running, 2 should be queued
                assert len(executor.processes) == 2
                assert len(executor.queued_tasks) == 2

                # Complete one task
                first_process = mock_processes[0]
                first_process.poll.return_value = 0
                first_process.returncode = 0
                first_process.communicate.return_value = ("", "")

                # Check for finished tasks (should start a queued task)
                finished_tasks = executor.check_for_finished_tasks()

                # Should still have 2 running (one finished, one started from queue)
                assert len(executor.processes) == 2
                assert len(executor.queued_tasks) == 1
                assert len(finished_tasks) == 1

        finally:
            executor.shutdown()

    def test_task_timeout_handling(self):
        """Test handling of task timeouts."""
        executor = MockableLocalExecutor()
        executor.task_timeout = 1  # 1 second timeout for testing

        try:
            with patch("subprocess.Popen") as mock_popen:
                with patch("time.time") as mock_time:
                    mock_process = MagicMock()
                    mock_process.pid = 12345
                    mock_process.poll.return_value = None  # Still running
                    mock_process.wait.return_value = None
                    mock_popen.return_value = mock_process

                    # Start time
                    start_time = 1000.0
                    mock_time.return_value = start_time

                    # Queue a task
                    task_key = ("dag", "long_task", "run")
                    executor.queue_task(task_key)

                    # Simulate time passing beyond timeout
                    mock_time.return_value = start_time + 2.0  # 2 seconds later

                    # Check for finished tasks
                    finished_tasks = executor.check_for_finished_tasks()

                    # Task should be terminated due to timeout
                    assert task_key in finished_tasks
                    assert task_key not in executor.processes
                    mock_process.terminate.assert_called_once()

        finally:
            executor.shutdown()

    def test_database_state_consistency(self, temp_db):
        """Test that database state remains consistent during execution."""
        session = get_session()

        try:
            # Create test DAG run
            dag_run = DagRun(
                dag_id="test_dag",
                run_id="consistency_test",
                state=State.RUNNING,
                execution_date=dt.datetime.now(dt.timezone.utc),
            )
            session.add(dag_run)
            session.commit()

            # Create test task instance
            task_instance = TaskInstance(
                task_id="test_task", dag_run_id=dag_run.id, state=State.SCHEDULED
            )
            session.add(task_instance)
            session.commit()

            # Test state transitions
            initial_state = task_instance.state
            assert initial_state == State.SCHEDULED

            # Transition to QUEUED
            task_instance.set_state(State.QUEUED)
            session.commit()

            # Refresh and verify
            session.refresh(task_instance)
            assert task_instance.state == State.QUEUED
            assert task_instance.queued_at is not None

            # Transition to RUNNING
            task_instance.set_state(State.RUNNING)
            session.commit()

            # Refresh and verify
            session.refresh(task_instance)
            assert task_instance.state == State.RUNNING
            assert task_instance.start_date is not None

            # Transition to SUCCESS
            task_instance.set_state(State.SUCCESS)
            session.commit()

            # Refresh and verify
            session.refresh(task_instance)
            assert task_instance.state == State.SUCCESS
            assert task_instance.end_date is not None
            assert task_instance.duration is not None

        finally:
            session.close()
