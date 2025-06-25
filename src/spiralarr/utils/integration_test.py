"""Integration test utilities for the 3-part execution system."""

import time
from typing import Tuple

from spiralarr.executors.local import LocalExecutor
from spiralarr.models.base import get_session
from spiralarr.models.dag_run import DagRun
from spiralarr.models.task_instance import TaskInstance
from spiralarr.utils.state import State


class ExecutionSystemIntegrationTest:
    """
    Test the integration between Scheduler → Executor → Task Runner.

    This class provides utilities to test the complete 3-part execution system
    without needing a full scheduler running.
    """

    def __init__(self):
        self.executor = LocalExecutor()
        self.session = get_session()

    def create_test_dag_run(self, dag_id: str, run_id: str) -> DagRun:
        """Create a test DAG run for testing."""
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            state=State.RUNNING,
            execution_date=time.time(),
        )
        self.session.add(dag_run)
        self.session.commit()
        return dag_run

    def create_test_task_instance(self, dag_run: DagRun, task_id: str) -> TaskInstance:
        """Create a test task instance for testing."""
        task_instance = TaskInstance(
            task_id=task_id, dag_run_id=dag_run.id, state=State.SCHEDULED
        )
        self.session.add(task_instance)
        self.session.commit()
        return task_instance

    def test_task_execution(self, dag_id: str, task_id: str, run_id: str) -> bool:
        """
        Test the complete execution flow for a single task.

        Args:
            dag_id: DAG identifier
            task_id: Task identifier
            run_id: Run identifier

        Returns:
            True if task executed successfully, False otherwise
        """
        print(f"Testing execution of {dag_id}.{task_id} (run: {run_id})")

        try:
            # Step 1: Create test data (simulating scheduler)
            dag_run = self.create_test_dag_run(dag_id, run_id)
            task_instance = self.create_test_task_instance(dag_run, task_id)

            # Step 2: Queue task with executor
            task_key = (dag_id, task_id, run_id)
            print(f"Queuing task {task_key} with executor...")

            # Update task to QUEUED state (simulating scheduler)
            task_instance.set_state(State.QUEUED)
            self.session.commit()

            # Queue with executor
            self.executor.queue_task(task_key)

            # Step 3: Wait for task completion
            print("Waiting for task completion...")
            max_wait_time = 30  # 30 seconds timeout
            start_time = time.time()

            while time.time() - start_time < max_wait_time:
                finished_tasks = self.executor.check_for_finished_tasks()
                if task_key in finished_tasks:
                    print(f"Task {task_key} finished")
                    break
                time.sleep(1)
            else:
                print(f"Task {task_key} timed out after {max_wait_time} seconds")
                return False

            # Step 4: Check final state
            self.session.refresh(task_instance)
            final_state = task_instance.state
            print(f"Final task state: {final_state}")

            return final_state == State.SUCCESS

        except Exception as e:
            print(f"Integration test failed: {e}")
            return False
        finally:
            self.cleanup()

    def test_multiple_tasks(self, tasks: list[Tuple[str, str, str]]) -> dict:
        """
        Test execution of multiple tasks.

        Args:
            tasks: List of (dag_id, task_id, run_id) tuples

        Returns:
            Dictionary mapping task keys to success status
        """
        results = {}

        print(f"Testing execution of {len(tasks)} tasks")

        try:
            # Create all test data
            task_instances = {}
            for dag_id, task_id, run_id in tasks:
                dag_run = self.create_test_dag_run(dag_id, run_id)
                task_instance = self.create_test_task_instance(dag_run, task_id)
                task_instances[(dag_id, task_id, run_id)] = task_instance

            # Queue all tasks
            for task_key in tasks:
                task_instance = task_instances[task_key]
                task_instance.set_state(State.QUEUED)
                self.executor.queue_task(task_key)

            self.session.commit()

            # Wait for all tasks to complete
            remaining_tasks = set(tasks)
            max_wait_time = 60  # 60 seconds for multiple tasks
            start_time = time.time()

            while remaining_tasks and time.time() - start_time < max_wait_time:
                finished_tasks = self.executor.check_for_finished_tasks()
                for task_key in finished_tasks:
                    if task_key in remaining_tasks:
                        remaining_tasks.remove(task_key)

                        # Check final state
                        task_instance = task_instances[task_key]
                        self.session.refresh(task_instance)
                        results[task_key] = task_instance.state == State.SUCCESS
                        print(
                            f"Task {task_key} finished with state: "
                            f"{task_instance.state}"
                        )

                time.sleep(1)

            # Mark any remaining tasks as timed out
            for task_key in remaining_tasks:
                results[task_key] = False
                print(f"Task {task_key} timed out")

            return results

        except Exception as e:
            print(f"Multiple task test failed: {e}")
            return {task_key: False for task_key in tasks}
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up test resources."""
        try:
            self.executor.shutdown()
            self.session.close()
        except Exception as e:
            print(f"Cleanup error: {e}")

    def get_executor_stats(self):
        """Get current executor statistics."""
        return self.executor.get_executor_stats()


def run_basic_integration_test():
    """Run a basic integration test of the execution system."""
    print("=" * 60)
    print("Running Basic Integration Test")
    print("=" * 60)

    test = ExecutionSystemIntegrationTest()

    try:
        # Test single task execution
        success = test.test_task_execution("example_dag", "bash_task", "test_run_1")

        if success:
            print("✓ Basic integration test PASSED")
            return True
        else:
            print("✗ Basic integration test FAILED")
            return False

    except Exception as e:
        print(f"✗ Integration test error: {e}")
        return False
    finally:
        test.cleanup()


def run_multi_task_integration_test():
    """Run a multi-task integration test."""
    print("=" * 60)
    print("Running Multi-Task Integration Test")
    print("=" * 60)

    test = ExecutionSystemIntegrationTest()

    try:
        # Test multiple tasks
        tasks = [
            ("example_dag", "bash_task", "test_run_2"),
            ("example_dag", "python_task", "test_run_3"),
        ]

        results = test.test_multiple_tasks(tasks)

        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)

        print(f"Multi-task test results: {success_count}/{total_count} tasks succeeded")

        if success_count == total_count:
            print("✓ Multi-task integration test PASSED")
            return True
        else:
            print("✗ Multi-task integration test FAILED")
            return False

    except Exception as e:
        print(f"✗ Multi-task integration test error: {e}")
        return False
    finally:
        test.cleanup()


if __name__ == "__main__":
    # Run integration tests
    basic_success = run_basic_integration_test()
    multi_success = run_multi_task_integration_test()

    if basic_success and multi_success:
        print("\n🎉 All integration tests PASSED!")
    else:
        print("\n❌ Some integration tests FAILED!")
