"""Test executor that overrides LocalExecutor for testing compatibility."""

import time
from typing import List, Tuple

from spiralarr.executors.local import LocalExecutor


class MockableLocalExecutor(LocalExecutor):
    """
    Test-compatible version of LocalExecutor.

    Uses synchronous polling instead of event-driven monitoring
    to work with mocked subprocess objects in tests.
    """

    def __init__(self):
        # Initialize parent but override event-driven behavior
        super().__init__()

        # Stop the background thread immediately
        self.process_watcher_running = False
        if (
            hasattr(self, "process_watcher_thread")
            and self.process_watcher_thread.is_alive()
        ):
            self.process_watcher_thread.join(timeout=1)

        print("Test executor initialized with synchronous polling")

    def check_for_finished_tasks(self) -> List[Tuple[str, str, str]]:
        """
        Test-compatible version that uses synchronous polling.

        This works with mocked subprocess objects that don't generate
        real process completion signals.
        """
        finished_tasks = []

        # Check mocked processes synchronously (including timeout handling)
        for task_key, process in list(self.processes.items()):
            process_finished = False

            # Check if process finished
            if process.poll() is not None:  # Process finished
                process_finished = True

            # Check for timeout (similar to parent class logic)
            elif hasattr(self, "task_timeout"):
                start_time = self.process_start_times.get(task_key, time.time())
                if time.time() - start_time > self.task_timeout:
                    print(f"TIMEOUT Task {task_key} after {self.task_timeout}s")
                    process.terminate()
                    # Simulate timeout by setting returncode
                    process.returncode = -1
                    process_finished = True

            if process_finished:
                finished_tasks.append(task_key)

                # Create completion signal for consistency with parent behavior
                start_time = self.process_start_times.get(task_key, time.time())
                completion_info = {
                    "task_key": task_key,
                    "returncode": process.returncode,
                    "execution_time": time.time() - start_time,
                    "success": process.returncode == 0,
                }
                if process.returncode == -1:
                    completion_info["timeout"] = True

                self.completion_signals.put(completion_info)

                # Log completion like parent
                if completion_info.get("timeout"):
                    print(f"Task {task_key} timed out")
                elif completion_info["success"]:
                    print(f"Task {task_key} completed successfully")
                else:
                    print(f"Task {task_key} failed")

                # Clean up
                self._cleanup_task(task_key)

        # Check for any signals in queue (for consistency)
        while not self.completion_signals.empty():
            try:
                completion_info = self.completion_signals.get_nowait()
                task_key = completion_info["task_key"]
                if task_key not in finished_tasks:
                    finished_tasks.append(task_key)
            except Exception:
                break

        # Start queued tasks if we have capacity
        while self.queued_tasks and len(self.processes) < self.max_parallelism:
            next_task = self.queued_tasks.pop(0)
            self.queue_task(next_task)

        return finished_tasks

    def shutdown(self) -> None:
        """Shutdown without waiting for background thread."""
        print("Shutting down test executor...")

        # Stop any remaining background thread
        self.process_watcher_running = False

        if not self.processes and not self.queued_tasks:
            print("Test executor shutdown complete (no active tasks)")
            return

        print(
            f"Shutting down test executor with {len(self.processes)} running tasks "
            f"and {len(self.queued_tasks)} queued tasks"
        )

        # First, try graceful termination
        for task_key, process in list(self.processes.items()):
            if process.poll() is None:  # Still running
                print(f"Gracefully terminating task {task_key}")
                process.terminate()

        # Wait for processes to terminate gracefully
        terminated_count = 0
        for task_key, process in list(self.processes.items()):
            try:
                process.wait(timeout=10)
                terminated_count += 1
                print(f"Task {task_key} terminated gracefully")
            except Exception:
                print(f"Force killing task {task_key}")
                try:
                    process.kill()
                    process.wait(timeout=5)
                    terminated_count += 1
                except Exception:
                    print(f"Failed to kill task {task_key}")

        print(
            f"Test executor shutdown complete. "
            f"Terminated {terminated_count}/{len(self.processes)} tasks"
        )

        # Clear all tracking data
        self.processes.clear()
        self.process_start_times.clear()
        self.queued_tasks.clear()

        # Clear any remaining completion signals
        while not self.completion_signals.empty():
            try:
                self.completion_signals.get_nowait()
            except Exception:
                break
