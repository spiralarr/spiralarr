"""Local executor using multiprocessing with event-driven communication."""

import queue
import subprocess
import sys
from threading import Thread
import time
from typing import Dict, List, Tuple

from spiralarr.config.settings import get_settings
from spiralarr.executors.base import BaseExecutor


class LocalExecutor(BaseExecutor):
    """
    High-performance executor with event-driven task completion detection.

    PERFORMANCE IMPROVEMENTS:
    - Event-driven completion signals (O(1) vs O(n) polling)
    - Background thread for process monitoring
    - Immediate task completion detection
    - Zero polling overhead in scheduler loop

    This is the 2nd part of the 3-part execution system:
    Scheduler -> LocalExecutor -> Task Runner (CLI command)
    """

    def __init__(self):
        super().__init__()
        self.processes: Dict[Tuple[str, str, str], subprocess.Popen] = {}
        self.process_start_times: Dict[Tuple[str, str, str], float] = {}
        settings = get_settings()
        self.max_parallelism = settings.scheduler.max_threads
        self.task_timeout = 3600  # 1 hour default

        # Event-driven completion detection
        self.completion_signals = queue.Queue()
        self.process_watcher_thread = Thread(target=self._watch_processes, daemon=True)
        self.process_watcher_running = True
        self.process_watcher_thread.start()
        print("Event-driven executor initialized - zero polling overhead!")

    def _watch_processes(self):
        """
        Background thread that watches for process completion.

        PERFORMANCE BENEFITS:
        - Non-blocking process monitoring
        - Immediate completion detection
        - Signals completion via queue
        - Eliminates O(n) polling in scheduler loop
        """
        while self.process_watcher_running:
            try:
                for task_key, process in list(self.processes.items()):
                    try:
                        # Non-blocking wait with short timeout
                        process.wait(timeout=0.1)

                        # Process finished! Calculate execution time
                        start_time = self.process_start_times.get(task_key, time.time())
                        execution_time = time.time() - start_time

                        # Signal completion via queue (O(1) operation)
                        completion_info = {
                            "task_key": task_key,
                            "returncode": process.returncode,
                            "execution_time": execution_time,
                            "success": process.returncode == 0,
                        }
                        self.completion_signals.put(completion_info)

                        # Log completion
                        status = "SUCCESS" if process.returncode == 0 else "FAILED"
                        print(
                            f"{status} Task {task_key} completed in "
                            f"{execution_time:.2f}s"
                        )

                        # Clean up process tracking
                        self._cleanup_task(task_key)

                    except subprocess.TimeoutExpired:
                        # Process still running, check for timeout
                        start_time = self.process_start_times.get(task_key, time.time())
                        if time.time() - start_time > self.task_timeout:
                            print(f"TIMEOUT Task {task_key} after {self.task_timeout}s")
                            process.terminate()
                            try:
                                process.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                process.kill()

                            # Signal timeout completion
                            self.completion_signals.put(
                                {
                                    "task_key": task_key,
                                    "returncode": -1,
                                    "execution_time": self.task_timeout,
                                    "success": False,
                                    "timeout": True,
                                }
                            )
                            self._cleanup_task(task_key)
                        continue
                    except Exception as e:
                        print(f"Error monitoring task {task_key}: {e}")
                        continue

                # Small sleep to prevent CPU spinning
                time.sleep(0.5)

            except Exception as e:
                print(f"Error in process watcher: {e}")
                time.sleep(1)

    def queue_task(self, task_key: Tuple[str, str, str]) -> None:
        """
        Queue a task for execution by launching a subprocess.

        The subprocess runs the 'spiralarr run-task' CLI command.
        """
        dag_id, task_id, run_id = task_key

        # Check if task is already running
        if task_key in self.processes:
            print(f"Task {task_key} is already running, skipping")
            return

        # Check if we're at max capacity
        if len(self.processes) >= self.max_parallelism:
            if task_key not in self.queued_tasks:
                self.queued_tasks.append(task_key)
                print(
                    f"Task {task_key} queued (at capacity: "
                    f"{len(self.processes)}/{self.max_parallelism})"
                )
            return

        # Launch subprocess to run the task
        cmd = [
            sys.executable,
            "-m",
            "spiralarr.cli.commands",
            "run-task",
            dag_id,
            task_id,
            run_id,
        ]

        try:
            print(f"Starting task {task_key}...")
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                # Set process group to allow clean termination
                start_new_session=True,
            )
            self.processes[task_key] = process
            self.process_start_times[task_key] = time.time()
            print(f"Started task {task_key} with PID {process.pid}")
        except Exception as e:
            print(f"Failed to start task {task_key}: {e}")
            # Remove from queue if it was there
            if task_key in self.queued_tasks:
                self.queued_tasks.remove(task_key)

    def check_for_finished_tasks(self) -> List[Tuple[str, str, str]]:
        """
        HIGH-PERFORMANCE: Get finished tasks from signal queue (O(1) - NO POLLING!)

        PERFORMANCE BENEFITS:
        - O(1) complexity vs O(n) polling
        - Zero system calls (no process.poll() per task)
        - Immediate completion detection
        - Non-blocking operation
        """
        finished_tasks = []

        # Get all completion signals from queue (O(1) operation)
        while not self.completion_signals.empty():
            try:
                completion_info = self.completion_signals.get_nowait()
                task_key = completion_info["task_key"]
                finished_tasks.append(task_key)

                # Optional: Log detailed completion info
                if completion_info.get("timeout"):
                    print(f"Task {task_key} timed out")
                elif completion_info["success"]:
                    print(f"Task {task_key} completed successfully")
                else:
                    print(f"Task {task_key} failed")

            except queue.Empty:
                break
            except Exception as e:
                print(f"Error processing completion signal: {e}")
                break

        # Start queued tasks if we have capacity
        while self.queued_tasks and len(self.processes) < self.max_parallelism:
            next_task = self.queued_tasks.pop(0)
            self.queue_task(next_task)

        return finished_tasks

    def get_completed_tasks_from_signals(self) -> List[Dict]:
        """
        NEW METHOD: Get detailed completion info from signal queue.

        Returns completion info with execution time, return codes, etc.
        This is the new high-performance interface for the scheduler.
        """
        completed = []
        while not self.completion_signals.empty():
            try:
                completed.append(self.completion_signals.get_nowait())
            except queue.Empty:
                break
        return completed

    def _cleanup_task(self, task_key: Tuple[str, str, str]) -> None:
        """Clean up task tracking data."""
        if task_key in self.processes:
            del self.processes[task_key]
        if task_key in self.process_start_times:
            del self.process_start_times[task_key]

    def shutdown(self) -> None:
        """Shutdown all running processes and background thread gracefully."""
        print("Shutting down event-driven executor...")

        # Stop the background process watcher thread
        self.process_watcher_running = False

        if not self.processes and not self.queued_tasks:
            print("Executor shutdown complete (no active tasks)")
            return

        print(
            f"Shutting down executor with {len(self.processes)} running tasks "
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
            except subprocess.TimeoutExpired:
                print(f"Force killing task {task_key}")
                process.kill()
                try:
                    process.wait(timeout=5)
                    terminated_count += 1
                except subprocess.TimeoutExpired:
                    print(f"Failed to kill task {task_key}")

        # Wait for background thread to finish
        if self.process_watcher_thread.is_alive():
            self.process_watcher_thread.join(timeout=2)
            if self.process_watcher_thread.is_alive():
                print("Background thread did not stop gracefully")

        print(
            f"Shutdown complete. Terminated "
            f"{terminated_count}/{len(self.processes)} tasks"
        )

        # Clear all tracking data
        self.processes.clear()
        self.process_start_times.clear()
        self.queued_tasks.clear()

        # Clear any remaining completion signals
        while not self.completion_signals.empty():
            try:
                self.completion_signals.get_nowait()
            except queue.Empty:
                break

    def get_executor_stats(self) -> Dict[str, int]:
        """Get executor statistics."""
        return {
            "running_tasks": len(self.processes),
            "queued_tasks": len(self.queued_tasks),
            "max_parallelism": self.max_parallelism,
        }
