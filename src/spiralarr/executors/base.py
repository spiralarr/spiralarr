"""Base executor class."""

from abc import ABC, abstractmethod
from typing import List, Tuple


class BaseExecutor(ABC):
    """
    Base class for all executors.

    Executors are responsible for actually running tasks by managing
    the execution environment (local processes, containers, etc.).
    """

    def __init__(self):
        self.running_tasks = {}  # task_key -> process/job info
        self.queued_tasks = []  # list of task keys waiting to run

    @abstractmethod
    def queue_task(self, task_key: Tuple[str, str, str]) -> None:
        """
        Queue a task for execution.

        Args:
            task_key: Tuple of (dag_id, task_id, run_id)
        """
        pass

    @abstractmethod
    def check_for_finished_tasks(self) -> List[Tuple[str, str, str]]:
        """
        Check for tasks that have finished execution.

        Returns:
            List of task keys that have completed
        """
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Shutdown the executor and clean up resources."""
        pass

    def get_running_task_count(self) -> int:
        """Get the number of currently running tasks."""
        return len(self.running_tasks)

    def get_queued_task_count(self) -> int:
        """Get the number of queued tasks."""
        return len(self.queued_tasks)
