"""
High-Performance DAG Run and Task Instance Management

PERFORMANCE IMPROVEMENTS:
- Bulk operations for task instance creation
- Optimized state management
- Efficient DAG run lifecycle handling
- Reduced database round-trips
"""

from datetime import datetime
from typing import Dict, List

from sqlalchemy import text

from spiralarr.models.dag_run import DagRun
from spiralarr.models.task_instance import TaskInstance
from spiralarr.utils.state import State


class HighPerformanceDAGRunManager:
    """
    Enhanced DAG run management with performance optimizations.

    PERFORMANCE FEATURES:
    - Bulk task instance creation
    - Optimized state transitions
    - Efficient database operations
    - Reduced query overhead
    """

    def __init__(self, session):
        self.session = session

    def create_task_instances_for_dag_run(
        self, dag_run: DagRun, task_configs: List[Dict]
    ):
        """
        BULK OPERATION: Create task instances for a DAG run with high performance.

        Args:
            dag_run: DagRun object
            task_configs: List of task configuration dictionaries
        """
        if not task_configs:
            print(f"No tasks found for DAG {dag_run.dag_id}")
            return

        # Prepare bulk insert data
        task_instances_data = []
        for task_config in task_configs:
            task_id = task_config.get("task_id")
            if not task_id:
                continue

            task_instances_data.append(
                {
                    "task_id": task_id,
                    "dag_id": dag_run.dag_id,
                    "dag_run_id": dag_run.id,
                    "execution_date": dag_run.execution_date,
                    "state": State.SCHEDULED,
                    "start_date": None,
                    "end_date": None,
                    "try_number": 1,
                    "max_tries": 1,
                }
            )

        # Bulk insert for performance
        if task_instances_data:
            self.session.bulk_insert_mappings(TaskInstance, task_instances_data)
            print(
                f"Bulk created {len(task_instances_data)} task instances for "
                f"{dag_run.dag_id}"
            )

    def update_dag_run_state(self, dag_run: DagRun) -> bool:
        """
        OPTIMIZED: Update DAG run state based on task states with single query.

        Returns:
            True if state changed, False otherwise
        """
        # Single query to get task state summary
        result = self.session.execute(
            text("""
            SELECT
                COUNT(*) as total_tasks,
                SUM(CASE WHEN state = :success THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN state = :failed THEN 1 ELSE 0 END) as failed_count,
                SUM(CASE WHEN state IN (:running, :queued) THEN 1 ELSE 0 END)
                    as active_count
            FROM task_instance
            WHERE dag_run_id = :dag_run_id
        """),
            {
                "dag_run_id": dag_run.id,
                "success": State.SUCCESS,
                "failed": State.FAILED,
                "running": State.RUNNING,
                "queued": State.QUEUED,
            },
        ).fetchone()

        if not result or result.total_tasks == 0:
            return False

        total_tasks = result.total_tasks
        success_count = result.success_count
        failed_count = result.failed_count
        active_count = result.active_count

        # Determine new state
        old_state = dag_run.state
        new_state = old_state

        if success_count == total_tasks:
            new_state = State.SUCCESS
            dag_run.end_date = datetime.utcnow()
        elif failed_count > 0:
            new_state = State.FAILED
            dag_run.end_date = datetime.utcnow()
        elif active_count > 0:
            new_state = State.RUNNING

        # Update if state changed
        if new_state != old_state:
            dag_run.state = new_state
            self.session.commit()
            print(
                f"DAG run {dag_run.dag_id}.{dag_run.run_id}: {old_state} -> {new_state}"
            )
            return True

        return False

    def get_dag_run_stats(self, dag_run: DagRun) -> Dict:
        """Get comprehensive statistics for a DAG run."""
        result = self.session.execute(
            text("""
            SELECT
                state,
                COUNT(*) as count,
                AVG(CASE
                    WHEN start_date IS NOT NULL AND end_date IS NOT NULL
                    THEN (julianday(end_date) - julianday(start_date)) * 86400
                    ELSE NULL
                END) as avg_duration_seconds
            FROM task_instance
            WHERE dag_run_id = :dag_run_id
            GROUP BY state
        """),
            {"dag_run_id": dag_run.id},
        ).fetchall()

        stats = {
            "dag_id": dag_run.dag_id,
            "run_id": dag_run.run_id,
            "state": dag_run.state,
            "execution_date": dag_run.execution_date,
            "start_date": dag_run.start_date,
            "end_date": dag_run.end_date,
            "task_states": {},
            "total_tasks": 0,
        }

        for row in result:
            state = row.state
            count = row.count
            avg_duration = row.avg_duration_seconds

            stats["task_states"][state] = {
                "count": count,
                "avg_duration_seconds": avg_duration,
            }
            stats["total_tasks"] += count

        return stats

    def cleanup_old_dag_runs(self, dag_id: str, keep_last_n: int = 10):
        """
        Clean up old DAG runs to prevent database bloat.

        Args:
            dag_id: DAG identifier
            keep_last_n: Number of recent runs to keep
        """
        # Get old DAG runs to delete
        old_runs = self.session.execute(
            text("""
            SELECT id FROM dag_run
            WHERE dag_id = :dag_id
            ORDER BY execution_date DESC
            LIMIT -1 OFFSET :keep_last_n
        """),
            {"dag_id": dag_id, "keep_last_n": keep_last_n},
        ).fetchall()

        if not old_runs:
            return

        old_run_ids = [row.id for row in old_runs]

        # Delete task instances first (foreign key constraint)
        deleted_tasks = self.session.execute(
            text("""
            DELETE FROM task_instance
            WHERE dag_run_id IN :run_ids
        """),
            {"run_ids": tuple(old_run_ids)},
        ).rowcount

        # Delete DAG runs
        deleted_runs = self.session.execute(
            text("""
            DELETE FROM dag_run
            WHERE id IN :run_ids
        """),
            {"run_ids": tuple(old_run_ids)},
        ).rowcount

        self.session.commit()
        print(
            f"Cleaned up {deleted_runs} old DAG runs and {deleted_tasks} "
            f"task instances for {dag_id}"
        )

    def get_runnable_tasks_for_dag_run(self, dag_run: DagRun) -> List[TaskInstance]:
        """Get runnable tasks specifically for a DAG run."""
        return (
            self.session.query(TaskInstance)
            .filter(
                TaskInstance.dag_run_id == dag_run.id,
                TaskInstance.state == State.SCHEDULED,
            )
            .all()
        )

    def mark_dag_run_as_failed(self, dag_run: DagRun, reason: str = None):
        """Mark a DAG run as failed and update all running tasks."""
        # Update DAG run state
        dag_run.state = State.FAILED
        dag_run.end_date = datetime.utcnow()

        # Update all non-terminal task states to failed
        updated_count = self.session.execute(
            text("""
            UPDATE task_instance
            SET state = :failed_state, end_date = :end_date
            WHERE dag_run_id = :dag_run_id
            AND state NOT IN (:success, :failed)
        """),
            {
                "failed_state": State.FAILED,
                "end_date": datetime.utcnow(),
                "dag_run_id": dag_run.id,
                "success": State.SUCCESS,
                "failed": State.FAILED,
            },
        ).rowcount

        self.session.commit()

        reason_msg = f" (reason: {reason})" if reason else ""
        print(f"Marked DAG run {dag_run.dag_id}.{dag_run.run_id} as FAILED{reason_msg}")
        print(f"   Updated {updated_count} task instances to FAILED")


def create_dag_run_manager(session):
    """Factory function to create DAG run manager."""
    return HighPerformanceDAGRunManager(session)
