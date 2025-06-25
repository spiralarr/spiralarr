"""
High-performance dependency resolution model.

PERFORMANCE ADVANTAGE:
- Pre-computed dependency table in database
- Single SQL query for dependency checking (faster than Airflow's N+1)
- O(1) dependency lookups vs O(n) file parsing
"""

from sqlalchemy import Index, String
from sqlalchemy.orm import Mapped, mapped_column

from spiralarr.models.base import Base


class TaskDependency(Base):
    """
    Pre-computed task dependency relationships for high-performance resolution.

    This table stores all task dependencies in the database, enabling:
    - Single SQL query for all dependency checking
    - O(1) dependency lookups
    - Faster than Airflow's N+1 query approach
    - Dependencies computed once when DAG changes
    """

    __tablename__ = "task_dependencies"

    # Composite primary key
    dag_id: Mapped[str] = mapped_column(String(250), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(250), primary_key=True)
    upstream_task_id: Mapped[str] = mapped_column(String(250), primary_key=True)

    def __repr__(self):
        return (
            f"<TaskDependency(dag_id='{self.dag_id}', "
            f"task_id='{self.task_id}', "
            f"upstream_task_id='{self.upstream_task_id}')>"
        )

    @classmethod
    def get_upstream_tasks(cls, session, dag_id: str, task_id: str):
        """Get all upstream task IDs for a given task."""
        return (
            session.query(cls.upstream_task_id)
            .filter(cls.dag_id == dag_id, cls.task_id == task_id)
            .all()
        )

    @classmethod
    def get_downstream_tasks(cls, session, dag_id: str, task_id: str):
        """Get all downstream task IDs for a given task."""
        return (
            session.query(cls.task_id)
            .filter(cls.dag_id == dag_id, cls.upstream_task_id == task_id)
            .all()
        )

    @classmethod
    def clear_dag_dependencies(cls, session, dag_id: str):
        """Clear all dependencies for a DAG (when DAG is updated)."""
        session.query(cls).filter(cls.dag_id == dag_id).delete()

    @classmethod
    def bulk_insert_dependencies(cls, session, dependencies: list):
        """
        Bulk insert dependencies for performance.

        Args:
            dependencies: List of dicts with keys: dag_id, task_id, upstream_task_id
        """
        if dependencies:
            session.bulk_insert_mappings(cls, dependencies)


# Performance indexes for fast dependency lookups
Index(
    "idx_task_deps_downstream", TaskDependency.dag_id, TaskDependency.upstream_task_id
)
Index("idx_task_deps_upstream", TaskDependency.dag_id, TaskDependency.task_id)
