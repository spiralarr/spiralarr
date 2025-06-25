"""DAG run model for tracking DAG execution instances."""

import datetime as dt
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from spiralarr.models.base import Base
from spiralarr.utils.state import State

if TYPE_CHECKING:
    from spiralarr.models.dag import DagModel
    from spiralarr.models.task_instance import TaskInstance


class DagRun(Base):
    """
    Represents a specific execution instance of a DAG.
    Each time a DAG is triggered, a new DagRun is created.
    """

    __tablename__ = "dag_run"

    id: Mapped[int] = mapped_column(primary_key=True)
    dag_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("dag.dag_id", ondelete="CASCADE"),
        index=True,
    )
    run_id: Mapped[str] = mapped_column(String(255), index=True)

    # Execution metadata
    execution_date: Mapped[dt.datetime] = mapped_column(DateTime, index=True)
    start_date: Mapped[Optional[dt.datetime]] = mapped_column(DateTime)
    end_date: Mapped[Optional[dt.datetime]] = mapped_column(DateTime)
    state: Mapped[str] = mapped_column(
        String(20), default=State.DAG_RUNNING, index=True
    )

    # Run type and configuration
    run_type: Mapped[str] = mapped_column(
        String(50), default="manual"
    )  # manual, scheduled, backfill
    external_trigger: Mapped[str] = mapped_column(
        String(5), default="False"
    )  # "True"/"False" as string

    # Relationships
    dag_model: Mapped["DagModel"] = relationship(lazy="selectin")
    task_instances: Mapped[List["TaskInstance"]] = relationship(
        back_populates="dag_run", cascade="all, delete-orphan", lazy="selectin"
    )

    __table_args__ = (
        UniqueConstraint("dag_id", "run_id", name="uq_dag_run_dag_id_run_id"),
        UniqueConstraint(
            "dag_id", "execution_date", name="uq_dag_run_dag_id_execution_date"
        ),
    )

    def __repr__(self):
        return f"<DagRun {self.dag_id}.{self.run_id} state={self.state}>"

    @property
    def external_trigger_bool(self) -> bool:
        """Convert string external_trigger to boolean."""
        return bool(self.external_trigger == "True")

    def set_external_trigger(self, external_trigger: bool):
        """Set external_trigger as string."""
        self.external_trigger = "True" if external_trigger else "False"

    def get_task_instances(self) -> List["TaskInstance"]:
        """Get all task instances for this DAG run."""
        return self.task_instances
