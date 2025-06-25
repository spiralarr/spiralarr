"""DAG model for storing DAG metadata."""

import datetime as dt
from typing import Optional

from sqlalchemy import DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from spiralarr.models.base import Base


class DagModel(Base):
    """
    Stores metadata about DAGs that have been loaded into the system.
    This is the database representation of a DAG definition.
    """

    __tablename__ = "dag"

    id: Mapped[int] = mapped_column(primary_key=True)
    dag_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)

    # DAG metadata
    description: Mapped[Optional[str]] = mapped_column(Text)
    schedule_interval: Mapped[Optional[str]] = mapped_column(
        String(100)
    )  # cron expression or special values
    start_date: Mapped[Optional[dt.datetime]] = mapped_column(DateTime)
    end_date: Mapped[Optional[dt.datetime]] = mapped_column(DateTime)

    # System metadata
    is_active: Mapped[str] = mapped_column(
        String(5), default="True"
    )  # "True"/"False" as string
    last_parsed_time: Mapped[Optional[dt.datetime]] = mapped_column(DateTime)
    last_scheduler_run: Mapped[Optional[dt.datetime]] = mapped_column(DateTime)

    # File information
    fileloc: Mapped[Optional[str]] = mapped_column(String(2000))  # Path to the DAG file

    def __repr__(self):
        return f"<DagModel {self.dag_id}>"

    @property
    def is_active_bool(self) -> bool:
        """Convert string is_active to boolean."""
        return bool(self.is_active == "True")

    def set_is_active(self, is_active: bool):
        """Set is_active as string."""
        self.is_active = "True" if is_active else "False"
