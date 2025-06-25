"""Tests for DagRun model."""

import datetime as dt

import pytest
from sqlalchemy import select

from spiralarr.models.dag import DagModel
from spiralarr.models.dag_run import DagRun
from spiralarr.utils.state import State


class TestDagRun:
    """Test DagRun model."""

    @pytest.mark.asyncio
    async def test_dag_run_creation(self, async_session):
        """Test DagRun creation and relationships."""
        # Create DAG first
        dag = DagModel(dag_id="test_dag", is_active="True")
        async_session.add(dag)
        await async_session.commit()

        # Create DAG run
        dag_run = DagRun(
            dag_id="test_dag",
            run_id="test_run_1",
            execution_date=dt.datetime.now(dt.timezone.utc).replace(tzinfo=None),
            state=State.DAG_RUNNING,
        )
        async_session.add(dag_run)
        await async_session.commit()

        # Test retrieval
        result = await async_session.execute(
            select(DagRun).where(DagRun.run_id == "test_run_1")
        )
        retrieved_run = result.scalar_one_or_none()

        assert retrieved_run is not None
        assert retrieved_run.dag_id == "test_dag"
        assert retrieved_run.state == State.DAG_RUNNING
