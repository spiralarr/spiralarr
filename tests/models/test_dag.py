"""Tests for DAG model."""

import datetime as dt

import pytest
from sqlalchemy import select

from spiralarr.models.dag import DagModel


class TestDagModel:
    """Test DAG model."""

    @pytest.mark.asyncio
    async def test_dag_model_creation(self, async_session):
        """Test DagModel creation and properties."""
        dag = DagModel(
            dag_id="test_dag",
            description="Test DAG",
            schedule_interval="0 0 * * *",
            start_date=dt.datetime(2024, 1, 1),
            is_active="True",
        )
        async_session.add(dag)
        await async_session.commit()

        # Test retrieval
        result = await async_session.execute(
            select(DagModel).where(DagModel.dag_id == "test_dag")
        )
        retrieved_dag = result.scalar_one_or_none()

        assert retrieved_dag is not None
        assert retrieved_dag.dag_id == "test_dag"
        assert retrieved_dag.description == "Test DAG"
        assert retrieved_dag.is_active_bool is True
