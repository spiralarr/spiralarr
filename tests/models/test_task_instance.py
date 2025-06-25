"""Tests for TaskInstance model."""

import datetime as dt

import pytest

from spiralarr.models.dag import DagModel
from spiralarr.models.dag_run import DagRun
from spiralarr.models.task_instance import TaskInstance
from spiralarr.utils.state import State


class TestTaskInstance:
    """Test TaskInstance model."""

    @pytest.mark.asyncio
    async def test_task_instance_creation(self, async_session):
        """Test TaskInstance creation and state management."""
        # Create DAG and DAG run
        dag = DagModel(dag_id="test_dag", is_active="True")
        async_session.add(dag)
        await async_session.commit()

        dag_run = DagRun(
            dag_id="test_dag",
            run_id="test_run_1",
            execution_date=dt.datetime.now(dt.timezone.utc).replace(tzinfo=None),
        )
        async_session.add(dag_run)
        await async_session.commit()

        # Create task instance
        task_instance = TaskInstance(
            task_id="test_task", dag_run_id=dag_run.id, state=State.SCHEDULED
        )
        async_session.add(task_instance)
        await async_session.commit()

        # Test state changes
        task_instance.set_state(State.RUNNING)
        assert task_instance.state == State.RUNNING
        assert task_instance.start_date is not None

        task_instance.set_state(State.SUCCESS)
        assert task_instance.state == State.SUCCESS
        assert task_instance.end_date is not None
