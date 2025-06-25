"""Tests for base operator."""

from unittest.mock import MagicMock

import pytest

from spiralarr.operators.base import BaseOperator


class MockOperator(BaseOperator):
    """Mock implementation of BaseOperator for testing."""

    def execute(self, context):
        return "test_result"


class TestBaseOperator:
    """Test cases for BaseOperator."""

    def test_init_valid_parameters(self):
        """Test BaseOperator initialization with valid parameters."""
        operator = MockOperator(
            task_id="test_task", dag_id="test_dag", retries=3, retry_delay_seconds=600
        )

        assert operator.task_id == "test_task"
        assert operator.dag_id == "test_dag"
        assert operator.retries == 3
        assert operator.retry_delay_seconds == 600
        assert operator.operator_name == "MockOperator"

    def test_init_invalid_task_id(self):
        """Test BaseOperator initialization with invalid task_id."""
        with pytest.raises(ValueError, match="task_id must be a non-empty string"):
            MockOperator(task_id="")

        with pytest.raises(ValueError, match="task_id must be a non-empty string"):
            MockOperator(task_id=None)

    def test_init_invalid_retries(self):
        """Test BaseOperator initialization with invalid retries."""
        with pytest.raises(ValueError, match="retries must be non-negative"):
            MockOperator(task_id="test", retries=-1)

    def test_get_task_key(self):
        """Test get_task_key method."""
        operator = MockOperator(task_id="test_task", dag_id="test_dag")
        assert operator.get_task_key() == "test_dag.test_task"

        operator_no_dag = MockOperator(task_id="test_task")
        assert operator_no_dag.get_task_key() == "test_task"

    def test_pre_execute_valid_context(self):
        """Test pre_execute with valid context."""
        operator = MockOperator(task_id="test_task")

        # Create a valid context
        context = {
            "dag_id": "test_dag",
            "task_id": "test_task",
            "run_id": "test_run",
            "execution_date": "2024-01-01",
            "task_instance": MagicMock(),
            "dag_run": MagicMock(),
        }

        # Should not raise an exception
        operator.pre_execute(context)

    def test_pre_execute_invalid_context(self):
        """Test pre_execute with invalid context."""
        operator = MockOperator(task_id="test_task")

        # Invalid context missing required keys
        context = {"dag_id": "test_dag"}

        with pytest.raises(ValueError, match="Invalid execution context"):
            operator.pre_execute(context)
