"""Tests for python operator."""

import pytest

from spiralarr.operators.python import PythonOperator


class TestPythonOperator:
    """Test python operator functionality."""

    def test_python_operator_success(self):
        """Test PythonOperator with successful function."""

        def test_function():
            return "Hello from Python"

        operator = PythonOperator(task_id="test_python", python_callable=test_function)

        context = {"task_id": "test_python"}
        result = operator.execute(context)

        assert result == "Hello from Python"

    def test_python_operator_with_args(self):
        """Test PythonOperator with arguments."""

        def test_function_with_args(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        operator = PythonOperator(
            task_id="test_python_args",
            python_callable=test_function_with_args,
            op_args=["World"],
            op_kwargs={"greeting": "Hi"},
        )

        context = {"task_id": "test_python_args"}
        result = operator.execute(context)

        assert result == "Hi, World!"

    def test_python_operator_failure(self):
        """Test PythonOperator with failing function."""

        def failing_function():
            raise ValueError("Test error")

        operator = PythonOperator(
            task_id="test_python_fail", python_callable=failing_function
        )

        context = {"task_id": "test_python_fail"}

        with pytest.raises(RuntimeError):
            operator.execute(context)

    def test_python_operator_with_context(self):
        """Test PythonOperator with context parameter."""

        def context_func(context):
            return f"DAG: {context['dag_id']}, Task: {context['task_id']}"

        operator = PythonOperator(
            task_id="test_context", python_callable=context_func, provide_context=True
        )

        context = {"dag_id": "test_dag", "task_id": "test_context"}
        result = operator.execute(context)

        assert result == "DAG: test_dag, Task: test_context"

    def test_python_operator_validation(self):
        """Test PythonOperator parameter validation."""

        def test_func():
            return "test"

        # Test invalid callable
        with pytest.raises(
            ValueError, match="python_callable must be a callable object"
        ):
            PythonOperator(task_id="test", python_callable="not_callable")

        # Test valid operator
        operator = PythonOperator(task_id="test", python_callable=test_func)
        operator.validate_parameters()  # Should not raise
