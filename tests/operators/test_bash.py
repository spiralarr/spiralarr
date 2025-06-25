"""Tests for bash operator."""

from unittest.mock import MagicMock, patch

import pytest

from spiralarr.operators.bash import BashOperator


class TestBashOperator:
    """Test bash operator functionality."""

    def test_bash_operator_success(self):
        """Test BashOperator with successful command."""
        operator = BashOperator(task_id="test_bash", bash_command="echo 'Hello World'")

        context = {"task_id": "test_bash"}
        result = operator.execute(context)

        assert "Hello World" in result

    def test_bash_operator_failure(self):
        """Test BashOperator with failing command."""
        operator = BashOperator(task_id="test_bash_fail", bash_command="exit 1")

        context = {"task_id": "test_bash_fail"}

        with pytest.raises(RuntimeError):
            operator.execute(context)

    def test_bash_operator_templating(self):
        """Test BashOperator command templating."""
        operator = BashOperator(
            task_id="test_template",
            bash_command="echo 'DAG: {{ dag_id }}, Task: {{ task_id }}'",
        )

        context = {
            "dag_id": "test_dag",
            "task_id": "test_template",
            "run_id": "test_run",
            "execution_date": "2024-01-01",
            "ds": "2024-01-01",
            "ds_nodash": "20240101",
        }

        templated = operator._template_command(context)
        assert templated == "echo 'DAG: test_dag, Task: test_template'"

    @patch("subprocess.run")
    def test_bash_operator_with_env(self, mock_run):
        """Test BashOperator with environment variables."""
        mock_result = MagicMock()
        mock_result.stdout = "test_value"
        mock_result.stderr = ""
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        operator = BashOperator(
            task_id="test_env",
            bash_command="echo $TEST_VAR",
            env={"TEST_VAR": "test_value"},
        )

        context = {"dag_id": "test", "task_id": "test_env", "run_id": "test_run"}
        result = operator.execute(context)

        assert result == "test_value"

        # Check that environment variables were passed
        call_args = mock_run.call_args
        env = call_args[1]["env"]
        assert "TEST_VAR" in env
        assert env["TEST_VAR"] == "test_value"

    def test_bash_operator_validation(self):
        """Test BashOperator parameter validation."""
        # Test invalid command
        with pytest.raises(ValueError, match="bash_command must be a non-empty string"):
            BashOperator(task_id="test", bash_command="")

        # Test valid operator
        operator = BashOperator(task_id="test", bash_command="echo 'test'")
        operator.validate_parameters()  # Should not raise

        # Test invalid timeout
        operator.timeout = -1
        with pytest.raises(ValueError, match="timeout must be positive"):
            operator.validate_parameters()
