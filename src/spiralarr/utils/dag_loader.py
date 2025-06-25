"""DAG loading utilities for spiralarr."""

import importlib.util
import os
import sys
from typing import Any, Dict, Optional

from spiralarr.operators.base import BaseOperator
from spiralarr.operators.bash import BashOperator
from spiralarr.operators.python import PythonOperator


class DagLoader:
    """
    Utility class for loading DAG definitions and creating task objects.

    This is a simplified DAG loader for Phase 2. In later phases, this will
    be enhanced to support proper DAG classes and more complex loading logic.
    """

    def __init__(self, dags_folder: str = None):
        """
        Initialize the DAG loader.

        Args:
            dags_folder: Path to the folder containing DAG files
        """
        if dags_folder is None:
            # Default to the dags folder in the project
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(
                os.path.dirname(os.path.dirname(current_dir))
            )
            self.dags_folder = os.path.join(project_root, "dags")
        else:
            self.dags_folder = dags_folder

        self._dag_cache: Dict[str, Dict[str, Any]] = {}

    def load_dag_config(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """
        Load DAG configuration from DAG files.

        Args:
            dag_id: The DAG identifier to load

        Returns:
            DAG configuration dictionary or None if not found
        """
        if dag_id in self._dag_cache:
            return self._dag_cache[dag_id]

        # Search for DAG files in the dags folder
        try:
            filenames = os.listdir(self.dags_folder)
        except (FileNotFoundError, OSError):
            # Handle missing or inaccessible dags folder gracefully
            return None

        for filename in filenames:
            if filename.endswith(".py") and not filename.startswith("__"):
                dag_config = self._load_dag_from_file(
                    os.path.join(self.dags_folder, filename), dag_id
                )
                if dag_config:
                    self._dag_cache[dag_id] = dag_config
                    return dag_config

        return None

    def _load_dag_from_file(
        self, file_path: str, target_dag_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Load DAG configuration from a specific Python file.

        Args:
            file_path: Path to the Python file
            target_dag_id: The DAG ID we're looking for

        Returns:
            DAG configuration if found, None otherwise
        """
        try:
            # Load the module
            spec = importlib.util.spec_from_file_location("dag_module", file_path)
            if spec is None or spec.loader is None:
                return None

            module = importlib.util.module_from_spec(spec)

            # Add the module to sys.modules temporarily to handle imports
            module_name = f"dag_module_{target_dag_id}"
            sys.modules[module_name] = module

            try:
                spec.loader.exec_module(module)

                # Look for DAG configuration
                # First, try to find a config dictionary
                for attr_name in dir(module):
                    if attr_name.endswith("_dag_config"):
                        config = getattr(module, attr_name)
                        if (
                            isinstance(config, dict)
                            and config.get("dag_id") == target_dag_id
                        ):
                            return config

                # If no config found, return None
                return None

            finally:
                # Clean up the module from sys.modules
                if module_name in sys.modules:
                    del sys.modules[module_name]

        except Exception as e:
            print(f"Error loading DAG from {file_path}: {e}")
            return None

    def create_task_operator(self, dag_id: str, task_id: str) -> Optional[BaseOperator]:
        """
        Create a task operator instance from DAG configuration.

        Args:
            dag_id: The DAG identifier
            task_id: The task identifier

        Returns:
            Operator instance or None if not found
        """
        dag_config = self.load_dag_config(dag_id)
        if not dag_config:
            print(f"DAG {dag_id} not found")
            return None

        # Find the task configuration
        task_config = None
        for task in dag_config.get("tasks", []):
            if task.get("task_id") == task_id:
                task_config = task
                break

        if not task_config:
            print(f"Task {task_id} not found in DAG {dag_id}")
            return None

        # Create the operator based on the configuration
        operator_type = task_config.get("operator")

        if operator_type == "BashOperator":
            return BashOperator(
                task_id=task_id,
                dag_id=dag_id,
                bash_command=task_config.get(
                    "bash_command", 'echo "No command specified"'
                ),
            )
        elif operator_type == "PythonOperator":
            # For Python operators, we need to resolve the callable
            callable_name = task_config.get("python_callable")
            if callable_name:
                python_callable = self._resolve_python_callable(dag_id, callable_name)
                if python_callable:
                    return PythonOperator(
                        task_id=task_id,
                        dag_id=dag_id,
                        python_callable=python_callable,
                        op_args=task_config.get("op_args", []),
                        op_kwargs=task_config.get("op_kwargs", {}),
                    )
                else:
                    print(f"Could not resolve Python callable: {callable_name}")
                    return None
            else:
                print(f"No python_callable specified for task {task_id}")
                return None
        else:
            print(f"Unknown operator type: {operator_type}")
            return None

    def _resolve_python_callable(
        self, dag_id: str, callable_name: str
    ) -> Optional[callable]:
        """
        Resolve a Python callable from the DAG module.

        Args:
            dag_id: The DAG identifier
            callable_name: Name of the callable to resolve

        Returns:
            The callable function or None if not found
        """
        # Re-load the DAG file to get access to the functions
        try:
            filenames = os.listdir(self.dags_folder)
        except (FileNotFoundError, OSError):
            # Handle missing or inaccessible dags folder gracefully
            return None

        for filename in filenames:
            if filename.endswith(".py") and not filename.startswith("__"):
                file_path = os.path.join(self.dags_folder, filename)
                try:
                    spec = importlib.util.spec_from_file_location(
                        "dag_module", file_path
                    )
                    if spec is None or spec.loader is None:
                        continue

                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)

                    # Check if this module contains our DAG
                    for attr_name in dir(module):
                        if attr_name.endswith("_dag_config"):
                            config = getattr(module, attr_name)
                            if (
                                isinstance(config, dict)
                                and config.get("dag_id") == dag_id
                            ):
                                # This is the right module, look for the callable
                                if hasattr(module, callable_name):
                                    return getattr(module, callable_name)

                except Exception as e:
                    print(f"Error resolving callable from {file_path}: {e}")
                    continue

        return None

    def list_available_dags(self) -> Dict[str, Dict[str, Any]]:
        """
        List all available DAGs in the dags folder.

        Returns:
            Dictionary mapping dag_id to DAG configuration
        """
        available_dags = {}

        try:
            filenames = os.listdir(self.dags_folder)
        except (FileNotFoundError, OSError):
            # Handle missing or inaccessible dags folder gracefully
            return available_dags

        for filename in filenames:
            if filename.endswith(".py") and not filename.startswith("__"):
                file_path = os.path.join(self.dags_folder, filename)
                try:
                    spec = importlib.util.spec_from_file_location(
                        "dag_module", file_path
                    )
                    if spec is None or spec.loader is None:
                        continue

                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)

                    # Look for DAG configurations
                    for attr_name in dir(module):
                        if attr_name.endswith("_dag_config"):
                            config = getattr(module, attr_name)
                            if isinstance(config, dict) and "dag_id" in config:
                                available_dags[config["dag_id"]] = config

                except Exception as e:
                    print(f"Error loading DAGs from {file_path}: {e}")
                    continue

        return available_dags


# Global DAG loader instance
_dag_loader = None


def get_dag_loader() -> DagLoader:
    """Get the global DAG loader instance."""
    global _dag_loader
    if _dag_loader is None:
        _dag_loader = DagLoader()
    return _dag_loader


def load_task_operator(dag_id: str, task_id: str) -> Optional[BaseOperator]:
    """
    Convenience function to load a task operator.

    Args:
        dag_id: The DAG identifier
        task_id: The task identifier

    Returns:
        Operator instance or None if not found
    """
    return get_dag_loader().create_task_operator(dag_id, task_id)
