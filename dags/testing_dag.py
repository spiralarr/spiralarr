"""Testing DAG for demonstrating error handling and edge cases."""

import datetime as dt
import time


def quick_success_task():
    """Task that completes quickly and successfully."""
    print("Quick success task starting...")
    time.sleep(1)  # Simulate brief processing
    print("Quick success task completed!")
    return {"status": "success", "duration": 1}


def slow_success_task():
    """Task that takes longer but completes successfully."""
    print("Slow success task starting...")
    time.sleep(5)  # Simulate longer processing
    print("Slow success task completed!")
    return {"status": "success", "duration": 5}


def failing_task():
    """Task that always fails for testing error handling."""
    print("Failing task starting...")
    time.sleep(2)  # Simulate some processing before failure
    raise ValueError("This task is designed to fail for testing purposes")


def timeout_simulation_task():
    """Task that simulates a very long-running operation."""
    print("Long-running task starting...")
    print("This task simulates a long-running operation...")

    # This would timeout if executor has timeout configured
    for i in range(100):
        print(f"Processing step {i + 1}/100...")
        time.sleep(1)  # Sleep for 1 second each iteration (100 seconds total)

    print("Long-running task completed!")
    return {"status": "success", "steps": 100}


def memory_intensive_task():
    """Task that uses significant memory for testing resource limits."""
    print("Memory intensive task starting...")

    # Create large data structures
    large_list = []
    for i in range(100000):
        large_list.append(f"Item {i} with some additional data to use memory")

    print(f"Created list with {len(large_list)} items")

    # Process the data
    processed_count = 0
    for item in large_list:
        if "data" in item:
            processed_count += 1

    print(f"Processed {processed_count} items")

    # Clean up
    del large_list

    return {"status": "success", "processed_items": processed_count}


def context_validation_task(context):
    """Task that validates the execution context."""
    print("Context validation task starting...")

    required_keys = [
        "dag_id",
        "task_id",
        "run_id",
        "execution_date",
        "task_instance",
        "dag_run",
        "ds",
        "ds_nodash",
    ]

    missing_keys = []
    for key in required_keys:
        if key not in context:
            missing_keys.append(key)

    if missing_keys:
        raise ValueError(f"Missing required context keys: {missing_keys}")

    print("Context validation passed!")
    print(f"Context keys present: {list(context.keys())}")

    return {
        "status": "success",
        "context_keys": list(context.keys()),
        "dag_id": context["dag_id"],
        "task_id": context["task_id"],
    }


def parameter_testing_task(param1, param2="default", **kwargs):
    """Task that tests parameter passing."""
    print("Parameter testing task starting...")
    print(f"param1: {param1}")
    print(f"param2: {param2}")
    print(f"kwargs: {kwargs}")

    result = {"param1": param1, "param2": param2, "kwargs_count": len(kwargs)}

    print(f"Parameter test result: {result}")
    return result


# Testing DAG configuration
testing_dag_config = {
    "dag_id": "testing_dag",
    "description": "Testing DAG for error handling and edge cases",
    "schedule_interval": None,  # Manual trigger only
    "start_date": dt.datetime(2024, 1, 1),
    "tasks": [
        # Success scenarios
        {
            "task_id": "quick_success",
            "operator": "PythonOperator",
            "python_callable": "quick_success_task",
        },
        {
            "task_id": "slow_success",
            "operator": "PythonOperator",
            "python_callable": "slow_success_task",
        },
        # Failure scenarios
        {
            "task_id": "designed_failure",
            "operator": "PythonOperator",
            "python_callable": "failing_task",
        },
        {
            "task_id": "bash_failure",
            "operator": "BashOperator",
            "bash_command": "echo 'This command will fail' && exit 1",
        },
        # Resource and timeout testing
        {
            "task_id": "memory_intensive",
            "operator": "PythonOperator",
            "python_callable": "memory_intensive_task",
        },
        {
            "task_id": "timeout_simulation",
            "operator": "PythonOperator",
            "python_callable": "timeout_simulation_task",
        },
        # Context and parameter testing
        {
            "task_id": "context_validation",
            "operator": "PythonOperator",
            "python_callable": "context_validation_task",
            "provide_context": True,
        },
        {
            "task_id": "parameter_testing",
            "operator": "PythonOperator",
            "python_callable": "parameter_testing_task",
            "op_args": ["test_value"],
            "op_kwargs": {"param2": "custom_value", "extra_param": "extra_value"},
        },
        # Bash testing scenarios
        {
            "task_id": "bash_success",
            "operator": "BashOperator",
            "bash_command": """
                echo "Bash success test starting..."
                echo "Current date: $(date)"
                echo "DAG ID: {{ dag_id }}"
                echo "Task ID: {{ task_id }}"
                echo "Execution Date: {{ ds }}"
                echo "Bash success test completed!"
            """,
        },
        {
            "task_id": "bash_with_output",
            "operator": "BashOperator",
            "bash_command": """
                echo "Generating test output..."

                # Create some output
                for i in {1..5}; do
                    echo "Output line $i: Processing item $i"
                    sleep 1
                done

                echo "Test output generation completed!"
            """,
        },
        {
            "task_id": "bash_environment_test",
            "operator": "BashOperator",
            "bash_command": """
                echo "Testing environment variables..."
                echo "AIRFLOW_LITE_DAG_ID: $AIRFLOW_LITE_DAG_ID"
                echo "AIRFLOW_LITE_TASK_ID: $AIRFLOW_LITE_TASK_ID"
                echo "AIRFLOW_LITE_RUN_ID: $AIRFLOW_LITE_RUN_ID"
                echo "AIRFLOW_LITE_EXECUTION_DATE: $AIRFLOW_LITE_EXECUTION_DATE"

                # Test command substitution
                echo "Current working directory: $(pwd)"
                echo "Available disk space: $(df -h . | tail -1 | awk '{print $4}')"

                echo "Environment test completed!"
            """,
        },
        # Edge case testing
        {
            "task_id": "empty_return_task",
            "operator": "PythonOperator",
            "python_callable": "lambda: None",  # Returns None
        },
        {
            "task_id": "large_output_task",
            "operator": "BashOperator",
            "bash_command": """
                echo "Generating large output..."

                # Generate substantial output
                for i in {1..100}; do
                    echo "Large output line $i: Test content for output handling"
                done

                echo "Large output generation completed!"
            """,
        },
    ],
}
