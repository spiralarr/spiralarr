"""Example DAG demonstrating PythonOperator and BashOperator usage patterns."""

import datetime as dt


def hello_world():
    """Simple hello world function."""
    print("Hello, World from Python!")
    return "Hello World task completed"


def process_data(name="World"):
    """Function that processes data with parameters."""
    message = f"Processing data for {name}"
    print(message)
    return {"status": "completed", "message": message}


def context_aware_function(context):
    """Function that uses execution context."""
    dag_id = context["dag_id"]
    task_id = context["task_id"]
    execution_date = context["execution_date"]

    print(f"Running task {task_id} in DAG {dag_id}")
    print(f"Execution date: {execution_date}")
    print(f"Date string: {context['ds']}")

    return {"dag_id": dag_id, "task_id": task_id, "execution_date": str(execution_date)}


def calculate_metrics():
    """Function that performs calculations."""
    import random

    # Simulate some calculations
    metrics = {
        "total_records": random.randint(1000, 5000),
        "success_rate": round(random.uniform(0.85, 0.99), 3),
        "processing_time": round(random.uniform(10.5, 45.2), 2),
    }

    print(f"Calculated metrics: {metrics}")
    return metrics


# Enhanced example DAG configuration with comprehensive operator examples
example_dag_config = {
    "dag_id": "example_dag",
    "description": (
        "Comprehensive example DAG demonstrating PythonOperator and BashOperator"
    ),
    "schedule_interval": "0 0 * * *",  # Daily at midnight
    "start_date": dt.datetime(2024, 1, 1),
    "tasks": [
        # Basic Bash tasks
        {
            "task_id": "bash_hello",
            "operator": "BashOperator",
            "bash_command": "echo 'Hello from Bash!'",
        },
        {
            "task_id": "bash_with_templating",
            "operator": "BashOperator",
            "bash_command": (
                "echo 'DAG: {{ dag_id }}, Task: {{ task_id }}, Date: {{ ds }}'"
            ),
        },
        {
            "task_id": "bash_multiline",
            "operator": "BashOperator",
            "bash_command": """
                echo "Starting multi-line bash script..."
                echo "Current directory: $(pwd)"
                echo "Current date: $(date)"
                echo "Environment variables:"
                echo "  AIRFLOW_LITE_DAG_ID: $AIRFLOW_LITE_DAG_ID"
                echo "  AIRFLOW_LITE_TASK_ID: $AIRFLOW_LITE_TASK_ID"
                echo "Script completed successfully!"
            """,
        },
        # Basic Python tasks
        {
            "task_id": "python_hello",
            "operator": "PythonOperator",
            "python_callable": "hello_world",
        },
        {
            "task_id": "python_with_args",
            "operator": "PythonOperator",
            "python_callable": "process_data",
            "op_args": ["Airflow-Lite"],
        },
        {
            "task_id": "python_with_context",
            "operator": "PythonOperator",
            "python_callable": "context_aware_function",
            "provide_context": True,
        },
        {
            "task_id": "python_calculations",
            "operator": "PythonOperator",
            "python_callable": "calculate_metrics",
        },
    ],
}
