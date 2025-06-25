"""Advanced examples DAG demonstrating complex operator usage patterns."""

import datetime as dt
import os


def extract_data():
    """Extract data from a source."""
    print("Extracting data from source...")

    # Simulate data extraction
    data = {
        "records": [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 150},
        ],
        "metadata": {
            "extracted_at": str(dt.datetime.now()),
            "source": "example_database",
            "count": 3,
        },
    }

    print(f"Extracted {len(data['records'])} records")
    return data


def transform_data(data):
    """Transform the extracted data."""
    print("Transforming data...")

    # Calculate total value
    total_value = sum(record["value"] for record in data["records"])

    # Add calculated fields
    for record in data["records"]:
        record["percentage"] = round((record["value"] / total_value) * 100, 2)

    # Update metadata
    data["metadata"]["transformed_at"] = str(dt.datetime.now())
    data["metadata"]["total_value"] = total_value

    print(f"Transformed data with total value: {total_value}")
    return data


def load_data(data):
    """Load the transformed data."""
    print("Loading data to destination...")

    # Simulate loading to destination
    print(f"Loading {len(data['records'])} records:")
    for record in data["records"]:
        print(f"  {record['name']}: {record['value']} ({record['percentage']}%)")

    print("Data loaded successfully!")
    return {"status": "success", "records_loaded": len(data["records"])}


def validate_environment():
    """Validate the environment setup."""
    print("Validating environment...")

    required_vars = ["AIRFLOW_LITE_DAG_ID", "AIRFLOW_LITE_TASK_ID"]
    missing_vars = []

    for var in required_vars:
        if var not in os.environ:
            missing_vars.append(var)

    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    print("Environment validation passed!")
    return {"status": "valid", "checked_vars": required_vars}


def error_handling_example():
    """Example function that demonstrates error handling."""
    import random

    # Randomly succeed or fail for testing
    if random.random() < 0.7:  # 70% success rate
        print("Task completed successfully!")
        return {"status": "success", "message": "All operations completed"}
    else:
        raise RuntimeError("Simulated task failure for testing error handling")


def context_processing_example(context):
    """Advanced context processing example."""
    print("Processing execution context...")

    # Extract useful information from context
    dag_info = {
        "dag_id": context["dag_id"],
        "task_id": context["task_id"],
        "run_id": context["run_id"],
        "execution_date": str(context["execution_date"]),
        "ds": context["ds"],
        "ds_nodash": context["ds_nodash"],
    }

    # Log context information
    print("Execution Context:")
    for key, value in dag_info.items():
        print(f"  {key}: {value}")

    # Perform context-dependent logic
    if "test" in context["run_id"].lower():
        print("Running in test mode - using test configuration")
        config = {"mode": "test", "timeout": 30}
    else:
        print("Running in production mode - using production configuration")
        config = {"mode": "production", "timeout": 300}

    return {**dag_info, "config": config}


# Advanced examples DAG configuration
advanced_examples_dag_config = {
    "dag_id": "advanced_examples_dag",
    "description": "Advanced examples demonstrating complex operator usage patterns",
    "schedule_interval": "0 6 * * *",  # Daily at 6 AM
    "start_date": dt.datetime(2024, 1, 1),
    "tasks": [
        # Data pipeline example
        {
            "task_id": "extract_data",
            "operator": "PythonOperator",
            "python_callable": "extract_data",
        },
        {
            "task_id": "transform_data",
            "operator": "PythonOperator",
            "python_callable": "transform_data",
            "op_args": [
                {"records": [], "metadata": {}}
            ],  # Placeholder, would be from previous task
        },
        {
            "task_id": "load_data",
            "operator": "PythonOperator",
            "python_callable": "load_data",
            "op_args": [
                {"records": [], "metadata": {}}
            ],  # Placeholder, would be from previous task
        },
        # Environment and validation examples
        {
            "task_id": "validate_environment",
            "operator": "PythonOperator",
            "python_callable": "validate_environment",
        },
        {
            "task_id": "check_disk_space",
            "operator": "BashOperator",
            "bash_command": """
                echo "Checking disk space..."
                df -h

                # Check if we have at least 1GB free space
                available=$(df / | tail -1 | awk '{print $4}' | sed 's/G//')
                if [ "$available" -lt 1 ]; then
                    echo "ERROR: Less than 1GB free space available"
                    exit 1
                else
                    echo "OK: Sufficient disk space available (${available}GB)"
                fi
            """,
        },
        {
            "task_id": "system_info",
            "operator": "BashOperator",
            "bash_command": """
                echo "=== System Information ==="
                echo "Date: $(date)"
                echo "Hostname: $(hostname)"
                echo "User: $(whoami)"
                echo "Working Directory: $(pwd)"
                echo "Python Version: $(python3 --version)"
                echo "Disk Usage:"
                df -h | head -5
                echo "Memory Usage:"
                free -h 2>/dev/null || echo "free command not available"
                echo "=== End System Information ==="
            """,
        },
        # Advanced context and error handling examples
        {
            "task_id": "context_processing",
            "operator": "PythonOperator",
            "python_callable": "context_processing_example",
            "provide_context": True,
        },
        {
            "task_id": "error_handling_demo",
            "operator": "PythonOperator",
            "python_callable": "error_handling_example",
        },
        # File operations examples
        {
            "task_id": "create_temp_files",
            "operator": "BashOperator",
            "bash_command": """
                echo "Creating temporary files for processing..."

                # Create temp directory
                TEMP_DIR="/tmp/spiralarr_{{ ds_nodash }}"
                mkdir -p "$TEMP_DIR"

                # Create sample files
                echo "Sample data for {{ ds }}" > "$TEMP_DIR/data.txt"
                echo '{"dag_id": "{{ dag_id }}", "task_id": "{{ task_id }}"}' > \
                    "$TEMP_DIR/metadata.json"

                # List created files
                echo "Created files:"
                ls -la "$TEMP_DIR"

                echo "Temporary files created successfully in $TEMP_DIR"
            """,
        },
        {
            "task_id": "process_files",
            "operator": "BashOperator",
            "bash_command": """
                echo "Processing files..."

                TEMP_DIR="/tmp/spiralarr_{{ ds_nodash }}"

                if [ -d "$TEMP_DIR" ]; then
                    echo "Processing files in $TEMP_DIR"

                    # Process data file
                    if [ -f "$TEMP_DIR/data.txt" ]; then
                        echo "Data file content:"
                        cat "$TEMP_DIR/data.txt"

                        # Add processing timestamp
                        echo "Processed at: $(date)" >> "$TEMP_DIR/data.txt"
                    fi

                    # Process metadata file
                    if [ -f "$TEMP_DIR/metadata.json" ]; then
                        echo "Metadata file content:"
                        cat "$TEMP_DIR/metadata.json"
                    fi

                    echo "File processing completed"
                else
                    echo "ERROR: Temp directory $TEMP_DIR not found"
                    exit 1
                fi
            """,
        },
        {
            "task_id": "cleanup_temp_files",
            "operator": "BashOperator",
            "bash_command": """
                echo "Cleaning up temporary files..."

                TEMP_DIR="/tmp/spiralarr_{{ ds_nodash }}"

                if [ -d "$TEMP_DIR" ]; then
                    echo "Removing $TEMP_DIR"
                    rm -rf "$TEMP_DIR"
                    echo "Cleanup completed"
                else
                    echo "No temp directory to clean up"
                fi
            """,
        },
    ],
}
