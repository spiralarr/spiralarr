"""CLI commands for spiralarr."""

import os
import sys

from alembic import command
from alembic.config import Config
import typer

from spiralarr.config.settings import get_settings
from spiralarr.models.base import init_db

app = typer.Typer(help="Airflow-Lite CLI")


@app.command()
def db_init():
    """Initialize the database."""
    typer.echo("Initializing database...")
    settings = get_settings()
    database_url = os.getenv(
        "SPIRALARR_DATABASE__CONNECTION_STRING", settings.database.connection_string
    )
    init_db(database_url)

    cfg = Config(os.path.join(os.path.dirname(__file__), "..", "alembic.ini"))
    command.upgrade(cfg, "head")

    typer.echo("Database initialized successfully!")


@app.command()
def scheduler():
    """Start the scheduler."""
    typer.echo("Starting scheduler...")
    # Import here to avoid circular imports
    from spiralarr.scheduler.scheduler import SchedulerRunner

    settings = get_settings()
    init_db(settings.database.connection_string)
    scheduler_runner = SchedulerRunner()

    try:
        scheduler_runner.run()
    except KeyboardInterrupt:
        typer.echo("Scheduler stopped by user")
        scheduler_runner.shutdown()


@app.command()
def run_task(
    dag_id: str = typer.Argument(..., help="DAG ID"),
    task_id: str = typer.Argument(..., help="Task ID"),
    run_id: str = typer.Argument(..., help="Run ID"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
):
    """
    Run a specific task (Task Runner component).

    This is the 3rd part of the 3-part execution system.
    Called by the LocalExecutor to actually execute tasks.

    Args:
        dag_id: The DAG identifier
        task_id: The task identifier within the DAG
        run_id: The specific run identifier
        verbose: Enable detailed logging output
    """
    settings = get_settings()

    if verbose:
        typer.echo(
            f"[VERBOSE] Starting task runner for {dag_id}.{task_id} (run: {run_id})"
        )
        typer.echo(f"[VERBOSE] Database URL: {settings.database.connection_string}")

    typer.echo(f"Running task {dag_id}.{task_id} for run {run_id}")

    # Import here to avoid circular imports
    from spiralarr.task_runner.runner import TaskRunner

    try:
        # Initialize database connection
        if verbose:
            typer.echo("[VERBOSE] Initializing database connection...")
        init_db(settings.database.connection_string)

        # Create task runner
        if verbose:
            typer.echo("[VERBOSE] Creating task runner...")
        task_runner = TaskRunner()

        # Execute the task
        if verbose:
            typer.echo(f"[VERBOSE] Executing task {dag_id}.{task_id}...")
        success = task_runner.run_task(dag_id, task_id, run_id)

        if success:
            typer.echo(f"✓ Task {dag_id}.{task_id} completed successfully")
            sys.exit(0)
        else:
            typer.echo(f"✗ Task {dag_id}.{task_id} failed")
            sys.exit(1)

    except KeyboardInterrupt:
        typer.echo(f"✗ Task {dag_id}.{task_id} interrupted by user")
        sys.exit(130)  # Standard exit code for SIGINT
    except Exception as e:
        typer.echo(f"✗ Task {dag_id}.{task_id} failed with error: {e}")
        if verbose:
            import traceback

            typer.echo(f"[VERBOSE] Full traceback:\n{traceback.format_exc()}")
        sys.exit(1)


@app.command()
def db_create_migrations(
    message: str = typer.Option(..., "-m", help="Migration message"),
):
    """Create a new migration."""
    typer.echo(f"Creating migration: {message}")
    cfg = Config(os.path.join(os.path.dirname(__file__), "..", "alembic.ini"))
    command.revision(cfg, message=message, autogenerate=True)


@app.command()
def db_migrate():
    """Upgrade the database to the latest revision."""
    typer.echo("Migrating database...")
    settings = get_settings()
    database_url = os.getenv(
        "SPIRALARR_DATABASE__CONNECTION_STRING", settings.database.connection_string
    )
    init_db(database_url)
    cfg = Config(os.path.join(os.path.dirname(__file__), "..", "alembic.ini"))
    command.upgrade(cfg, "head")
    typer.echo("Database migrated successfully!")


@app.command()
def list_dags(
    dags_folder: str = typer.Option("dags", "--dags-folder", help="DAG files folder"),
):
    """List all available DAGs."""
    typer.echo(f"Scanning DAGs in folder: {dags_folder}")

    # Import here to avoid circular imports
    from spiralarr.utils.dag_loader import DagLoader

    try:
        loader = DagLoader(dags_folder)
        dags = loader.list_available_dags()

        if not dags:
            typer.echo("No DAGs found.")
            return

        typer.echo(f"\nFound {len(dags)} DAG(s):")
        for dag_id, config in dags.items():
            tasks_count = len(config.get("tasks", []))
            description = config.get("description", "No description")
            schedule = config.get("schedule_interval", "No schedule")
            typer.echo(f"  • {dag_id}: {tasks_count} tasks")
            typer.echo(f"    Description: {description}")
            typer.echo(f"    Schedule: {schedule}")
            typer.echo()

    except Exception as e:
        typer.echo(f"Error listing DAGs: {e}")
        sys.exit(1)


@app.command()
def show_dag(
    dag_id: str = typer.Argument(..., help="DAG ID to show"),
    dags_folder: str = typer.Option("dags", "--dags-folder", help="DAG files folder"),
):
    """Show DAG structure and tasks."""
    typer.echo(f"Loading DAG: {dag_id}")

    # Import here to avoid circular imports
    from spiralarr.utils.dag_loader import DagLoader

    try:
        loader = DagLoader(dags_folder)
        dag_config = loader.load_dag_config(dag_id)

        if not dag_config:
            typer.echo(f"DAG '{dag_id}' not found.")
            sys.exit(1)

        # Show DAG info
        typer.echo(f"\nDAG: {dag_config['dag_id']}")
        typer.echo(f"Description: {dag_config.get('description', 'No description')}")
        typer.echo(f"Schedule: {dag_config.get('schedule_interval', 'No schedule')}")
        typer.echo(f"Start Date: {dag_config.get('start_date', 'Not specified')}")

        # Show tasks
        tasks = dag_config.get("tasks", [])
        typer.echo(f"\nTasks ({len(tasks)}):")
        for task in tasks:
            task_id = task.get("task_id", "Unknown")
            operator = task.get("operator", "Unknown")
            typer.echo(f"  • {task_id} ({operator})")

            # Show task-specific details
            if operator == "BashOperator":
                cmd = task.get("bash_command", "")
                if len(cmd) > 50:
                    cmd = cmd[:47] + "..."
                typer.echo(f"    Command: {cmd}")
            elif operator == "PythonOperator":
                callable_name = task.get("python_callable", "")
                typer.echo(f"    Callable: {callable_name}")

    except Exception as e:
        typer.echo(f"Error showing DAG: {e}")
        sys.exit(1)


@app.command()
def test_task(
    dag_id: str = typer.Argument(..., help="DAG ID"),
    task_id: str = typer.Argument(..., help="Task ID"),
    dags_folder: str = typer.Option("dags", "--dags-folder", help="DAG files folder"),
):
    """Test a task by loading and executing it without database."""
    typer.echo(f"Testing task {dag_id}.{task_id}")

    # Import here to avoid circular imports
    from datetime import datetime

    from spiralarr.utils.dag_loader import DagLoader

    try:
        loader = DagLoader(dags_folder)
        operator = loader.create_task_operator(dag_id, task_id)

        if not operator:
            typer.echo(f"Task '{task_id}' not found in DAG '{dag_id}'.")
            sys.exit(1)

        # Create a mock context for testing
        mock_context = {
            "dag_id": dag_id,
            "task_id": task_id,
            "run_id": "test_run",
            "execution_date": datetime.now(),
            "logical_date": datetime.now(),
            "ds": datetime.now().strftime("%Y-%m-%d"),
            "ds_nodash": datetime.now().strftime("%Y%m%d"),
            "task_instance": None,
            "dag_run": None,
            "params": {},
            "conf": {},
            "var": None,
            "conn": None,
        }

        typer.echo(f"Executing {operator}...")

        # Execute the task
        result = operator.execute(mock_context)

        typer.echo("✓ Task completed successfully")
        if result is not None:
            typer.echo(f"Result: {result}")

    except Exception as e:
        typer.echo(f"✗ Task failed: {e}")
        import traceback

        typer.echo(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


@app.command()
def api():
    """Start the API server."""
    settings = get_settings()
    typer.echo(f"Starting API server on {settings.api.host}:{settings.api.port}")

    # Import here to avoid circular imports
    import uvicorn

    from spiralarr.api.app import app as fastapi_app

    init_db(settings.database.connection_string)

    uvicorn.run(
        fastapi_app,
        host=settings.api.host,
        port=settings.api.port,
        log_level=settings.logging.component_log_level.lower(),
    )


if __name__ == "__main__":
    app()
