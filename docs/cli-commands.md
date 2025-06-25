# CLI Commands

Spiralarr provides a comprehensive command-line interface for managing your workflow orchestration system. All commands are accessed through the `spiralarr` command.

## Quick Reference

```bash
# Database Management
spiralarr db-init                       # Initialize database
spiralarr db-create-migrations -m "message"  # Create migration files
spiralarr db-migrate                    # Apply migrations

# DAG Management
spiralarr list-dags                     # List all available DAGs
spiralarr show-dag <dag_id>             # Show DAG structure and tasks
spiralarr test-task <dag_id> <task_id>  # Test individual task execution

# Core Components
spiralarr scheduler                     # Start scheduler
spiralarr api                           # Start API server
spiralarr run-task <dag> <task> <run>   # Execute specific task

# Get help
spiralarr --help                        # Show all commands
spiralarr <command> --help              # Show command-specific help
```

## Database Commands

### `db-init`

Initialize the database with the required schema.

```bash
spiralarr db-init
```

**What it does:**
- Creates database connection using configured URL
- Runs Alembic migrations to create all tables
- Sets up the initial schema for DAGs, runs, and task instances

**Environment Variables:**
- `AIRFLOW_LITE_DATABASE_URL` - Override default database URL

**Example:**
```bash
# Use default SQLite database
spiralarr db-init

# Use custom database URL
AIRFLOW_LITE_DATABASE_URL="postgresql://user:pass@localhost/airflow" spiralarr db-init
```

### `db-create-migrations`

Create new database migration files when you modify models.

```bash
spiralarr db-create-migrations -m "Migration description"
```

**Parameters:**
- `-m, --message` (required) - Description of the migration

**What it does:**
- Uses Alembic's autogenerate feature to detect schema changes
- Creates migration files in `src/spiralarr/migrations/versions/`
- Compares current models with database schema

**Example:**
```bash
spiralarr db-create-migrations -m "Add new column to task table"
spiralarr db-create-migrations -m "Create user permissions table"
```

### `db-migrate`

Apply pending migrations to upgrade the database schema.

```bash
spiralarr db-migrate
```

**What it does:**
- Runs all pending Alembic migrations
- Updates database schema to match current models
- Safe to run multiple times (idempotent)

**Example:**
```bash
# Apply all pending migrations
spiralarr db-migrate
```

## DAG Management Commands

### `list-dags`

List all available DAGs discovered from the filesystem.

```bash
spiralarr list-dags [OPTIONS]
```

**Options:**
- `--dags-folder` - Specify DAG files folder (default: "dags")

**What it does:**
- Scans the DAGs folder for Python files containing DAG definitions
- Parses each DAG file to extract metadata
- Displays DAG information including task count, description, and schedule

**Example:**
```bash
# List all DAGs in default folder
spiralarr list-dags

# List DAGs from custom folder
spiralarr list-dags --dags-folder /path/to/my/dags

# Example output:
# Scanning DAGs in folder: dags
#
# Found 3 DAG(s):
#   • example_dag: 7 tasks
#     Description: Comprehensive example DAG demonstrating operators
#     Schedule: 0 0 * * *
#
#   • advanced_examples_dag: 11 tasks
#     Description: Advanced examples demonstrating complex patterns
#     Schedule: 0 6 * * *
```

### `show-dag`

Show detailed structure and tasks for a specific DAG.

```bash
spiralarr show-dag <dag_id> [OPTIONS]
```

**Parameters:**
- `dag_id` - DAG identifier to inspect

**Options:**
- `--dags-folder` - Specify DAG files folder (default: "dags")

**What it does:**
- Loads the specified DAG from filesystem
- Displays DAG metadata (description, schedule, start date)
- Lists all tasks with their operators and key parameters
- Shows task-specific configuration details

**Example:**
```bash
# Show DAG structure
spiralarr show-dag example_dag

# Example output:
# Loading DAG: example_dag
#
# DAG: example_dag
# Description: Comprehensive example DAG demonstrating operators
# Schedule: 0 0 * * *
# Start Date: 2024-01-01 00:00:00
#
# Tasks (7):
#   • bash_hello (BashOperator)
#     Command: echo 'Hello from Bash!'
#   • python_hello (PythonOperator)
#     Callable: hello_world
#   • python_with_context (PythonOperator)
#     Callable: context_aware_function
```

### `test-task`

Test individual task execution without database dependencies.

```bash
spiralarr test-task <dag_id> <task_id> [OPTIONS]
```

**Parameters:**
- `dag_id` - DAG identifier
- `task_id` - Task identifier to test

**Options:**
- `--dags-folder` - Specify DAG files folder (default: "dags")

**What it does:**
- Loads the task operator from DAG file
- Creates a mock execution context for testing
- Executes the task in isolation (no database required)
- Shows task output and execution results
- Perfect for development and debugging

**Example:**
```bash
# Test a Python task
spiralarr test-task example_dag python_hello

# Test a Bash task
spiralarr test-task example_dag bash_hello

# Test context-aware task
spiralarr test-task example_dag python_with_context

# Example output:
# Testing task example_dag.python_hello
# Executing <PythonOperator python_hello callable=hello_world>...
# Hello, World from Python!
# ✓ Task completed successfully
# Result: Hello World task completed
```

**Benefits:**
- **Fast Development** - Test tasks without running scheduler
- **Debugging** - Isolate task issues from scheduling problems
- **Validation** - Verify DAG syntax and task logic
- **No Side Effects** - No database changes or state persistence

## Core Component Commands

### `scheduler`

Start the Spiralarr scheduler process.

```bash
spiralarr scheduler
```

**What it does:**
- **DAG Discovery** - Automatically scans filesystem for DAG files
- **DAG Synchronization** - Syncs DAG metadata to database every 30 seconds
- **DAG Run Creation** - Creates DAG runs for discovered DAGs
- **Task Instance Creation** - Generates task instances for each DAG run
- **Task Scheduling** - Finds runnable tasks and queues them for execution
- **Parallel Execution** - Manages concurrent task execution (default: 4 tasks)
- **Zombie Detection** - Handles stuck tasks and cleanup
- **State Management** - Tracks task state transitions

**Key Features:**
- **On-demand DAG Parsing** - No continuous DAG parsing loops
- **Filesystem Integration** - Direct DAG file loading for workers
- **Per-task Transactions** - Immediate worker visibility
- **Session Management** - Proper SQLAlchemy session handling
- **Graceful Shutdown** - Handles Ctrl+C properly

**Example:**
```bash
# Start scheduler (runs until Ctrl+C)
spiralarr scheduler

# Example output:
# Starting scheduler...
# Scheduler started
# Syncing DAGs from filesystem...
# Added new DAG: example_dag
# Added new DAG: advanced_examples_dag
# Synced 2 DAGs
# Created 7 task instances for example_dag
# Created DAG run: example_dag.manual__20250625T212948
# Task ('example_dag', 'python_hello', 'manual__20250625T212948') state: scheduled → queued
# Starting task ('example_dag', 'python_hello', 'manual__20250625T212948')...
# Task ('example_dag', 'python_hello', 'manual__20250625T212948') completed successfully
```

**Logs:**
The scheduler outputs detailed logs about:
- DAG discovery and synchronization
- DAG run and task instance creation
- Task scheduling and queuing decisions
- Parallel task execution status
- Task completion and state transitions
- Error handling and recovery

### `api`

Start the FastAPI web server for REST API access.

```bash
spiralarr api
```

**What it does:**
- Starts FastAPI server with uvicorn
- Provides REST endpoints for DAG and task management
- Enables programmatic control of Spiralarr

**Configuration:**
- Host: Configured via `AIRFLOW_LITE_API_HOST` (default: 127.0.0.1)
- Port: Configured via `AIRFLOW_LITE_API_PORT` (default: 8080)
- Log Level: Configured via `AIRFLOW_LITE_LOG_LEVEL` (default: INFO)

**Available Endpoints:**
- `GET /api/v1/health` - Health check endpoint

**Example:**
```bash
# Start API server
spiralarr api

# Test health endpoint
curl http://localhost:8080/api/v1/health
```

### `run-task`

Execute a specific task instance (used internally by the executor).

```bash
spiralarr run-task <dag_id> <task_id> <run_id> [OPTIONS]
```

**Parameters:**
- `dag_id` - DAG identifier
- `task_id` - Task identifier within the DAG
- `run_id` - Specific DAG run identifier

**Options:**
- `--verbose, -v` - Enable verbose logging output

**What it does:**
- **Task Runner Component** - The 3rd part of the 3-part execution system
- **Database Integration** - Loads task instance from database using provided identifiers
- **DAG File Parsing** - Dynamically loads task operator from DAG files on filesystem
- **Rich Context Creation** - Builds comprehensive execution context with metadata
- **Operator Execution** - Executes tasks using enhanced operators (Python, Bash, etc.)
- **Context-Aware Execution** - Passes rich context to operators for advanced functionality
- **State Management** - Updates task state transitions with proper session handling
- **Error Handling** - Comprehensive exception handling and logging
- **Lifecycle Management** - Handles pre-execute and post-execute hooks
- **Return Codes** - Returns proper exit codes (0 for success, 1 for failure)

**Execution Flow:**
1. **Database Lookup** - Find TaskInstance by dag_id, task_id, run_id with proper session management
2. **State Update** - Set task state to RUNNING with transaction handling
3. **DAG File Loading** - Parse DAG file from filesystem to load actual operator
4. **Rich Context Building** - Create comprehensive execution context with:
   - Task metadata (dag_id, task_id, run_id)
   - Execution timing (execution_date, logical_date)
   - Date strings (ds, ds_nodash)
   - Object references (task_instance, dag_run)
5. **Pre-execution Hooks** - Run operator.pre_execute(context)
6. **Task Execution** - Call operator.execute(context) with enhanced error handling
7. **Post-execution Hooks** - Run operator.post_execute(context, result)
8. **State Update** - Set final state (SUCCESS/FAILED) and commit to database

**Examples:**
```bash
# Execute specific task (typically called by executor)
spiralarr run-task example_dag python_hello manual__20250625T212948

# Execute with verbose logging for debugging
spiralarr run-task example_dag python_hello manual__20250625T212948 --verbose

# Example output:
# Running task example_dag.python_hello for run manual__20250625T212948
# Task ('example_dag', 'python_hello', 'manual__20250625T212948') state: scheduled → running
# Starting pre-execution for task example_dag.python_hello
# Executing task example_dag.python_hello
# Hello, World from Python!
# Starting post-execution for task example_dag.python_hello
# Task python_hello completed successfully
# Task ('example_dag', 'python_hello', 'manual__20250625T212948') state: running → success
# ✓ Task example_dag.python_hello completed successfully
# Task result: Hello World task completed
```

**Error Handling:**
- **Task Not Found** - Returns exit code 1 if task instance doesn't exist
- **Operator Failure** - Catches exceptions and sets task state to FAILED
- **Database Errors** - Handles connection issues and transaction failures
- **Timeout Handling** - Managed by parent executor process

**Supported Operators:**
- **PythonOperator** - Executes Python functions with arguments and context
- **BashOperator** - Executes shell commands with templating and environment variables
- **Fallback Operators** - Dummy operators for testing and development

**Note:** This command is primarily used internally by the LocalExecutor. You typically don't run it manually unless debugging specific task execution or testing DAG configurations.

## Architecture Integration

### 3-Part Execution System

Spiralarr uses a clean 3-part execution architecture:

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│  Scheduler  │───▶│   Executor   │───▶│ Task Runner │
│             │    │              │    │             │
│ scheduler   │    │ (built-in)   │    │ run-task    │
│ command     │    │              │    │ command     │
└─────────────┘    └──────────────┘    └─────────────┘
```

1. **Scheduler** (`scheduler` command) - Finds schedulable tasks
2. **Executor** (built-in LocalExecutor) - Manages task execution pool
3. **Task Runner** (`run-task` command) - Actually executes tasks

## Configuration

All commands respect these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `AIRFLOW_LITE_DATABASE_URL` | `sqlite:///spiralarr.db` | Database connection URL |
| `AIRFLOW_LITE_API_HOST` | `127.0.0.1` | API server host |
| `AIRFLOW_LITE_API_PORT` | `8080` | API server port |
| `AIRFLOW_LITE_LOG_LEVEL` | `INFO` | Logging level |

## Common Workflows

### Initial Setup
```bash
# 1. Initialize database
spiralarr db-init

# 2. Start scheduler in background
spiralarr scheduler &

# 3. Start API server (optional)
spiralarr api &
```

### Development Workflow
```bash
# 1. Make model changes
# 2. Create migration
spiralarr db-create-migrations -m "Add new feature"

# 3. Apply migration
spiralarr db-migrate

# 4. Restart scheduler
pkill -f "spiralarr scheduler"
spiralarr scheduler &
```

### Production Deployment
```bash
# Set production database
export AIRFLOW_LITE_DATABASE_URL="postgresql://user:pass@db:5432/airflow"

# Initialize production database
spiralarr db-init

# Start scheduler (use process manager like systemd)
spiralarr scheduler
```

## Troubleshooting

### Common Issues

**Database Connection Errors:**
```bash
# Check database URL
echo $AIRFLOW_LITE_DATABASE_URL

# Test database connection
spiralarr db-init
```

**Migration Issues:**
```bash
# Check current migration status
# (View migrations in src/spiralarr/migrations/versions/)

# Force migration to specific version
# (Use Alembic directly if needed)
```

**Task Execution Failures:**
```bash
# Run task manually for debugging
spiralarr run-task my_dag failing_task test_run

# Check task logs and database state
```

### Getting Help

```bash
# General help
spiralarr --help

# Command-specific help
spiralarr scheduler --help
spiralarr db-create-migrations --help
```

For more detailed information, see the [Development Guide](development/contributing.md).
