# Troubleshooting Guide

This guide helps you diagnose and resolve common issues with Spiralarr.

## 🚨 Common Issues

### Database Connection Problems

#### Symptoms
- `Database connection failed` errors
- `No such table` errors
- `Database is locked` errors

#### Solutions

**Check Database URL**:
```bash
echo $AIRFLOW_LITE_DATABASE_URL
# Should show your database connection string
```

**Initialize Database**:
```bash
# Initialize or reinitialize the database
spiralarr db-init
```

**SQLite Lock Issues**:
```bash
# Check if database file is accessible
ls -la spiralarr.db

# Check for stale lock files
rm -f spiralarr.db-wal spiralarr.db-shm

# Restart all spiralarr processes
pkill -f spiralarr
spiralarr scheduler &
```

**PostgreSQL Connection Issues**:
```bash
# Test connection manually
psql $AIRFLOW_LITE_DATABASE_URL -c "SELECT 1;"

# Check if database exists
createdb spiralarr  # If using PostgreSQL
```

### Task Execution Failures

#### Symptoms
- Tasks stuck in RUNNING state
- Tasks failing with unclear errors
- `Task not found` errors

#### Debugging Steps

**Check Task State**:
```bash
# Run task manually for debugging
spiralarr run-task my_dag my_task test_run --verbose
```

**Verify DAG Configuration**:
```python
# Check DAG file syntax
python3 dags/my_dag.py

# Verify DAG config structure
from spiralarr.utils.dag_loader import get_dag_loader
loader = get_dag_loader()
config = loader.load_dag_config("my_dag")
print(config)
```

**Check Operator Configuration**:
```bash
# For PythonOperator - verify callable exists
python3 -c "from dags.my_dag import my_function; print(my_function)"

# For BashOperator - test command manually
bash -c "echo 'test command'"
```

**Database State Inspection**:
```sql
-- Check task instance state
SELECT dag_id, task_id, run_id, state, start_date, end_date
FROM task_instance
WHERE dag_id = 'my_dag'
ORDER BY start_date DESC;

-- Check DAG runs
SELECT dag_id, run_id, state, execution_date
FROM dag_run
WHERE dag_id = 'my_dag'
ORDER BY execution_date DESC;
```

### Scheduler Issues

#### Symptoms
- Scheduler not picking up tasks
- Scheduler crashing or stopping
- Tasks not being scheduled

#### Solutions

**Check Scheduler Status**:
```bash
# Check if scheduler is running
ps aux | grep "spiralarr scheduler"

# Check scheduler logs
spiralarr scheduler  # Run in foreground to see logs
```

**Verify DAG Files**:
```bash
# Check DAG directory
ls -la dags/

# Validate DAG files
python3 -m py_compile dags/*.py
```

**Database State Check**:
```sql
-- Check for zombie tasks
SELECT * FROM task_instance
WHERE state = 'RUNNING'
AND start_date < datetime('now', '-1 hour');

-- Reset zombie tasks
UPDATE task_instance
SET state = 'FAILED'
WHERE state = 'RUNNING'
AND start_date < datetime('now', '-1 hour');
```

### Executor Problems

#### Symptoms
- Tasks queued but not executing
- Executor at capacity
- Process creation failures

#### Debugging

**Check Executor Stats**:
```python
from spiralarr.executors.local import LocalExecutor
executor = LocalExecutor()
print(executor.get_executor_stats())
```

**Monitor System Resources**:
```bash
# Check available memory
free -h

# Check CPU usage
top

# Check process limits
ulimit -a

# Check disk space
df -h
```

**Adjust Executor Settings**:
```python
# In your configuration
executor.max_parallelism = 2  # Reduce if resource constrained
executor.task_timeout = 1800  # 30 minutes
```

## 🔧 Debugging Techniques

### Verbose Logging

**Enable Verbose Mode**:
```bash
# For task execution
spiralarr run-task my_dag my_task test_run --verbose

# For scheduler (set log level)
AIRFLOW_LITE_LOG_LEVEL=DEBUG spiralarr scheduler
```

### Manual Task Testing

**Test Python Functions**:
```python
# Test function directly
from dags.my_dag import my_function
result = my_function()
print(f"Result: {result}")

# Test with context
from spiralarr.utils.context import ExecutionContext
context = {
    "dag_id": "test_dag",
    "task_id": "test_task",
    "run_id": "manual_test",
    "execution_date": "2024-01-01",
    "task_instance": None,
    "dag_run": None,
}
result = my_function(context)  # If function accepts context
```

**Test Bash Commands**:
```bash
# Test command directly
bash -c "echo 'Hello World'"

# Test with environment variables
AIRFLOW_LITE_DAG_ID=test_dag \
AIRFLOW_LITE_TASK_ID=test_task \
bash -c "echo 'DAG: $AIRFLOW_LITE_DAG_ID, Task: $AIRFLOW_LITE_TASK_ID'"
```

### Database Inspection

**SQLite Browser**:
```bash
# Install sqlite3 browser
sudo apt-get install sqlite3  # Ubuntu/Debian
brew install sqlite3          # macOS

# Open database
sqlite3 spiralarr.db

# Useful queries
.tables                       # List all tables
.schema task_instance        # Show table schema
SELECT * FROM task_instance LIMIT 5;
```

**Database Queries**:
```sql
-- Find failed tasks
SELECT dag_id, task_id, run_id, state, start_date, end_date
FROM task_instance
WHERE state = 'FAILED'
ORDER BY start_date DESC;

-- Check task duration
SELECT dag_id, task_id,
       AVG(duration) as avg_duration,
       MAX(duration) as max_duration
FROM task_instance
WHERE state = 'SUCCESS' AND duration IS NOT NULL
GROUP BY dag_id, task_id;

-- Find long-running tasks
SELECT dag_id, task_id, run_id,
       start_date,
       (julianday('now') - julianday(start_date)) * 24 * 60 as minutes_running
FROM task_instance
WHERE state = 'RUNNING'
ORDER BY minutes_running DESC;
```

## 🐛 Error Messages and Solutions

### "Task not found in DAG"

**Cause**: Task ID doesn't exist in DAG configuration

**Solution**:
```python
# Check DAG configuration
from spiralarr.utils.dag_loader import get_dag_loader
loader = get_dag_loader()
config = loader.load_dag_config("my_dag")
task_ids = [task["task_id"] for task in config.get("tasks", [])]
print(f"Available tasks: {task_ids}")
```

### "Python callable not found"

**Cause**: Function name doesn't exist in DAG file

**Solution**:
```python
# Check if function exists
import importlib.util
spec = importlib.util.spec_from_file_location("dag_module", "dags/my_dag.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
print(dir(module))  # List all available functions
```

### "Bash command failed with return code X"

**Cause**: Shell command returned non-zero exit code

**Solution**:
```bash
# Test command manually
bash -c "your_command_here"
echo $?  # Check exit code

# Add error handling to command
bash -c "your_command_here || echo 'Command failed but continuing'"
```

### "Database is locked"

**Cause**: Multiple processes accessing SQLite simultaneously

**Solution**:
```bash
# Stop all spiralarr processes
pkill -f spiralarr

# Remove lock files
rm -f spiralarr.db-wal spiralarr.db-shm

# Restart with single process first
spiralarr scheduler
```

## 🔍 Performance Issues

### Slow Task Execution

**Check System Resources**:
```bash
# Monitor during task execution
htop
iotop  # If available
```

**Optimize Database**:
```sql
-- SQLite optimization
PRAGMA optimize;
VACUUM;
ANALYZE;
```

**Reduce Parallelism**:
```python
# Temporarily reduce concurrent tasks
executor.max_parallelism = 1
```

### Memory Issues

**Monitor Memory Usage**:
```bash
# Check memory usage by process
ps aux --sort=-%mem | head

# Monitor memory during execution
watch -n 1 'free -h'
```

**Optimize Python Tasks**:
```python
# In your Python functions
import gc

def memory_intensive_task():
    # Your processing
    large_data = process_data()

    # Clean up explicitly
    del large_data
    gc.collect()

    return result
```

## 📞 Getting Help

### Log Collection

**Collect System Information**:
```bash
# Create debug info bundle
echo "=== System Info ===" > debug_info.txt
uname -a >> debug_info.txt
python3 --version >> debug_info.txt
echo "=== Database Info ===" >> debug_info.txt
ls -la spiralarr.db* >> debug_info.txt
echo "=== Process Info ===" >> debug_info.txt
ps aux | grep spiralarr >> debug_info.txt
echo "=== Recent Logs ===" >> debug_info.txt
# Add any log files you have
```

### Minimal Reproduction

**Create Test Case**:
```python
# Create minimal DAG for testing
test_dag_config = {
    "dag_id": "debug_dag",
    "description": "Minimal DAG for debugging",
    "schedule_interval": None,
    "start_date": "2024-01-01",
    "tasks": [
        {
            "task_id": "simple_test",
            "operator": "PythonOperator",
            "python_callable": "lambda: print('Hello Debug')",
        }
    ],
}
```

### Community Resources

- 📖 [Documentation](index.md)
- 🐛 [Issue Tracker](TBD)
- 💬 [Discussions](TBD)
- 📧 [Support](TBD)

## 🔄 Recovery Procedures

### Reset Task States

```sql
-- Reset failed tasks to retry
UPDATE task_instance
SET state = 'SCHEDULED',
    start_date = NULL,
    end_date = NULL,
    duration = NULL
WHERE state = 'FAILED'
AND dag_id = 'my_dag';
```

### Clean Database

```sql
-- Remove old task instances (older than 30 days)
DELETE FROM task_instance
WHERE start_date < datetime('now', '-30 days');

-- Remove old DAG runs
DELETE FROM dag_run
WHERE execution_date < datetime('now', '-30 days');
```

### Fresh Start

```bash
# Complete reset (WARNING: Loses all data)
rm -f spiralarr.db*
spiralarr db-init
spiralarr scheduler &
```
