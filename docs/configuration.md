# Spiralarr Configuration Guide

Spiralarr uses a modern, type-safe configuration system built with Pydantic BaseSettings. This provides flexible configuration through multiple sources with clear validation and excellent developer experience.

## Configuration Sources (Priority Order)

Configuration is loaded in the following priority order (later sources override earlier ones):

1. **Default Values** (lowest priority) - Built-in defaults in the code
2. **TOML Configuration File** - User-friendly configuration files
3. **Environment Variables** (highest priority) - Perfect for production deployments

## Configuration Structure

The configuration is organized into logical sections:

### Core Settings (`core`)
- `spiralarr_home`: Base directory for Spiralarr data (default: `~/.spiralarr`)
- `dags_folder`: Directory containing DAG files (default: `~/.spiralarr/dags`)
- `logs_folder`: Directory for log files (default: `~/.spiralarr/logs`)
- `secret_key`: Security key for sessions (auto-generated if not provided)

### Database Settings (`database`)
- `connection_string`: Database connection URL (default: SQLite in spiralarr_home)
- `echo_sql`: Enable SQL query logging for debugging (default: `false`)

### Scheduler Settings (`scheduler`)
- `dag_discovery_interval`: How often to scan for new DAGs in seconds (default: `300`)
- `max_threads`: Maximum parallel threads for task execution (default: `4`)
- `heartbeat_sec`: Scheduler heartbeat interval in seconds (default: `5`)
- `zombie_task_threshold_sec`: Time before a QUEUED task is considered zombie (default: `300`)

### API Settings (`api`)
- `host`: API server host (default: `127.0.0.1`)
- `port`: API server port (default: `8080`)

### Logging Settings (`logging`)
- `component_log_level`: Log level for Spiralarr components (default: `WARN`)
- `task_log_level`: Log level for individual task execution (default: `INFO`)
- `log_file_template`: Jinja2 template for log file naming

## Configuration Methods

### 1. TOML Configuration File

Create a `spiralarr.toml` file in one of these locations (searched in order):
- Current working directory
- `~/.spiralarr/spiralarr.toml`
- `~/spiralarr.toml`

Example `spiralarr.toml`:

```toml
[core]
spiralarr_home = "/custom/spiralarr"
dags_folder = "/custom/spiralarr/dags"

[database]
connection_string = "postgresql://user:password@localhost:5432/spiralarr"
echo_sql = false

[scheduler]
dag_discovery_interval = 60
max_threads = 8

[api]
host = "0.0.0.0"
port = 8080

[logging]
component_log_level = "INFO"
task_log_level = "DEBUG"
```

### 2. Environment Variables

Use the format `SPIRALARR_<SECTION>__<KEY>` (note the double underscore for nested values):

```bash
# Database configuration
export SPIRALARR_DATABASE__CONNECTION_STRING="postgresql://user:pass@host:port/db"
export SPIRALARR_DATABASE__ECHO_SQL="true"

# API configuration
export SPIRALARR_API__HOST="0.0.0.0"
export SPIRALARR_API__PORT="9090"

# Scheduler configuration
export SPIRALARR_SCHEDULER__MAX_THREADS="16"
export SPIRALARR_SCHEDULER__DAG_DISCOVERY_INTERVAL="120"

# Logging configuration
export SPIRALARR_LOGGING__COMPONENT_LOG_LEVEL="DEBUG"
export SPIRALARR_LOGGING__TASK_LOG_LEVEL="INFO"
```

### 3. Programmatic Access

```python
from spiralarr.config.settings import get_settings

# Get singleton settings instance
settings = get_settings()

# Access nested configuration
print(f"Database: {settings.database.connection_string}")
print(f"API endpoint: {settings.api.host}:{settings.api.port}")
print(f"Max threads: {settings.scheduler.max_threads}")
```

## Production Deployment Examples

### Docker Environment

```dockerfile
ENV SPIRALARR_DATABASE__CONNECTION_STRING="postgresql://spiralarr:password@db:5432/spiralarr"
ENV SPIRALARR_API__HOST="0.0.0.0"
ENV SPIRALARR_API__PORT="8080"
ENV SPIRALARR_LOGGING__COMPONENT_LOG_LEVEL="INFO"
```

### Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: spiralarr-config
data:
  SPIRALARR_DATABASE__CONNECTION_STRING: "postgresql://spiralarr:password@postgres:5432/spiralarr"
  SPIRALARR_API__HOST: "0.0.0.0"
  SPIRALARR_SCHEDULER__MAX_THREADS: "8"
  SPIRALARR_LOGGING__COMPONENT_LOG_LEVEL: "INFO"
```

## Database Connection Examples

### SQLite (Default)
```toml
[database]
connection_string = "sqlite:///~/.spiralarr/spiralarr.db"
```

### PostgreSQL
```toml
[database]
connection_string = "postgresql://username:password@localhost:5432/spiralarr"
```

### MySQL
```toml
[database]
connection_string = "mysql://username:password@localhost:3306/spiralarr"
```

## Validation and Type Safety

The configuration system automatically validates:
- Data types (strings, integers, booleans)
- Enum values (log levels must be valid)
- Required vs optional fields
- Nested structure integrity

Invalid configuration will raise clear error messages at startup, preventing runtime issues.

## Migration from Legacy Configuration

If you're upgrading from an older version, update your environment variables:

**Old format:**
```bash
AIRFLOW_LITE_DATABASE_URL="sqlite:///spiralarr.db"
AIRFLOW_LITE_API_HOST="127.0.0.1"
```

**New format:**
```bash
SPIRALARR_DATABASE__CONNECTION_STRING="sqlite:///spiralarr.db"
SPIRALARR_API__HOST="127.0.0.1"
```

The new system provides better organization, type safety, and more flexible configuration options.
