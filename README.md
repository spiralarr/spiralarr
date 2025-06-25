# Spiralarr 🚀

A minimal workflow orchestrator inspired by Apache Airflow, built for simplicity and performance.

## ✨ Features

- **🏗️ 3-Part Execution System**: Scheduler → Executor → Task Runner
- **⚡ Per-Task Transactions**: Immediate worker visibility with zombie protection
- **🗄️ Database as Source of Truth**: SQLite-optimized with clean schema
- **🔧 FastAPI Integration**: REST API ready for LLM agents
- **📦 Minimal Dependencies**: Only essential packages
- **🧪 Fully Tested**: Comprehensive test suite with 100% core coverage

## 🚀 Quick Start

### Installation

```bash
# Install from PyPI
pip install spiralarr

# Or with UV (recommended)
uv add spiralarr

# Initialize database
spiralarr db-init
```

### Your First DAG

Create a simple DAG and run it:

```bash
# Start the scheduler
spiralarr scheduler

# Or start the API server
spiralarr api

# Check health
curl http://localhost:8080/api/v1/health
```

### CLI Commands

```bash
# Database operations
spiralarr db-init

# Start components
spiralarr scheduler
spiralarr api

# Run specific task (used by executor)
spiralarr run-task <dag_id> <task_id> <run_id>
```

## 🏗️ Architecture

### 3-Part Execution System

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│  Scheduler  │───▶│   Executor   │───▶│ Task Runner │
│             │    │              │    │             │
│ Finds tasks │    │ Manages pool │    │ Executes &  │
│ Queues them │    │ Launches     │    │ Updates DB  │
└─────────────┘    └──────────────┘    └─────────────┘
```

### Key Design Principles

- **Database as Single Source of Truth**: All state persisted in DB
- **Per-Task Transactions**: Immediate worker visibility
- **Zombie Protection**: Automatic recovery from failed processes
- **Decoupled Components**: Clean separation of concerns

## 📊 Current Status

| Component | Status | Description |
|-----------|--------|-------------|
| **Models** | ✅ Complete | DAG, DagRun, TaskInstance |
| **Operators** | ✅ Complete | Bash, Python operators |
| **Scheduler** | ✅ Complete | Main scheduling loop |
| **Executor** | ✅ Complete | Local multiprocessing |
| **Task Runner** | ✅ Complete | Task execution component |
| **CLI** | ✅ Complete | All essential commands |
| **API** | ✅ Basic | Health checks, ready for expansion |
| **Tests** | ✅ Complete | Unit + integration tests |



### Project Structure

```
spiralarr/
├── src/spiralarr/          # Main package
│   ├── models/                # Database models
│   ├── operators/             # Task operators
│   ├── scheduler/             # Scheduling logic
│   ├── executors/             # Task execution
│   ├── task_runner/           # Task runner component
│   ├── api/                   # FastAPI application
│   ├── cli/                   # CLI commands
│   ├── config/                # Configuration
│   └── utils/                 # Utilities
├── tests/                     # Test suite
├── dags/                      # Example DAGs
└── docs/                      # Documentation
```

## 🤝 Contributing

See our [Contributing Guide](docs/development/contributing.md) for development setup and guidelines.

## 📄 License

This project is licensed under the Apache License 2.0.
