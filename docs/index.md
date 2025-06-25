# Spiralarr 🚀

A minimal workflow orchestrator inspired by Apache Airflow, designed for simplicity, performance, and LLM integration.

## ✨ Features

- **🏗️ 3-Part Execution System**: Clean separation between Scheduler, Executor, and Task Runner
- **⚡ Per-Task Transactions**: Immediate worker visibility with zombie task protection
- **🗄️ Database as Source of Truth**: SQLite-optimized schema with proper relationships
- **🔧 FastAPI Integration**: REST API ready for LLM agents and programmatic control
- **📦 Minimal Dependencies**: Only essential packages, no bloat
- **🧪 Fully Tested**: Comprehensive test suite with unit and integration tests

## 🚀 Quick Start

### Installation

```bash
# Install from PyPI
pip install spiralarr

# Or with UV (recommended)
uv add spiralarr

# Initialize the database
spiralarr db-init
```

### Running Spiralarr

```bash
# Start the scheduler (runs tasks automatically)
spiralarr scheduler

# Or start the API server (for programmatic control)
spiralarr api

# Check health
curl http://localhost:8080/api/v1/health
```

### Example Usage

```python
# The system is designed around database models
from spiralarr.models.dag import DagModel
from spiralarr.models.dag_run import DagRun
from spiralarr.models.task_instance import TaskInstance
from spiralarr.operators.python import PythonOperator
from spiralarr.operators.bash import BashOperator

# Tasks are executed by the 3-part system:
# Scheduler finds tasks → Executor manages processes → Task Runner executes
```

## 🎯 Why Spiralarr?

| Feature | Apache Airflow | Spiralarr |
|---------|----------------|--------------|
| Setup Time | 15+ minutes | < 2 minutes ⚡ |
| Dependencies | 100+ packages | < 10 packages 📦 |
| Memory Usage | 500MB+ | < 50MB 💾 |
| Learning Curve | Steep | Gentle 📈 |
| Database | PostgreSQL required | SQLite included 🗄️ |
| API | Complex REST API | Simple FastAPI 🔌 |
| Testing | Complex setup | Instant with pytest 🧪 |

## 🏗️ Architecture Overview

### 3-Part Execution System

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│  Scheduler  │───▶│   Executor   │───▶│ Task Runner │
│             │    │              │    │             │
│ • Finds     │    │ • Manages    │    │ • Executes  │
│   runnable  │    │   process    │    │   operators │
│   tasks     │    │   pool       │    │ • Updates   │
│ • Queues    │    │ • Launches   │    │   database  │
│   them      │    │   runners    │    │   state     │
└─────────────┘    └──────────────┘    └─────────────┘
```

### Key Components

- **Models**: DAG, DagRun, TaskInstance with clean relationships
- **Operators**: BashOperator, PythonOperator with execution context
- **Scheduler**: Main loop with zombie task protection
- **Executor**: Local multiprocessing with capacity management
- **Task Runner**: Critical component that actually executes tasks
- **API**: FastAPI server for programmatic control
- **CLI**: Essential commands for database and component management

## � Documentation

- [CLI Commands](cli-commands.md) - Complete command reference and usage guide
- [Contributing](development/contributing.md) - Development setup and guidelines

## 🤝 Community

- 🐛 [Report Issues](TBD)
- 💡 [Feature Requests](TBD)
- 📖 [Documentation](TBD)

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](TBD) file for details.

---

Built with ❤️ by the Airflow Lite community
