# Contributing to Airflow Lite

We're excited you want to contribute! This guide has everything you need to get started.

## 🚀 Quick Start

Get your development environment ready with these commands. You'll need Python 3.8+ and [UV](https://docs.astral.sh/uv/) installed.

```bash
# 1. Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR-USERNAME/spiralarr.git
cd spiralarr

# 2. Install all dependencies into a virtual environment
uv sync --extra dev --extra docs

# 3. Install pre-commit hooks for automated code quality
uv run pre-commit install

# 4. Run the test suite to ensure everything is working
uv run pytest
```

## 🔄 Contribution Workflow

1.  **Create a Branch:** Name your branch with a prefix like `feature/` or `fix/`.
    ```bash
    git checkout -b feature/my-new-feature
    ```

2.  **Write Code & Tests:** Make your changes. Add tests for new features and ensure existing tests pass.

3.  **Commit Changes:** We use [Conventional Commits](https://www.conventionalcommits.org/). Pre-commit hooks will automatically format and lint your code.
    ```bash
    git commit -m "feat: add amazing new feature"
    ```

4.  **Push and Open a Pull Request:** Push your branch to your fork and open a PR against the main repository.

## 🛠️ Development Guide

Here are the common commands you'll use during development. All commands should be run with `uv run`.

### Dependency Management (UV)

-   **Add a dependency:** `uv add <package-name>`
-   **Add a dev dependency:** `uv add --group dev <package-name>`
-   **Sync environment:** `uv sync` (after pulling changes or modifying `pyproject.toml`)

### Code Quality (Ruff & Pre-commit)

We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting. Pre-commit hooks handle this automatically, but you can run them manually:

-   **Check and fix all files:** `uv run ruff check --fix .`
-   **Format all files:** `uv run ruff format .`

> **Important:** We do not allow relative imports. Always use absolute imports from the `spiralarr` root (e.g., `from spiralarr.models import ...`).

### Testing (Pytest)

-   **Run all tests:** `uv run pytest`
-   **Run tests for a specific file:** `uv run pytest tests/models/test_task_instance.py`
-   **Run tests with coverage:** `uv run pytest --cov=spiralarr`

#### Test Organization

Tests are organized to mirror the `src/spiralarr/` structure for easy navigation:

```
tests/
├── cli/
│   └── test_commands.py          # Tests for CLI commands
├── executors/
│   └── test_local.py             # Tests for local executor
├── models/
│   ├── test_base.py              # Tests for base models
│   ├── test_dag.py               # Tests for DAG model
│   ├── test_dag_run.py           # Tests for DagRun model
│   └── test_task_instance.py     # Tests for TaskInstance model
├── operators/
│   ├── test_base.py              # Tests for base operator
│   ├── test_bash.py              # Tests for bash operator
│   └── test_python.py            # Tests for python operator
├── scheduler/
│   └── test_scheduler.py         # Tests for scheduler
├── task_runner/
│   └── test_runner.py            # Tests for task runner
└── utils/
    ├── test_context.py           # Tests for context utilities
    ├── test_dag_loader.py        # Tests for DAG loader
    ├── test_dates.py             # Tests for date utilities
    └── test_state.py             # Tests for state utilities
```

**Guidelines for writing tests:**
- Each source file should have a corresponding test file (e.g., `models/dag.py` → `tests/models/test_dag.py`)
- Test files should be placed in the same relative path under `tests/` as their source counterparts
- Use descriptive test names that explain what is being tested
- Include both positive and negative test cases
- Mock external dependencies when appropriate

### Documentation (MkDocs)

-   **Preview docs locally:** `uv run mkdocs serve`
-   Documentation files are located in the `docs/` directory. Please update them if you change user-facing behavior.

---

Thank you for contributing! 🎉
