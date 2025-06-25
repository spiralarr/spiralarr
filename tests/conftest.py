"""Pytest configuration and fixtures for spiralarr tests."""

from pathlib import Path
import tempfile

import pytest
import pytest_asyncio

from spiralarr.models.base import (
    close_async_db,
    create_tables,
    get_async_session,
    init_async_db,
)


@pytest_asyncio.fixture
async def async_engine():
    """Create async test engine with in-memory SQLite."""
    engine = await init_async_db("sqlite+aiosqlite:///:memory:")
    await create_tables()
    yield engine
    await close_async_db()


@pytest_asyncio.fixture
async def async_session(async_engine):
    """Create async test session that rolls back after each test."""
    async for session in get_async_session():
        try:
            yield session
        finally:
            # Just close the session, let the context manager handle cleanup
            pass
        break


@pytest.fixture
def temp_db():
    """Create a temporary database for testing (legacy sync support)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    # Initialize the database for backward compatibility
    from spiralarr.models.base import create_tables_sync, init_db

    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    create_tables_sync()

    yield database_url

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def sample_dag_config():
    """Sample DAG configuration for testing."""
    return {
        "dag_id": "test_dag",
        "description": "Test DAG for unit tests",
        "schedule_interval": "0 0 * * *",
        "start_date": "2024-01-01",
        "tasks": [
            {
                "task_id": "test_task_1",
                "operator": "BashOperator",
                "bash_command": "echo 'test'",
            },
            {
                "task_id": "test_task_2",
                "operator": "PythonOperator",
                "python_callable": "test_function",
            },
        ],
    }
