"""SQLAlchemy 2.0 modern async declarative base and database utilities."""

from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Modern SQLAlchemy 2.0 declarative base."""

    pass


# Global async engine and session factory (will be configured at runtime)
async_engine = None
AsyncSessionLocal = None


def validate_async_db_url(url: str) -> str:
    """Auto-convert sync URLs to async equivalents."""
    if url.startswith("sqlite://"):
        return url.replace("sqlite://", "sqlite+aiosqlite://")
    elif url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://")
    return url


async def init_async_db(database_url: str = "sqlite:///spiralarr.db"):
    """Initialize async database connection."""
    global async_engine, AsyncSessionLocal

    # Convert to async URL if needed
    async_url = validate_async_db_url(database_url)

    # Configure engine based on database type
    if "sqlite" in async_url:
        # SQLite configuration
        async_engine = create_async_engine(
            async_url,
            echo=False,  # Set to True for SQL debugging
            connect_args={"check_same_thread": False},
        )
    else:
        # PostgreSQL configuration
        async_engine = create_async_engine(
            async_url,
            echo=False,  # Set to True for SQL debugging
            pool_size=20,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,  # Recycle connections every hour
        )

    AsyncSessionLocal = async_sessionmaker(
        async_engine,
        expire_on_commit=False,  # CRITICAL: Prevents detached instance errors
    )

    return async_engine


async def create_tables():
    """Create all tables in the database."""
    if async_engine is None:
        raise RuntimeError("Database not initialized. Call init_async_db() first.")

    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Get an async database session."""
    if AsyncSessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_async_db() first.")

    async with AsyncSessionLocal() as session:
        yield session


async def close_async_db():
    """Cleanup database connections."""
    if async_engine is not None:
        await async_engine.dispose()


# Backward compatibility for existing components
def init_db(database_url: str = "sqlite:///spiralarr.db"):
    """
    Backward compatibility wrapper for sync database initialization.

    This function exists to support existing components while the codebase
    transitions to async patterns. New code should use init_async_db().
    """
    import asyncio

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If we're already in an async context, we can't use asyncio.run()
            # This is a limitation for backward compatibility
            raise RuntimeError(
                "Cannot use sync init_db() from within async context. "
                "Use init_async_db() instead."
            )
    except RuntimeError:
        pass

    return asyncio.run(init_async_db(database_url))


def create_tables_sync():
    """
    Backward compatibility wrapper for sync table creation.

    This function exists to support existing tests while the codebase
    transitions to async patterns. New code should use await create_tables().
    """
    import asyncio

    async def _create_tables():
        await create_tables()

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            raise RuntimeError(
                "Cannot use sync create_tables() from within async context. "
                "Use await create_tables() instead."
            )
    except RuntimeError:
        pass

    return asyncio.run(_create_tables())


# Alias for backward compatibility
create_tables_legacy = create_tables_sync


def get_session():
    """
    Backward compatibility wrapper for sync session access.

    This creates a sync session for backward compatibility with existing components.
    This is a temporary solution during the migration to async patterns.

    New code should use 'async for session in get_async_session()' instead.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    if async_engine is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")

    # Extract the sync URL from async engine
    async_url = str(async_engine.url)
    if "aiosqlite" in async_url:
        sync_url = async_url.replace("sqlite+aiosqlite://", "sqlite://")
    elif "asyncpg" in async_url:
        sync_url = async_url.replace("postgresql+asyncpg://", "postgresql://")
    else:
        sync_url = async_url

    # Create a temporary sync engine for backward compatibility
    if "sqlite" in sync_url:
        sync_engine = create_engine(
            sync_url,
            echo=False,
            connect_args={"check_same_thread": False},
        )
    else:
        sync_engine = create_engine(sync_url, echo=False)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine)
    return SessionLocal()
