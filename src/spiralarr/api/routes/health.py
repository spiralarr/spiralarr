"""Health check endpoints."""

from fastapi import APIRouter
from sqlalchemy import text

from spiralarr.models.base import get_session

router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Test database connection
        session = get_session()
        session.execute(text("SELECT 1"))
        session.close()

        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}
