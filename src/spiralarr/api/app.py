"""FastAPI application for spiralarr."""

from fastapi import FastAPI

from spiralarr.api.routes import health, logs, results

app = FastAPI(
    title="Spiralarr API",
    description="High-performance LLM-native workflow orchestrator API",
    version="0.1.0",
)

# Include route modules
app.include_router(health.router, prefix="/api/v1")
app.include_router(results.router, prefix="/api/v1")
app.include_router(logs.router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Spiralarr API - LLM-Native Workflow Orchestrator",
        "version": "0.1.0",
    }
