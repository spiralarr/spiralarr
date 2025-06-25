"""
API endpoints for smart log parsing and querying.

These endpoints expose Phase 3 log parser functionality for LLM consumption,
providing intelligent log analysis without requiring agents to parse raw log files.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload

from spiralarr.models.base import get_session
from spiralarr.models.task_instance import TaskInstance
from spiralarr.utils.log_aggregation import get_log_aggregator
from spiralarr.utils.log_parser import (
    extract_task_data_flow,
    extract_task_errors,
    extract_task_metrics,
    query_task_log,
)
from spiralarr.utils.log_search import log_search_engine, search_task_logs

router = APIRouter(prefix="/logs", tags=["logs"])


# Pydantic schemas for API responses
class LogSectionResponse(BaseModel):
    """Response model for log section data."""

    count: int
    messages: List[str]
    content: str

    class Config:
        from_attributes = True


class LogQueryResponse(BaseModel):
    """Response model for log queries."""

    task_instance_id: int
    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    sections: dict  # Dynamic sections based on query

    class Config:
        from_attributes = True


class SearchResultResponse(BaseModel):
    """Response model for log search results."""

    timestamp: str
    level: str
    section: str
    message: str
    highlighted_message: str
    context_before: List[str]
    context_after: List[str]

    class Config:
        from_attributes = True


class ErrorContextResponse(BaseModel):
    """Response model for error context."""

    has_errors: bool
    stderr_messages: List[str]
    tracebacks: List[str]
    error_count: int
    last_error_time: Optional[str]

    class Config:
        from_attributes = True


class MetricsResponse(BaseModel):
    """Response model for performance metrics."""

    metrics: List[dict]
    checkpoints: List[str]
    metric_count: int
    checkpoint_count: int
    duration_metrics: List[dict]
    size_metrics: List[dict]
    count_metrics: List[dict]

    class Config:
        from_attributes = True


@router.get("/task/{task_instance_id}/query", response_model=LogQueryResponse)
def query_task_logs(
    task_instance_id: int,
    section: Optional[str] = Query(
        None, description="Specific section to query (stdout, stderr, metric, etc.)"
    ),
    search: Optional[str] = Query(None, description="Search pattern within logs"),
    try_number: int = Query(1, description="Task attempt number"),
    limit: Optional[int] = Query(None, description="Maximum number of entries"),
    session: Session = Depends(get_session),
):
    """
    Query logs for a specific task instance with semantic section filtering.

    This endpoint provides structured access to task logs without requiring
    LLM agents to parse raw log files.
    """
    # Get task instance with related data
    task_instance = (
        session.query(TaskInstance)
        .options(joinedload(TaskInstance.dag_run))
        .filter(TaskInstance.id == task_instance_id)
        .first()
    )

    if not task_instance:
        raise HTTPException(status_code=404, detail="Task instance not found")

    # Query logs using the smart parser
    sections = query_task_log(task_instance, section, search, try_number, limit)

    return LogQueryResponse(
        task_instance_id=task_instance_id,
        dag_id=task_instance.dag_run.dag_id,
        task_id=task_instance.task_id,
        run_id=task_instance.dag_run.run_id,
        try_number=try_number,
        sections=sections,
    )


@router.get(
    "/task/{task_instance_id}/search", response_model=List[SearchResultResponse]
)
def search_logs(
    task_instance_id: int,
    query: str = Query(..., description="Search query (text or regex)"),
    sections: Optional[str] = Query(
        None, description="Comma-separated list of sections to search"
    ),
    case_sensitive: bool = Query(False, description="Case sensitive search"),
    regex_mode: bool = Query(False, description="Use regex matching"),
    context_lines: int = Query(2, description="Lines of context around matches"),
    max_results: Optional[int] = Query(50, description="Maximum number of results"),
    try_number: int = Query(1, description="Task attempt number"),
    session: Session = Depends(get_session),
):
    """
    Search within task logs with advanced filtering and context.

    Provides powerful search capabilities that understand the semantic
    structure of logs and return results with context.
    """
    # Get task instance
    task_instance = (
        session.query(TaskInstance).filter(TaskInstance.id == task_instance_id).first()
    )

    if not task_instance:
        raise HTTPException(status_code=404, detail="Task instance not found")

    # Parse sections parameter
    sections_list = None
    if sections:
        sections_list = [s.strip() for s in sections.split(",")]

    # Perform search
    results = search_task_logs(
        task_instance=task_instance,
        query=query,
        sections=sections_list,
        case_sensitive=case_sensitive,
        regex_mode=regex_mode,
        context_lines=context_lines,
        max_results=max_results,
        try_number=try_number,
    )

    return [SearchResultResponse(**result) for result in results]


@router.get("/task/{task_instance_id}/errors", response_model=ErrorContextResponse)
def get_error_context(
    task_instance_id: int,
    try_number: int = Query(1, description="Task attempt number"),
    session: Session = Depends(get_session),
):
    """
    Extract error context including stderr, tracebacks, and surrounding logs.

    Perfect for LLM agents to quickly understand what went wrong in a failed task.
    """
    # Get task instance
    task_instance = (
        session.query(TaskInstance).filter(TaskInstance.id == task_instance_id).first()
    )

    if not task_instance:
        raise HTTPException(status_code=404, detail="Task instance not found")

    # Extract error context
    error_context = extract_task_errors(task_instance, try_number)

    return ErrorContextResponse(**error_context)


@router.get("/task/{task_instance_id}/metrics", response_model=MetricsResponse)
def get_performance_metrics(
    task_instance_id: int,
    try_number: int = Query(1, description="Task attempt number"),
    session: Session = Depends(get_session),
):
    """
    Extract performance metrics and timing information from task logs.

    Provides structured access to all metrics logged during task execution.
    """
    # Get task instance
    task_instance = (
        session.query(TaskInstance).filter(TaskInstance.id == task_instance_id).first()
    )

    if not task_instance:
        raise HTTPException(status_code=404, detail="Task instance not found")

    # Extract performance metrics
    metrics = extract_task_metrics(task_instance, try_number)

    return MetricsResponse(**metrics)


@router.get("/task/{task_instance_id}/data-flow", response_model=dict)
def get_data_flow(
    task_instance_id: int,
    try_number: int = Query(1, description="Task attempt number"),
    session: Session = Depends(get_session),
):
    """
    Extract data flow information including inputs, outputs, and processing steps.

    Helps LLM agents understand what data was processed and what outputs were generated.
    """
    # Get task instance
    task_instance = (
        session.query(TaskInstance).filter(TaskInstance.id == task_instance_id).first()
    )

    if not task_instance:
        raise HTTPException(status_code=404, detail="Task instance not found")

    # Extract data flow information
    data_flow = extract_task_data_flow(task_instance, try_number)

    return data_flow


@router.get("/task/{task_instance_id}/user-output", response_model=dict)
def get_user_output(
    task_instance_id: int,
    try_number: int = Query(1, description="Task attempt number"),
    session: Session = Depends(get_session),
):
    """
    Get user-generated output (stdout/stderr) separate from system logs.

    Provides clean access to what the user's code actually printed,
    without system logging noise.
    """
    # Get task instance
    task_instance = (
        session.query(TaskInstance).filter(TaskInstance.id == task_instance_id).first()
    )

    if not task_instance:
        raise HTTPException(status_code=404, detail="Task instance not found")

    # Get user output
    from spiralarr.utils.log_parser import log_parser

    user_output = log_parser.get_user_output(task_instance, try_number)

    return {
        "task_instance_id": task_instance_id,
        "try_number": try_number,
        "stdout": user_output["stdout"],
        "stderr": user_output["stderr"],
        "stdout_lines": len(user_output["stdout"].splitlines())
        if user_output["stdout"]
        else 0,
        "stderr_lines": len(user_output["stderr"].splitlines())
        if user_output["stderr"]
        else 0,
    }


@router.get("/task/{task_instance_id}/error-patterns", response_model=dict)
def find_error_patterns(
    task_instance_id: int,
    try_number: int = Query(1, description="Task attempt number"),
    session: Session = Depends(get_session),
):
    """
    Find common error patterns in task logs.

    Automatically detects and categorizes different types of errors
    to help LLM agents quickly identify the root cause of failures.
    """
    # Get task instance
    task_instance = (
        session.query(TaskInstance).filter(TaskInstance.id == task_instance_id).first()
    )

    if not task_instance:
        raise HTTPException(status_code=404, detail="Task instance not found")

    # Find error patterns
    error_patterns = log_search_engine.find_error_patterns(task_instance, try_number)

    # Convert to API-friendly format
    formatted_patterns = {}
    for error_type, results in error_patterns.items():
        formatted_patterns[error_type] = [
            {
                "timestamp": result.entry.timestamp.isoformat(),
                "message": result.entry.message,
                "highlighted_message": result.get_highlighted_message(),
                "context_before": [entry.message for entry in result.context_before],
                "context_after": [entry.message for entry in result.context_after],
            }
            for result in results
        ]

    return {
        "task_instance_id": task_instance_id,
        "try_number": try_number,
        "error_patterns": formatted_patterns,
        "total_error_types": len(formatted_patterns),
        "total_errors": sum(len(results) for results in formatted_patterns.values()),
    }


@router.get("/task/{dag_id}/{task_id}/aggregate", response_model=dict)
def aggregate_task_performance(
    dag_id: str,
    task_id: str,
    days_back: int = Query(30, description="Number of days to look back"),
    limit: Optional[int] = Query(None, description="Maximum number of runs to analyze"),
    session: Session = Depends(get_session),
):
    """
    Aggregate performance metrics for a task across multiple runs.

    Provides trend analysis, performance statistics, and pattern identification
    across multiple executions of the same task.
    """
    # Set up aggregator with session
    aggregator = get_log_aggregator()
    aggregator.session = session

    # Get aggregated performance data
    summary = aggregator.aggregate_task_metrics(dag_id, task_id, days_back, limit)

    # Convert to API-friendly format
    return {
        "dag_id": dag_id,
        "task_id": summary.task_id,
        "analysis_period_days": days_back,
        "total_runs": summary.total_runs,
        "successful_runs": summary.successful_runs,
        "failed_runs": summary.failed_runs,
        "success_rate": summary.success_rate,
        "duration_stats": {
            "avg_duration": summary.avg_duration,
            "min_duration": summary.min_duration,
            "max_duration": summary.max_duration,
        },
        "metrics": {
            name: {
                "count": agg.count,
                "min": agg.min_value,
                "max": agg.max_value,
                "mean": agg.mean_value,
                "median": agg.median_value,
                "std_dev": agg.std_deviation,
                "trend": agg.get_trend(),
            }
            for name, agg in summary.metric_aggregations.items()
        },
        "common_outputs": summary.common_outputs,
        "error_patterns": summary.error_patterns,
    }


@router.get("/dag/{dag_id}/aggregate", response_model=dict)
def aggregate_dag_performance(
    dag_id: str,
    days_back: int = Query(30, description="Number of days to look back"),
    limit: Optional[int] = Query(None, description="Maximum number of runs per task"),
    session: Session = Depends(get_session),
):
    """
    Aggregate performance data for all tasks in a DAG.

    Provides comprehensive analysis of DAG performance including
    task-level breakdowns and overall DAG health metrics.
    """
    # Set up aggregator with session
    aggregator = get_log_aggregator()
    aggregator.session = session

    # Get aggregated data for all tasks
    task_summaries = aggregator.aggregate_dag_performance(dag_id, days_back, limit)

    # Calculate DAG-level statistics
    total_runs = sum(summary.total_runs for summary in task_summaries.values())
    total_successful = sum(
        summary.successful_runs for summary in task_summaries.values()
    )
    overall_success_rate = (
        (total_successful / total_runs * 100) if total_runs > 0 else 0
    )

    # Format response
    return {
        "dag_id": dag_id,
        "analysis_period_days": days_back,
        "overall_stats": {
            "total_task_runs": total_runs,
            "successful_runs": total_successful,
            "failed_runs": total_runs - total_successful,
            "success_rate": overall_success_rate,
            "unique_tasks": len(task_summaries),
        },
        "task_summaries": {
            task_id: {
                "total_runs": summary.total_runs,
                "success_rate": summary.success_rate,
                "avg_duration": summary.avg_duration,
                "metric_count": len(summary.metric_aggregations),
                "common_outputs": summary.common_outputs,
                "error_patterns": summary.error_patterns,
            }
            for task_id, summary in task_summaries.items()
        },
    }


@router.get("/system/health-trends", response_model=dict)
def get_system_health_trends(
    days_back: int = Query(7, description="Number of days to analyze"),
    session: Session = Depends(get_session),
):
    """
    Get system-wide health trends over time.

    Provides high-level insights into overall system performance,
    success rates, and trends across all DAGs and tasks.
    """
    # Set up aggregator with session
    aggregator = get_log_aggregator()
    aggregator.session = session

    # Get system health trends
    health_data = aggregator.get_system_health_trends(days_back)

    return {"analysis_period_days": days_back, "system_health": health_data}
