"""API endpoints for TaskInstanceResult - Tier 1 structured metadata.

These endpoints provide LLM-optimized access to task execution outcomes
without requiring log file parsing. Designed for maximum signal-to-noise ratio.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload

from spiralarr.models.base import get_session
from spiralarr.models.dag_run import DagRun
from spiralarr.models.task_instance import TaskInstance
from spiralarr.models.task_instance_result import TaskInstanceResult

router = APIRouter(prefix="/results", tags=["results"])


# Pydantic schemas for API responses
class TaskResultSummary(BaseModel):
    """Compact task result summary for LLM consumption."""

    task_instance_id: int
    dag_id: str
    task_id: str
    run_id: str
    status: str
    duration_seconds: int
    start_time: str
    end_time: str
    return_value: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    exit_code: Optional[int] = None

    class Config:
        from_attributes = True


class TaskFailureDetail(BaseModel):
    """Detailed failure information for debugging."""

    task_instance_id: int
    dag_id: str
    task_id: str
    run_id: str
    error_type: str
    error_message: str
    exit_code: Optional[int] = None
    log_excerpt: Optional[str] = None
    duration_seconds: int
    start_time: str
    end_time: str

    class Config:
        from_attributes = True


class DagRunSummary(BaseModel):
    """Summary of all task results in a DAG run."""

    dag_id: str
    run_id: str
    total_tasks: int
    successful_tasks: int
    failed_tasks: int
    total_duration_seconds: int
    tasks: List[TaskResultSummary]

    class Config:
        from_attributes = True


@router.get("/task/{task_instance_id}", response_model=TaskResultSummary)
def get_task_result(task_instance_id: int, session: Session = Depends(get_session)):
    """
    Get structured result for a specific task instance.

    Returns compact summary optimized for LLM consumption.
    """
    result = (
        session.query(TaskInstanceResult)
        .options(
            joinedload(TaskInstanceResult.task_instance).joinedload(
                TaskInstance.dag_run
            )
        )
        .filter(TaskInstanceResult.task_instance_id == task_instance_id)
        .first()
    )

    if not result:
        raise HTTPException(status_code=404, detail="Task result not found")

    return TaskResultSummary(
        task_instance_id=result.task_instance_id,
        dag_id=result.task_instance.dag_run.dag_id,
        task_id=result.task_instance.task_id,
        run_id=result.task_instance.dag_run.run_id,
        status=result.status,
        duration_seconds=result.duration_seconds,
        start_time=result.start_time.isoformat(),
        end_time=result.end_time.isoformat(),
        return_value=result.return_value,
        error_type=result.error_type,
        error_message=result.error_message,
        exit_code=result.exit_code,
    )


@router.get("/dag/{dag_id}/run/{run_id}", response_model=DagRunSummary)
def get_dag_run_results(
    dag_id: str, run_id: str, session: Session = Depends(get_session)
):
    """
    Get structured results for all tasks in a DAG run.

    Perfect for LLM agents to understand overall DAG execution status
    with a single API call.
    """
    # Get all task results for this DAG run
    results = (
        session.query(TaskInstanceResult)
        .join(TaskInstance, TaskInstanceResult.task_instance_id == TaskInstance.id)
        .join(DagRun, TaskInstance.dag_run_id == DagRun.id)
        .filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
        .options(
            joinedload(TaskInstanceResult.task_instance).joinedload(
                TaskInstance.dag_run
            )
        )
        .all()
    )

    if not results:
        raise HTTPException(status_code=404, detail="No results found for this DAG run")

    # Calculate summary statistics
    total_tasks = len(results)
    successful_tasks = sum(1 for r in results if r.status == "SUCCESS")
    failed_tasks = sum(1 for r in results if r.status == "FAILED")
    total_duration = sum(r.duration_seconds for r in results)

    # Convert to response format
    task_summaries = [
        TaskResultSummary(
            task_instance_id=result.task_instance_id,
            dag_id=dag_id,
            task_id=result.task_instance.task_id,
            run_id=run_id,
            status=result.status,
            duration_seconds=result.duration_seconds,
            start_time=result.start_time.isoformat(),
            end_time=result.end_time.isoformat(),
            return_value=result.return_value,
            error_type=result.error_type,
            error_message=result.error_message,
            exit_code=result.exit_code,
        )
        for result in results
    ]

    return DagRunSummary(
        dag_id=dag_id,
        run_id=run_id,
        total_tasks=total_tasks,
        successful_tasks=successful_tasks,
        failed_tasks=failed_tasks,
        total_duration_seconds=total_duration,
        tasks=task_summaries,
    )


@router.get("/failures", response_model=List[TaskFailureDetail])
def get_recent_failures(
    limit: int = Query(10, ge=1, le=100),
    dag_id: Optional[str] = Query(None),
    session: Session = Depends(get_session),
):
    """
    Get recent task failures with detailed error information.

    Essential for LLM agents to quickly identify and diagnose issues.
    """
    query = (
        session.query(TaskInstanceResult)
        .join(TaskInstance, TaskInstanceResult.task_instance_id == TaskInstance.id)
        .join(DagRun, TaskInstance.dag_run_id == DagRun.id)
        .filter(TaskInstanceResult.status == "FAILED")
        .options(
            joinedload(TaskInstanceResult.task_instance).joinedload(
                TaskInstance.dag_run
            )
        )
        .order_by(TaskInstanceResult.end_time.desc())
    )

    if dag_id:
        query = query.filter(DagRun.dag_id == dag_id)

    results = query.limit(limit).all()

    return [
        TaskFailureDetail(
            task_instance_id=result.task_instance_id,
            dag_id=result.task_instance.dag_run.dag_id,
            task_id=result.task_instance.task_id,
            run_id=result.task_instance.dag_run.run_id,
            error_type=result.error_type,
            error_message=result.error_message,
            exit_code=result.exit_code,
            log_excerpt=result.log_excerpt,
            duration_seconds=result.duration_seconds,
            start_time=result.start_time.isoformat(),
            end_time=result.end_time.isoformat(),
        )
        for result in results
    ]


@router.get("/summary", response_model=dict)
def get_execution_summary(
    hours: int = Query(24, ge=1, le=168),  # Last 24 hours by default, max 1 week
    session: Session = Depends(get_session),
):
    """
    Get high-level execution summary for system health monitoring.

    Perfect for LLM agents to quickly assess system status.
    """
    from datetime import datetime, timedelta

    since = datetime.utcnow() - timedelta(hours=hours)

    # Get all results in time window
    results = (
        session.query(TaskInstanceResult)
        .filter(TaskInstanceResult.end_time >= since)
        .all()
    )

    if not results:
        return {
            "time_window_hours": hours,
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
            "success_rate": 0.0,
            "average_duration_seconds": 0.0,
            "total_execution_time_seconds": 0,
        }

    total_tasks = len(results)
    successful_tasks = sum(1 for r in results if r.status == "SUCCESS")
    failed_tasks = sum(1 for r in results if r.status == "FAILED")
    total_duration = sum(r.duration_seconds for r in results)

    return {
        "time_window_hours": hours,
        "total_tasks": total_tasks,
        "successful_tasks": successful_tasks,
        "failed_tasks": failed_tasks,
        "success_rate": round(successful_tasks / total_tasks * 100, 2)
        if total_tasks > 0
        else 0.0,
        "average_duration_seconds": round(total_duration / total_tasks, 2)
        if total_tasks > 0
        else 0.0,
        "total_execution_time_seconds": total_duration,
    }
