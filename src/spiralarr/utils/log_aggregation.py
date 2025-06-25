"""
Log aggregation utilities for analyzing metrics and outputs across multiple task runs.

This module provides aggregation capabilities that allow LLM agents to analyze
trends, patterns, and performance across multiple executions of tasks or DAGs.
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
import statistics
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from spiralarr.models.base import get_session
from spiralarr.models.dag_run import DagRun
from spiralarr.models.task_instance import TaskInstance
from spiralarr.utils.log_parser import log_parser
from spiralarr.utils.state import State


@dataclass
class MetricAggregation:
    """Aggregated statistics for a metric across multiple runs."""

    metric_name: str
    count: int
    min_value: float
    max_value: float
    mean_value: float
    median_value: float
    std_deviation: float
    values: List[float]
    timestamps: List[str]

    def get_trend(self) -> str:
        """Determine if metric is trending up, down, or stable."""
        if len(self.values) < 2:
            return "insufficient_data"

        # Simple trend analysis using first and last quartile
        quarter_size = len(self.values) // 4
        if quarter_size == 0:
            return "insufficient_data"

        first_quarter_avg = statistics.mean(self.values[:quarter_size])
        last_quarter_avg = statistics.mean(self.values[-quarter_size:])

        change_percent = (
            (last_quarter_avg - first_quarter_avg) / first_quarter_avg
        ) * 100

        if change_percent > 10:
            return "increasing"
        elif change_percent < -10:
            return "decreasing"
        else:
            return "stable"


@dataclass
class TaskPerformanceSummary:
    """Performance summary for a task across multiple runs."""

    task_id: str
    total_runs: int
    successful_runs: int
    failed_runs: int
    success_rate: float
    avg_duration: float
    min_duration: float
    max_duration: float
    metric_aggregations: Dict[str, MetricAggregation]
    common_outputs: List[str]
    error_patterns: Dict[str, int]


class LogAggregator:
    """
    Aggregates logs and metrics across multiple task runs for trend analysis.

    This class provides capabilities to analyze performance trends, identify
    patterns, and generate insights across multiple executions.
    """

    def __init__(self, session: Session = None):
        """Initialize with optional database session."""
        self.session = session or get_session()

    def aggregate_task_metrics(
        self,
        dag_id: str,
        task_id: str,
        days_back: int = 30,
        limit: Optional[int] = None,
    ) -> TaskPerformanceSummary:
        """
        Aggregate metrics for a specific task across multiple runs.

        Args:
            dag_id: DAG ID to analyze
            task_id: Task ID to analyze
            days_back: Number of days to look back
            limit: Maximum number of runs to analyze

        Returns:
            TaskPerformanceSummary with aggregated data
        """
        # Get task instances from the last N days
        cutoff_date = datetime.utcnow() - timedelta(days=days_back)

        query = (
            self.session.query(TaskInstance)
            .join(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
                TaskInstance.task_id == task_id,
                TaskInstance.start_date >= cutoff_date,
            )
            .order_by(TaskInstance.start_date.desc())
        )

        if limit:
            query = query.limit(limit)

        task_instances = query.all()

        if not task_instances:
            return TaskPerformanceSummary(
                task_id=task_id,
                total_runs=0,
                successful_runs=0,
                failed_runs=0,
                success_rate=0.0,
                avg_duration=0.0,
                min_duration=0.0,
                max_duration=0.0,
                metric_aggregations={},
                common_outputs=[],
                error_patterns={},
            )

        # Analyze runs
        successful_runs = sum(1 for ti in task_instances if ti.state == State.SUCCESS)
        failed_runs = sum(1 for ti in task_instances if ti.state == State.FAILED)
        success_rate = (successful_runs / len(task_instances)) * 100

        # Calculate duration statistics
        durations = []
        for ti in task_instances:
            if ti.start_date and ti.end_date:
                duration = (ti.end_date - ti.start_date).total_seconds()
                durations.append(duration)

        avg_duration = statistics.mean(durations) if durations else 0.0
        min_duration = min(durations) if durations else 0.0
        max_duration = max(durations) if durations else 0.0

        # Aggregate metrics
        metric_aggregations = self._aggregate_metrics(task_instances)

        # Find common outputs
        common_outputs = self._find_common_outputs(task_instances)

        # Analyze error patterns
        error_patterns = self._analyze_error_patterns(task_instances)

        return TaskPerformanceSummary(
            task_id=task_id,
            total_runs=len(task_instances),
            successful_runs=successful_runs,
            failed_runs=failed_runs,
            success_rate=success_rate,
            avg_duration=avg_duration,
            min_duration=min_duration,
            max_duration=max_duration,
            metric_aggregations=metric_aggregations,
            common_outputs=common_outputs,
            error_patterns=error_patterns,
        )

    def _aggregate_metrics(
        self, task_instances: List[TaskInstance]
    ) -> Dict[str, MetricAggregation]:
        """Aggregate metrics across task instances."""
        metrics_by_name = defaultdict(list)

        # Collect all metrics
        for ti in task_instances:
            try:
                metrics = log_parser.get_metrics(ti, 1)
                for metric in metrics:
                    name = metric.get("name")
                    value = metric.get("value")
                    timestamp = metric.get("timestamp")

                    if name and value is not None:
                        try:
                            float_value = float(value)
                            metrics_by_name[name].append((float_value, timestamp))
                        except (ValueError, TypeError):
                            continue
            except Exception:
                # Skip if log parsing fails
                continue

        # Create aggregations
        aggregations = {}
        for metric_name, values_and_timestamps in metrics_by_name.items():
            if len(values_and_timestamps) < 2:
                continue

            values = [v[0] for v in values_and_timestamps]
            timestamps = [v[1] for v in values_and_timestamps]

            aggregations[metric_name] = MetricAggregation(
                metric_name=metric_name,
                count=len(values),
                min_value=min(values),
                max_value=max(values),
                mean_value=statistics.mean(values),
                median_value=statistics.median(values),
                std_deviation=statistics.stdev(values) if len(values) > 1 else 0.0,
                values=values,
                timestamps=timestamps,
            )

        return aggregations

    def _find_common_outputs(self, task_instances: List[TaskInstance]) -> List[str]:
        """Find commonly generated outputs across runs."""
        output_counts = defaultdict(int)

        for ti in task_instances:
            try:
                outputs = log_parser.get_outputs(ti, 1)
                for output in outputs:
                    name = output.get("name")
                    if name:
                        output_counts[name] += 1
            except Exception:
                continue

        # Return outputs that appear in at least 50% of runs
        min_count = len(task_instances) * 0.5
        common_outputs = [
            name for name, count in output_counts.items() if count >= min_count
        ]

        return sorted(common_outputs)

    def _analyze_error_patterns(
        self, task_instances: List[TaskInstance]
    ) -> Dict[str, int]:
        """Analyze common error patterns across failed runs."""
        error_patterns = defaultdict(int)

        failed_instances = [ti for ti in task_instances if ti.state == State.FAILED]

        for ti in failed_instances:
            try:
                from spiralarr.utils.log_search import log_search_engine

                patterns = log_search_engine.find_error_patterns(ti, 1)

                for error_type, results in patterns.items():
                    if results:
                        error_patterns[error_type] += len(results)
            except Exception:
                continue

        return dict(error_patterns)

    def aggregate_dag_performance(
        self, dag_id: str, days_back: int = 30, limit: Optional[int] = None
    ) -> Dict[str, TaskPerformanceSummary]:
        """
        Aggregate performance data for all tasks in a DAG.

        Args:
            dag_id: DAG ID to analyze
            days_back: Number of days to look back
            limit: Maximum number of runs per task

        Returns:
            Dictionary mapping task IDs to performance summaries
        """
        # Get all unique task IDs for this DAG
        cutoff_date = datetime.utcnow() - timedelta(days=days_back)

        task_ids = (
            self.session.query(TaskInstance.task_id)
            .join(DagRun)
            .filter(DagRun.dag_id == dag_id, TaskInstance.start_date >= cutoff_date)
            .distinct()
            .all()
        )

        # Aggregate each task
        task_summaries = {}
        for (task_id,) in task_ids:
            summary = self.aggregate_task_metrics(dag_id, task_id, days_back, limit)
            task_summaries[task_id] = summary

        return task_summaries

    def get_system_health_trends(self, days_back: int = 7) -> Dict:
        """
        Get system-wide health trends over time.

        Args:
            days_back: Number of days to analyze

        Returns:
            Dictionary with system health metrics
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days_back)

        # Get all task instances in the time period
        task_instances = (
            self.session.query(TaskInstance)
            .filter(TaskInstance.start_date >= cutoff_date)
            .all()
        )

        if not task_instances:
            return {
                "total_tasks": 0,
                "success_rate": 0.0,
                "avg_duration": 0.0,
                "daily_trends": [],
            }

        # Calculate overall metrics
        successful = sum(1 for ti in task_instances if ti.state == State.SUCCESS)
        total = len(task_instances)
        success_rate = (successful / total) * 100

        # Calculate average duration
        durations = []
        for ti in task_instances:
            if ti.start_date and ti.end_date:
                duration = (ti.end_date - ti.start_date).total_seconds()
                durations.append(duration)

        avg_duration = statistics.mean(durations) if durations else 0.0

        # Group by day for trends
        daily_stats = defaultdict(
            lambda: {"total": 0, "successful": 0, "durations": []}
        )

        for ti in task_instances:
            if ti.start_date:
                day = ti.start_date.date().isoformat()
                daily_stats[day]["total"] += 1

                if ti.state == State.SUCCESS:
                    daily_stats[day]["successful"] += 1

                if ti.start_date and ti.end_date:
                    duration = (ti.end_date - ti.start_date).total_seconds()
                    daily_stats[day]["durations"].append(duration)

        # Convert to trend format
        daily_trends = []
        for day, stats in sorted(daily_stats.items()):
            daily_success_rate = (
                (stats["successful"] / stats["total"]) * 100
                if stats["total"] > 0
                else 0
            )
            daily_avg_duration = (
                statistics.mean(stats["durations"]) if stats["durations"] else 0
            )

            daily_trends.append(
                {
                    "date": day,
                    "total_tasks": stats["total"],
                    "successful_tasks": stats["successful"],
                    "success_rate": daily_success_rate,
                    "avg_duration": daily_avg_duration,
                }
            )

        return {
            "total_tasks": total,
            "successful_tasks": successful,
            "success_rate": success_rate,
            "avg_duration": avg_duration,
            "daily_trends": daily_trends,
        }


# Global aggregator instance (lazy initialization)
log_aggregator = None


def get_log_aggregator() -> LogAggregator:
    """Get or create the global log aggregator instance."""
    global log_aggregator
    if log_aggregator is None:
        log_aggregator = LogAggregator()
    return log_aggregator
