"""
Smart log parser for semantic log analysis and querying.

This module implements Phase 3 of the logging strategy - intelligent log parsing
that allows LLM agents to query specific semantic sections without processing
entire log files.
"""

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Dict, List, Optional, Union

from spiralarr.models.task_instance import TaskInstance
from spiralarr.utils.log_template import get_task_log_path


@dataclass
class LogEntry:
    """Represents a single log entry with semantic information."""

    timestamp: datetime
    level: str
    logger: str
    prefix: Optional[str]
    message: str
    raw_line: str


@dataclass
class LogSection:
    """Represents a semantic section of logs."""

    section_type: str
    entries: List[LogEntry]

    def get_messages(self) -> List[str]:
        """Get just the messages from this section."""
        return [entry.message for entry in self.entries]

    def get_content(self) -> str:
        """Get all messages joined as a single string."""
        return "\n".join(self.get_messages())

    def search(self, pattern: str, case_sensitive: bool = False) -> List[LogEntry]:
        """Search for a pattern within this section."""
        flags = 0 if case_sensitive else re.IGNORECASE
        regex = re.compile(pattern, flags)

        return [entry for entry in self.entries if regex.search(entry.message)]


class LogParser:
    """
    Smart parser for semantic logs with LLM-optimized querying capabilities.

    This parser can extract specific semantic sections from log files and
    provide structured access to different types of log content.
    """

    # Semantic prefixes to parse
    SEMANTIC_PREFIXES = {
        "STDOUT": "stdout",
        "STDERR": "stderr",
        "METRIC": "metric",
        "OUTPUT": "output",
        "CHECKPOINT": "checkpoint",
        "TRACEBACK": "traceback",
        "SYSTEM": "system",
    }

    def __init__(self):
        """Initialize the log parser."""
        self.log_pattern = re.compile(
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - "  # timestamp
            r"([^-]+) - "  # logger name
            r"(\w+) - "  # log level
            r"(.+)"  # message
        )
        self.prefix_pattern = re.compile(r"\[([A-Z]+)\]\s*(.*)")

    def parse_log_file(self, log_file_path: Union[str, Path]) -> Dict[str, LogSection]:
        """
        Parse a log file and extract semantic sections.

        Args:
            log_file_path: Path to the log file

        Returns:
            Dictionary mapping section types to LogSection objects
        """
        log_path = Path(log_file_path)
        if not log_path.exists():
            return {}

        sections = {
            section_type: LogSection(section_type, [])
            for section_type in self.SEMANTIC_PREFIXES.values()
        }
        sections["other"] = LogSection("other", [])

        try:
            with open(log_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    entry = self._parse_log_line(line)
                    if entry:
                        section_type = self._get_section_type(entry.prefix)
                        sections[section_type].entries.append(entry)

        except Exception as e:
            # If parsing fails, return empty sections
            print(f"Error parsing log file {log_path}: {e}")

        return sections

    def _parse_log_line(self, line: str) -> Optional[LogEntry]:
        """Parse a single log line into a LogEntry."""
        match = self.log_pattern.match(line)
        if not match:
            return None

        timestamp_str, logger, level, message = match.groups()

        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
        except ValueError:
            timestamp = datetime.now()

        # Extract semantic prefix if present
        prefix = None
        prefix_match = self.prefix_pattern.match(message)
        if prefix_match:
            prefix, message = prefix_match.groups()

        return LogEntry(
            timestamp=timestamp,
            level=level,
            logger=logger.strip(),
            prefix=prefix,
            message=message,
            raw_line=line,
        )

    def _get_section_type(self, prefix: Optional[str]) -> str:
        """Map a semantic prefix to a section type."""
        if prefix and prefix in self.SEMANTIC_PREFIXES:
            return self.SEMANTIC_PREFIXES[prefix]
        return "other"

    def query_task_logs(
        self,
        task_instance: TaskInstance,
        try_number: int = 1,
        sections: Optional[List[str]] = None,
        search_pattern: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, LogSection]:
        """
        Query logs for a specific task instance.

        Args:
            task_instance: TaskInstance to query logs for
            try_number: Attempt number
            sections: List of section types to include (None = all)
            search_pattern: Optional regex pattern to search for
            limit: Maximum number of entries per section

        Returns:
            Dictionary of filtered LogSection objects
        """
        log_path = get_task_log_path(task_instance, try_number)
        all_sections = self.parse_log_file(log_path)

        # Filter sections if specified
        if sections:
            filtered_sections = {
                section_type: all_sections.get(
                    section_type, LogSection(section_type, [])
                )
                for section_type in sections
            }
        else:
            filtered_sections = all_sections

        # Apply search pattern if specified
        if search_pattern:
            for section in filtered_sections.values():
                section.entries = section.search(search_pattern)

        # Apply limit if specified
        if limit:
            for section in filtered_sections.values():
                section.entries = section.entries[:limit]

        return filtered_sections

    def get_metrics(
        self, task_instance: TaskInstance, try_number: int = 1
    ) -> List[Dict]:
        """
        Extract all metrics from task logs.

        Args:
            task_instance: TaskInstance to get metrics for
            try_number: Attempt number

        Returns:
            List of metric dictionaries
        """
        sections = self.query_task_logs(task_instance, try_number, ["metric"])
        metric_section = sections.get("metric")

        if not metric_section:
            return []

        metrics = []
        for entry in metric_section.entries:
            try:
                # Parse JSON metric data
                metric_data = json.loads(entry.message)
                metric_data["timestamp"] = entry.timestamp.isoformat()
                metrics.append(metric_data)
            except (json.JSONDecodeError, KeyError):
                # Skip invalid metric entries
                continue

        return metrics

    def get_outputs(
        self, task_instance: TaskInstance, try_number: int = 1
    ) -> List[Dict]:
        """
        Extract all outputs from task logs.

        Args:
            task_instance: TaskInstance to get outputs for
            try_number: Attempt number

        Returns:
            List of output dictionaries
        """
        sections = self.query_task_logs(task_instance, try_number, ["output"])
        output_section = sections.get("output")

        if not output_section:
            return []

        outputs = []
        for entry in output_section.entries:
            try:
                # Parse JSON output data
                output_data = json.loads(entry.message)
                output_data["timestamp"] = entry.timestamp.isoformat()
                outputs.append(output_data)
            except (json.JSONDecodeError, KeyError):
                # Skip invalid output entries
                continue

        return outputs

    def get_checkpoints(
        self, task_instance: TaskInstance, try_number: int = 1
    ) -> List[str]:
        """
        Get all checkpoint messages for task progress tracking.

        Args:
            task_instance: TaskInstance to get checkpoints for
            try_number: Attempt number

        Returns:
            List of checkpoint messages
        """
        sections = self.query_task_logs(task_instance, try_number, ["checkpoint"])
        checkpoint_section = sections.get("checkpoint")

        if not checkpoint_section:
            return []

        return checkpoint_section.get_messages()

    def get_user_output(
        self, task_instance: TaskInstance, try_number: int = 1
    ) -> Dict[str, str]:
        """
        Get user-generated output (stdout/stderr) separate from system logs.

        Args:
            task_instance: TaskInstance to get user output for
            try_number: Attempt number

        Returns:
            Dictionary with 'stdout' and 'stderr' keys
        """
        sections = self.query_task_logs(task_instance, try_number, ["stdout", "stderr"])

        return {
            "stdout": sections.get("stdout", LogSection("stdout", [])).get_content(),
            "stderr": sections.get("stderr", LogSection("stderr", [])).get_content(),
        }


# Global parser instance
log_parser = LogParser()


class LogExtractor:
    """
    Utility class for extracting specific semantic sections from logs.

    This class provides high-level methods for common log extraction patterns
    that LLM agents frequently need.
    """

    def __init__(self, parser: LogParser = None):
        """Initialize with optional custom parser."""
        self.parser = parser or log_parser

    def extract_error_context(
        self, task_instance: TaskInstance, try_number: int = 1, context_lines: int = 3
    ) -> Dict:
        """
        Extract error context including stderr, tracebacks, and surrounding logs.

        Args:
            task_instance: TaskInstance to extract errors from
            try_number: Attempt number
            context_lines: Number of context lines around errors

        Returns:
            Dictionary with error information and context
        """
        sections = self.parser.query_task_logs(
            task_instance, try_number, ["stderr", "traceback", "system"]
        )

        error_info = {
            "has_errors": False,
            "stderr_messages": [],
            "tracebacks": [],
            "error_count": 0,
            "last_error_time": None,
        }

        # Extract stderr messages
        stderr_section = sections.get("stderr")
        if stderr_section and stderr_section.entries:
            error_info["has_errors"] = True
            error_info["stderr_messages"] = stderr_section.get_messages()
            error_info["error_count"] += len(stderr_section.entries)
            error_info["last_error_time"] = stderr_section.entries[
                -1
            ].timestamp.isoformat()

        # Extract tracebacks
        traceback_section = sections.get("traceback")
        if traceback_section and traceback_section.entries:
            error_info["has_errors"] = True
            error_info["tracebacks"] = traceback_section.get_messages()
            error_info["error_count"] += len(traceback_section.entries)
            if not error_info["last_error_time"]:
                error_info["last_error_time"] = traceback_section.entries[
                    -1
                ].timestamp.isoformat()

        return error_info

    def extract_performance_metrics(
        self, task_instance: TaskInstance, try_number: int = 1
    ) -> Dict:
        """
        Extract performance-related metrics and timing information.

        Args:
            task_instance: TaskInstance to extract metrics from
            try_number: Attempt number

        Returns:
            Dictionary with performance metrics
        """
        metrics = self.parser.get_metrics(task_instance, try_number)
        checkpoints = self.parser.get_checkpoints(task_instance, try_number)

        performance_info = {
            "metrics": metrics,
            "checkpoints": checkpoints,
            "metric_count": len(metrics),
            "checkpoint_count": len(checkpoints),
            "duration_metrics": [],
            "size_metrics": [],
            "count_metrics": [],
        }

        # Categorize metrics by type
        for metric in metrics:
            name = metric.get("name", "").lower()
            if "duration" in name or "time" in name or name.endswith("_seconds"):
                performance_info["duration_metrics"].append(metric)
            elif "size" in name or "bytes" in name or "mb" in name or "gb" in name:
                performance_info["size_metrics"].append(metric)
            elif "count" in name or "rows" in name or "records" in name:
                performance_info["count_metrics"].append(metric)

        return performance_info

    def extract_data_flow(
        self, task_instance: TaskInstance, try_number: int = 1
    ) -> Dict:
        """
        Extract data flow information including inputs, outputs, and processing steps.

        Args:
            task_instance: TaskInstance to extract data flow from
            try_number: Attempt number

        Returns:
            Dictionary with data flow information
        """
        outputs = self.parser.get_outputs(task_instance, try_number)
        user_output = self.parser.get_user_output(task_instance, try_number)

        data_flow = {
            "outputs": outputs,
            "output_count": len(outputs),
            "stdout_lines": len(user_output["stdout"].splitlines())
            if user_output["stdout"]
            else 0,
            "stderr_lines": len(user_output["stderr"].splitlines())
            if user_output["stderr"]
            else 0,
            "file_outputs": [],
            "data_outputs": [],
        }

        # Categorize outputs
        for output in outputs:
            value = output.get("value", "")

            if any(
                ext in value.lower()
                for ext in [".csv", ".json", ".txt", ".log", ".xml"]
            ):
                data_flow["file_outputs"].append(output)
            else:
                data_flow["data_outputs"].append(output)

        return data_flow


# Global extractor instance
log_extractor = LogExtractor()


def query_task_log(
    task_instance: TaskInstance,
    section: Optional[str] = None,
    search: Optional[str] = None,
    try_number: int = 1,
    limit: Optional[int] = None,
) -> Dict:
    """
    Convenience function to query task logs with LLM-friendly output.

    Args:
        task_instance: TaskInstance to query
        section: Specific section to query ('stdout', 'stderr', 'metric', etc.)
        search: Search pattern within the section
        try_number: Attempt number
        limit: Maximum number of entries

    Returns:
        Dictionary with structured log data
    """
    sections_to_query = [section] if section else None
    sections = log_parser.query_task_logs(
        task_instance, try_number, sections_to_query, search, limit
    )

    # Convert to LLM-friendly format
    result = {}
    for section_type, section_obj in sections.items():
        if section_obj.entries:  # Only include non-empty sections
            result[section_type] = {
                "count": len(section_obj.entries),
                "messages": section_obj.get_messages(),
                "content": section_obj.get_content(),
            }

    return result


# Convenience functions for common extraction patterns
def extract_task_errors(task_instance: TaskInstance, try_number: int = 1) -> Dict:
    """Extract error context for a task."""
    return log_extractor.extract_error_context(task_instance, try_number)


def extract_task_metrics(task_instance: TaskInstance, try_number: int = 1) -> Dict:
    """Extract performance metrics for a task."""
    return log_extractor.extract_performance_metrics(task_instance, try_number)


def extract_task_data_flow(task_instance: TaskInstance, try_number: int = 1) -> Dict:
    """Extract data flow information for a task."""
    return log_extractor.extract_data_flow(task_instance, try_number)
