"""
Advanced log search and filtering capabilities for semantic logs.

This module provides sophisticated search functionality that allows LLM agents
to find specific information within logs with context and intelligent filtering.
"""

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Dict, List, Optional, Tuple

from spiralarr.models.task_instance import TaskInstance
from spiralarr.utils.log_parser import LogEntry, LogParser, LogSection, log_parser


@dataclass
class SearchResult:
    """Represents a search result with context."""

    entry: LogEntry
    context_before: List[LogEntry]
    context_after: List[LogEntry]
    match_positions: List[Tuple[int, int]]  # (start, end) positions of matches

    def get_highlighted_message(
        self, highlight_start: str = "**", highlight_end: str = "**"
    ) -> str:
        """Get the message with search matches highlighted."""
        message = self.entry.message

        # Sort match positions in reverse order to avoid offset issues
        sorted_matches = sorted(self.match_positions, key=lambda x: x[0], reverse=True)

        for start, end in sorted_matches:
            message = (
                message[:start]
                + highlight_start
                + message[start:end]
                + highlight_end
                + message[end:]
            )

        return message


@dataclass
class SearchFilter:
    """Configuration for log search filtering."""

    sections: Optional[List[str]] = None  # Sections to search in
    time_range: Optional[Tuple[datetime, datetime]] = None  # Time range filter
    log_levels: Optional[List[str]] = None  # Log levels to include
    exclude_patterns: Optional[List[str]] = None  # Patterns to exclude
    case_sensitive: bool = False
    regex_mode: bool = False
    context_lines: int = 2  # Lines of context around matches
    max_results: Optional[int] = None


class LogSearchEngine:
    """
    Advanced search engine for semantic logs with intelligent filtering.

    This engine provides powerful search capabilities that go beyond simple
    text matching to understand the semantic structure of logs.
    """

    def __init__(self, parser: LogParser = None):
        """Initialize with optional custom parser."""
        self.parser = parser or log_parser

    def search(
        self,
        task_instance: TaskInstance,
        query: str,
        search_filter: SearchFilter = None,
        try_number: int = 1,
    ) -> List[SearchResult]:
        """
        Search logs with advanced filtering and context.

        Args:
            task_instance: TaskInstance to search logs for
            query: Search query (text or regex)
            search_filter: Search configuration
            try_number: Attempt number

        Returns:
            List of SearchResult objects with context
        """
        if search_filter is None:
            search_filter = SearchFilter()

        # Get log sections
        sections = self.parser.query_task_logs(
            task_instance, try_number, search_filter.sections
        )

        # Compile search pattern
        flags = 0 if search_filter.case_sensitive else re.IGNORECASE
        if search_filter.regex_mode:
            try:
                pattern = re.compile(query, flags)
            except re.error:
                # Fall back to literal search if regex is invalid
                pattern = re.compile(re.escape(query), flags)
        else:
            pattern = re.compile(re.escape(query), flags)

        # Compile exclude patterns
        exclude_patterns = []
        if search_filter.exclude_patterns:
            for exclude_pattern in search_filter.exclude_patterns:
                try:
                    exclude_patterns.append(re.compile(exclude_pattern, flags))
                except re.error:
                    continue

        results = []

        # Search each section
        for section_name, section in sections.items():
            if not section.entries:
                continue

            section_results = self._search_section(
                section, pattern, exclude_patterns, search_filter
            )
            results.extend(section_results)

        # Sort results by timestamp
        results.sort(key=lambda r: r.entry.timestamp)

        # Apply max results limit
        if search_filter.max_results:
            results = results[: search_filter.max_results]

        return results

    def _search_section(
        self,
        section: LogSection,
        pattern: re.Pattern,
        exclude_patterns: List[re.Pattern],
        search_filter: SearchFilter,
    ) -> List[SearchResult]:
        """Search within a specific log section."""
        results = []
        entries = section.entries

        for i, entry in enumerate(entries):
            # Apply time range filter
            if search_filter.time_range:
                start_time, end_time = search_filter.time_range
                if not (start_time <= entry.timestamp <= end_time):
                    continue

            # Apply log level filter
            if search_filter.log_levels and entry.level not in search_filter.log_levels:
                continue

            # Check for exclude patterns
            if any(
                exclude_pattern.search(entry.message)
                for exclude_pattern in exclude_patterns
            ):
                continue

            # Search for matches
            matches = list(pattern.finditer(entry.message))
            if matches:
                # Get context
                context_before = entries[max(0, i - search_filter.context_lines) : i]
                context_after = entries[i + 1 : i + 1 + search_filter.context_lines]

                # Extract match positions
                match_positions = [(match.start(), match.end()) for match in matches]

                result = SearchResult(
                    entry=entry,
                    context_before=context_before,
                    context_after=context_after,
                    match_positions=match_positions,
                )
                results.append(result)

        return results

    def search_metrics(
        self,
        task_instance: TaskInstance,
        metric_name: Optional[str] = None,
        value_range: Optional[Tuple[float, float]] = None,
        try_number: int = 1,
    ) -> List[Dict]:
        """
        Search for specific metrics with value filtering.

        Args:
            task_instance: TaskInstance to search
            metric_name: Name of metric to search for (supports regex)
            value_range: (min, max) range for metric values
            try_number: Attempt number

        Returns:
            List of matching metric dictionaries
        """
        metrics = self.parser.get_metrics(task_instance, try_number)

        filtered_metrics = []
        for metric in metrics:
            # Filter by metric name
            if metric_name:
                name_pattern = re.compile(metric_name, re.IGNORECASE)
                if not name_pattern.search(metric.get("name", "")):
                    continue

            # Filter by value range
            if value_range:
                try:
                    value = float(metric.get("value", 0))
                    min_val, max_val = value_range
                    if not (min_val <= value <= max_val):
                        continue
                except (ValueError, TypeError):
                    continue

            filtered_metrics.append(metric)

        return filtered_metrics

    def search_outputs(
        self,
        task_instance: TaskInstance,
        output_name: Optional[str] = None,
        file_extension: Optional[str] = None,
        try_number: int = 1,
    ) -> List[Dict]:
        """
        Search for specific outputs with filtering.

        Args:
            task_instance: TaskInstance to search
            output_name: Name of output to search for (supports regex)
            file_extension: File extension filter (e.g., 'csv', 'json')
            try_number: Attempt number

        Returns:
            List of matching output dictionaries
        """
        outputs = self.parser.get_outputs(task_instance, try_number)

        filtered_outputs = []
        for output in outputs:
            # Filter by output name
            if output_name:
                name_pattern = re.compile(output_name, re.IGNORECASE)
                if not name_pattern.search(output.get("name", "")):
                    continue

            # Filter by file extension
            if file_extension:
                value = output.get("value", "")
                if not value.lower().endswith(f".{file_extension.lower()}"):
                    continue

            filtered_outputs.append(output)

        return filtered_outputs

    def find_error_patterns(
        self, task_instance: TaskInstance, try_number: int = 1
    ) -> Dict[str, List[SearchResult]]:
        """
        Find common error patterns in logs.

        Args:
            task_instance: TaskInstance to analyze
            try_number: Attempt number

        Returns:
            Dictionary mapping error types to search results
        """
        error_patterns = {
            "exceptions": r"(Exception|Error):\s*(.+)",
            "file_not_found": r"(No such file|FileNotFoundError|file not found)",
            "permission_denied": r"(Permission denied|PermissionError)",
            "connection_errors": r"(Connection|ConnectionError|timeout|refused)",
            "memory_errors": r"(MemoryError|OutOfMemoryError|out of memory)",
            "syntax_errors": r"(SyntaxError|syntax error|invalid syntax)",
        }

        results = {}
        for error_type, pattern in error_patterns.items():
            search_filter = SearchFilter(
                sections=["stderr", "traceback"],
                regex_mode=True,
                case_sensitive=False,
                context_lines=3,
            )

            matches = self.search(task_instance, pattern, search_filter, try_number)
            if matches:
                results[error_type] = matches

        return results


# Global search engine instance
log_search_engine = LogSearchEngine()


def search_task_logs(
    task_instance: TaskInstance,
    query: str,
    sections: Optional[List[str]] = None,
    case_sensitive: bool = False,
    regex_mode: bool = False,
    context_lines: int = 2,
    max_results: Optional[int] = None,
    try_number: int = 1,
) -> List[Dict]:
    """
    Convenience function for searching task logs.

    Args:
        task_instance: TaskInstance to search
        query: Search query
        sections: Sections to search in
        case_sensitive: Whether search is case sensitive
        regex_mode: Whether to use regex matching
        context_lines: Lines of context around matches
        max_results: Maximum number of results
        try_number: Attempt number

    Returns:
        List of search result dictionaries
    """
    search_filter = SearchFilter(
        sections=sections,
        case_sensitive=case_sensitive,
        regex_mode=regex_mode,
        context_lines=context_lines,
        max_results=max_results,
    )

    results = log_search_engine.search(task_instance, query, search_filter, try_number)

    # Convert to LLM-friendly format
    formatted_results = []
    for result in results:
        formatted_result = {
            "timestamp": result.entry.timestamp.isoformat(),
            "level": result.entry.level,
            "section": result.entry.prefix or "other",
            "message": result.entry.message,
            "highlighted_message": result.get_highlighted_message(),
            "context_before": [entry.message for entry in result.context_before],
            "context_after": [entry.message for entry in result.context_after],
        }
        formatted_results.append(formatted_result)

    return formatted_results
