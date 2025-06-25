"""Date and time utilities."""

import datetime as dt
from typing import Optional


def utcnow() -> dt.datetime:
    """Get current UTC datetime."""
    return dt.datetime.utcnow()


def parse_execution_date(date_str: str) -> dt.datetime:
    """Parse execution date from string."""
    try:
        return dt.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        # Fallback to simple format
        return dt.datetime.strptime(date_str, "%Y-%m-%d")


def format_datetime(dt_obj: Optional[dt.datetime]) -> Optional[str]:
    """Format datetime for display."""
    if dt_obj is None:
        return None
    return dt_obj.isoformat()
