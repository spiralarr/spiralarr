"""Operators for spiralarr."""

from spiralarr.operators.base import BaseOperator
from spiralarr.operators.bash import BashOperator
from spiralarr.operators.python import PythonOperator

__all__ = ["BaseOperator", "BashOperator", "PythonOperator"]
