# xerparser
# validators.py
# Functions to validate data during object initialization

from datetime import datetime
from typing import Union

date_format = "%Y-%m-%d %H:%M"

def optional_date(value: Union[str, None]) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        return datetime.strptime(value, date_format)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid date format: {value}")

def optional_float(value: Union[str, None]) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid float value: {value}")

def float_or_zero(value: Union[str, None]) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid float value: {value}")

def optional_int(value: Union[str, None]) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid integer value: {value}")

def int_or_zero(value: Union[str, None]) -> int:
    if value is None or value == "":
        return 0
    try:
        return int(value)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid integer value: {value}")

def optional_str(value: Union[str, None]) -> str | None:
    if value is None or value == "":
        return None
    return value