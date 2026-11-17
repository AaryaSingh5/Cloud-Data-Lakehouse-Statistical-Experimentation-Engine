"""Value parsing shared by schema checks and validity rules."""
from __future__ import annotations

from datetime import datetime


def parse_value(vtype: str, value, ts_format: str) -> tuple[bool, object]:
    """Return (ok, parsed). Never raises."""
    try:
        if vtype == "string":
            return True, str(value)
        if vtype == "int":
            return True, int(str(value))
        if vtype == "float":
            return True, float(str(value))
        if vtype == "timestamp":
            return True, datetime.strptime(str(value), ts_format)
    except (ValueError, TypeError):
        return False, None
    return False, None