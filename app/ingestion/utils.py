from datetime import date


class ParseError(ValueError):
    pass


_DATE_FORMATS = ["%Y-%m-%d", "%Y%m%d"]


def parse_date(value: str) -> date:
    """Accept both YYYY-MM-DD and YYYYMMDD."""
    value = value.strip().strip('"').strip("'")
    for fmt in _DATE_FORMATS:
        try:
            from datetime import datetime
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ParseError(f"Cannot parse date: {value!r}. Expected YYYY-MM-DD or YYYYMMDD.")
