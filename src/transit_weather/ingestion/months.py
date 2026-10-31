"""Helpers for 'YYYY-MM' month strings."""


def month_range(start: str, end: str) -> list[str]:
    """Inclusive list of 'YYYY-MM' months from start to end."""
    year, month = (int(part) for part in start.split("-"))
    end_year, end_month = (int(part) for part in end.split("-"))
    months = []
    while (year, month) <= (end_year, end_month):
        months.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months
