"""Checks of the day header row (parse_calendar_excel).

pandas renames a repeated header: two columns "26" become "26" and "26.1".
A column like "26.1" is not all digits, so it would be dropped silently with
its X marks. These checks report repeated, missing and unexpected day labels.
"""
import calendar
import re


def day_header_issues(columns, year, month, period):
    """Return a list of messages about the day header (empty list = ok)."""
    cols = [str(c).strip() for c in columns]
    days = sorted(int(c) for c in cols if c.isdigit())
    repeated = [c for c in cols if re.fullmatch(r"\d+\.\d+", c)]
    issues = []
    if repeated:
        issues.append(f"repeated day label(s) {repeated} (pandas suffix): those columns are "
                      f"NOT read; fix the header in the input copy")
    if days:
        end = calendar.monthrange(year, month)[1]
        lo, hi = {"QTR_1": (1, 15), "QTR_2": (16, end)}.get(period, (1, end))
        missing = [d for d in range(lo, hi + 1) if d not in days]
        extra = [d for d in days if d < lo or d > hi]
        if missing:
            issues.append(f"day(s) {missing} missing for {period} ({lo}-{hi})")
        if extra:
            issues.append(f"day(s) {extra} outside {period} ({lo}-{hi}): shared or shifted "
                          f"boundary day")
    return issues
