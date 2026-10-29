"""Calendar codes -> period labels and exact start/end dates (qa_coverage).

Default periods come from the suffix (""/a/b). A calendar whose actual days differ
from the default (e.g. 2025_06b starting on day 15) is set in `period_overrides`;
the override and its note are written to calendar_periods.csv.
"""
import calendar
import re
from pathlib import Path

import pandas as pd


def calendar_code(path):
    """'data/2025/calproc_horas/2025_03a.csv' -> '2025_03a' (text only, no file is read)."""
    return Path(str(path).strip()).stem


def _day(value, month_end):
    """'end' -> last day of the month; otherwise an int."""
    return month_end if str(value).strip().lower() == "end" else int(value)


def period_table(codes, pattern, periods, overrides=None):
    """One row per calendar: period label, start/end date, days in period.

    periods:   {suffix: {label, first_day, last_day}}
    overrides: {calendar_code: {first_day, last_day, note}} (optional)
    """
    rx = re.compile(pattern)
    overrides = {str(k): v for k, v in (overrides or {}).items()}
    rows = []
    for code in sorted(set(codes)):
        m = rx.search(code)
        spec = periods.get(m["suffix"] or "") if m else None
        if spec is None:
            rows.append({"source_calendar": code, "parse_ok": False})
            continue
        y, mo = int(m["year"]), int(m["month"])
        end = calendar.monthrange(y, mo)[1]
        ovr = overrides.get(code, {})
        first = _day(ovr.get("first_day", spec["first_day"]), end)
        last = _day(ovr.get("last_day", spec["last_day"]), end)
        rows.append({
            "source_calendar": code, "parse_ok": True,
            "year_month": f"{y}-{mo:02d}", "suffix": m["suffix"] or "", "period": spec["label"],
            "start_date": f"{y}-{mo:02d}-{first:02d}", "end_date": f"{y}-{mo:02d}-{last:02d}",
            "days_in_period": last - first + 1, "days_in_month": end,
            "period_source": "override" if ovr else "default",
            "period_note": str(ovr.get("note", "")),
        })
    return pd.DataFrame(rows)


def unused_overrides(overrides, codes):
    """Override keys that match no calendar in the input (likely a typo)."""
    return sorted(set(map(str, (overrides or {}))) - set(codes))
