"""Calendar codes -> period labels and exact start/end dates."""
import calendar
import re
from pathlib import Path

import pandas as pd


def calendar_code(path):
    """'data/2025/calproc/2025_03a.csv' -> '2025_03a' (text only, no file is read)."""
    return Path(str(path).strip()).stem


def period_table(codes, pattern, periods):
    """One row per calendar: period label, start/end date, days in period."""
    rx = re.compile(pattern)
    rows = []
    for code in sorted(set(codes)):
        m = rx.search(code)
        if not m:
            rows.append({"source_calendar": code, "parse_ok": False})
            continue
        y, mo, suf = int(m["year"]), int(m["month"]), m["suffix"] or ""
        spec = periods.get(suf)
        if spec is None:
            rows.append({"source_calendar": code, "parse_ok": False, "suffix": suf})
            continue
        end = calendar.monthrange(y, mo)[1]
        first = spec["first_day"]
        last = end if spec["last_day"] == "end" else int(spec["last_day"])
        rows.append({
            "source_calendar": code, "parse_ok": True,
            "year_month": f"{y}-{mo:02d}", "suffix": suf, "period": spec["label"],
            "start_date": f"{y}-{mo:02d}-{first:02d}", "end_date": f"{y}-{mo:02d}-{last:02d}",
            "days_in_period": last - first + 1, "days_in_month": end,
        })
    return pd.DataFrame(rows)
