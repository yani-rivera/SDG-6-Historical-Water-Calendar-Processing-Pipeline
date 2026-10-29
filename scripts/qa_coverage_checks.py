"""Coverage checks: each day of a month counted once, and only once (qa_coverage).

Status values are ';'-joined flags; 'ok' / 'complete' only when no flag applies.
"""


def _status(flags_per_row, clean):
    return [";".join(f) if f else clean for f in flags_per_row]


def calendar_days(df, c, periods_df):
    """Per calendar: days in its period vs days observed (min/max across rows)."""
    g = (df.groupby("source_calendar")[c["days_observed"]]
           .agg(days_observed_min="min", days_observed_max="max", rows="size")
           .reset_index())
    out = periods_df.merge(g, on="source_calendar", how="left")
    flags = []
    for r in out.itertuples(index=False):
        f = []
        if r.days_observed_max < r.days_in_period:
            f.append("shorter_than_period")
        elif r.days_observed_max > r.days_in_period:
            f.append("longer_than_period")
        if r.days_observed_min != r.days_observed_max:
            f.append("varies_within_calendar")
        flags.append(f)
    out["status"] = _status(flags, "ok")
    return out


def month_combinations(cal):
    """Per month: calendars present, days covered (observed), gap or overlap.

    Valid combinations: a whole-month calendar alone, or a + b. Coverage uses the
    days OBSERVED in each calendar (not the nominal period), so a whole-month file
    holding only half a month is reported as partial.
    """
    g = (cal.groupby("year_month")
            .agg(calendars=("source_calendar", lambda x: ";".join(sorted(x))),
                 periods=("period", lambda x: ";".join(sorted(x))),
                 days_covered=("days_observed_max", "sum"),
                 days_in_month=("days_in_month", "first"))
            .reset_index())
    g["coverage"] = (g["days_covered"] / g["days_in_month"]).round(3)
    flags = []
    for r in g.itertuples(index=False):
        f = []
        if r.periods in ("QTR_1", "QTR_2"):
            f.append("single_half")
        elif r.periods not in ("MONTH", "QTR_1;QTR_2"):
            f.append("invalid_combination")
        if r.days_covered < r.days_in_month:
            f.append("partial")
        elif r.days_covered > r.days_in_month:
            f.append("overlap")
        flags.append(f)
    g["status"] = _status(flags, "ok")
    return g


def entry_coverage(df, c, periods_df):
    """Per wc_uid x group x month: days observed added across calendars vs month length.

    Within a calendar, simultaneous rows share days_observed, so the max is taken.
    """
    keys = [c["wc"], c["group"]]
    per_cal = (df.groupby(keys + ["source_calendar"], dropna=False)[c["days_observed"]]
                 .max().reset_index()
                 .merge(periods_df[["source_calendar", "year_month", "period", "days_in_month"]],
                        on="source_calendar"))
    g = (per_cal.groupby(keys + ["year_month"], dropna=False)
                .agg(days_observed_total=(c["days_observed"], "sum"),
                     days_in_month=("days_in_month", "first"),
                     periods=("period", lambda x: ";".join(sorted(x))),
                     calendars=("source_calendar", lambda x: ";".join(sorted(x))))
                .reset_index())
    g["status"] = "complete"
    g.loc[g["days_observed_total"] < g["days_in_month"], "status"] = "partial"
    g.loc[g["days_observed_total"] > g["days_in_month"], "status"] = "double_counted"
    return g


def entry_partial_reasons(ent, mon):
    """Split partial entries: month itself partial vs entry missing from one half."""
    p = ent[ent["status"] == "partial"].merge(
        mon[["year_month", "status"]].rename(columns={"status": "month_status"}),
        on="year_month", how="left")
    in_partial_month = p["month_status"].str.contains("partial", na=False)
    return {"in_partial_month": int(in_partial_month.sum()),
            "in_complete_month": int((~in_partial_month).sum())}


def source_totals(df, c, periods_df):
    """Per calendar: rows, entries, total days with water, total hours."""
    t = (df.groupby("source_calendar")
           .agg(rows=(c["hours"], "size"),
                entries=(c["wc"], "nunique"),
                days_with_water_total=(c["days_with_water"], "sum"),
                hours_total=(c["hours"], "sum"))
           .reset_index())
    return periods_df[["year_month", "source_calendar", "period"]].merge(t, on="source_calendar")
