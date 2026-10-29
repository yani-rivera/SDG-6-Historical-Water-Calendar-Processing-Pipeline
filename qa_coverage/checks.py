"""Coverage checks: each day of a month counted once, and only once."""
import pandas as pd


def calendar_days(df, c, periods_df):
    """Per calendar: expected days vs observed days (distinct values across rows)."""
    g = (df.groupby("source_calendar")[c["days_observed"]]
           .agg(days_observed_min="min", days_observed_max="max", rows="size")
           .reset_index())
    out = periods_df.merge(g, on="source_calendar", how="left")
    out["status"] = "ok"
    out.loc[out["days_observed_max"] < out["days_in_period"], "status"] = "shorter_than_period"
    out.loc[out["days_observed_max"] > out["days_in_period"], "status"] = "longer_than_period"
    out.loc[out["days_observed_min"] != out["days_observed_max"], "status"] += ";varies_within_calendar"
    return out


def month_combinations(periods_df):
    """Per month: which calendars exist; valid = whole month alone, or a + b."""
    g = (periods_df.groupby("year_month")
           .agg(calendars=("source_calendar", lambda x: ";".join(sorted(x))),
                periods=("period", lambda x: ";".join(sorted(x))),
                days_covered=("days_in_period", "sum"),
                days_in_month=("days_in_month", "first"))
           .reset_index())
    valid = {"MONTH", "QTR_1;QTR_2"}
    g["status"] = "ok"
    g.loc[~g["periods"].isin(valid), "status"] = "invalid_combination"
    g.loc[g["days_covered"] < g["days_in_month"], "status"] += ";gap"
    g.loc[g["days_covered"] > g["days_in_month"], "status"] += ";overlap"
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


def source_totals(df, c, periods_df):
    """Per month and calendar: rows, entries, total days observed/with water, total hours."""
    t = (df.groupby("source_calendar")
           .agg(rows=(c["hours"], "size"),
                entries=(c["wc"], "nunique"),
                days_with_water_total=(c["days_with_water"], "sum"),
                hours_total=(c["hours"], "sum"))
           .reset_index())
    return periods_df[["year_month", "source_calendar", "period"]].merge(t, on="source_calendar")
