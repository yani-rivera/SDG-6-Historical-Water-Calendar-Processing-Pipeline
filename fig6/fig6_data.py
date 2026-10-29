"""Data loading and summary statistics for Figure 6 (SDG-6 water calendars)."""
import calendar

import pandas as pd


def load_entries(path, cols, first_year, last_year):
    """Level 1: calendar entries exactly as written by the pipeline.

    Nothing is filtered or altered here; data fixes belong in the pipeline.
    Rows outside the expected period are only reported, not dropped.
    """
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    ym = cols.get("nb_yearmonth", "year_month")
    if cols["year"] not in df.columns and ym in df.columns:
        # aggregated files carry year_month (e.g. 2025-03) instead of year + month
        d = pd.to_datetime(df[ym].astype(str), format=cols.get("nb_yearmonth_format", "%Y-%m"))
        df[cols["year"]], df[cols["month"]] = d.dt.year, d.dt.month
    for c in (cols["year"], cols["month"], cols["hours"], cols["days"]):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    out_of_period = df[~df[cols["year"]].between(first_year, last_year)]
    df["date"] = pd.to_datetime(dict(year=df[cols["year"]], month=df[cols["month"]], day=1))
    return df, out_of_period


def load_neighborhoods(path, cols):
    """Neighborhood x month summaries, read as written by the pipeline.

    Reports (does not resolve) ids that appear more than once in a month,
    since panel (c) expects one value per neighborhood and month.
    """
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    df["date"] = pd.to_datetime(df[cols["nb_yearmonth"]].astype(str),
                                format=cols.get("nb_yearmonth_format", "%Y-%m"))
    df[cols["nb_hours"]] = pd.to_numeric(df[cols["nb_hours"]], errors="coerce")
    dup = df.duplicated(subset=[cols["nb_id"], "date"], keep=False)
    return df, df[dup]


def collapse_groups(df, id_col, value):
    """One value per neighborhood and month: mean across its distribution groups.

    Same rule as Methods (simultaneous schedules -> mean). Figure summary only;
    the input file is not changed.
    """
    return (df.dropna(subset=[value])
              .groupby([id_col, "date"], as_index=False)[value].mean())


def monthly_quantiles(df, value):
    """Per-month min, quartiles, median and max of a variable."""
    g = df.dropna(subset=[value]).groupby("date")[value]
    q = g.quantile([0.0, 0.25, 0.5, 0.75, 1.0]).unstack()
    q.columns = ["min", "q1", "median", "q3", "max"]
    q["n"] = g.size()
    # Reindex to every month so missing months (e.g. Oct 2022) break the lines
    q = q.reindex(pd.date_range(q.index.min(), q.index.max(), freq="MS"))
    q["days_in_month"] = [calendar.monthrange(d.year, d.month)[1] for d in q.index]
    return q


def month_coverage(df, column):
    """Coverage of each month by its calendars: the highest coverage of any row.

    Rows of a month share the calendars, so the max is the calendar coverage
    (a neighborhood present in only one half has lower coverage itself).
    """
    if not column or column not in df.columns:
        return None
    return pd.to_numeric(df[column], errors="coerce").groupby(df["date"]).max()


def classify_months(q, k_sd, coverage=None, min_coverage=None):
    """Label each month low/typical/high by its median against mean +- k*SD of medians.

    Months with coverage below min_coverage are labelled 'partial' and are left
    out of the mean and SD, so a half-covered month cannot set the thresholds.
    """
    q = q.copy()
    q["coverage"] = coverage.reindex(q.index) if coverage is not None else 1.0
    partial = (q["coverage"] < min_coverage) if min_coverage is not None else q["coverage"] < -1
    full = q.loc[~partial, "median"]
    mu, sd = full.mean(), full.std()                 # NaN months are skipped
    lo, hi = mu - k_sd * sd, mu + k_sd * sd
    q["availability"] = "typical"
    q.loc[q["median"] < lo, "availability"] = "low"
    q.loc[q["median"] > hi, "availability"] = "high"
    q.loc[partial, "availability"] = "partial"
    q.loc[q["median"].isna(), "availability"] = "missing"
    return q, {"mean_of_medians": mu, "sd_of_medians": sd, "low_below": lo, "high_above": hi,
               "min_coverage": min_coverage,
               "partial_months": [f"{d:%Y-%m}" for d in q.index[partial & q["median"].notna()]]}


def dispersion(df, value, statistic):
    """Spread between neighborhoods per month (IQR or coefficient of variation)."""
    g = df.dropna(subset=[value]).groupby("date")[value]
    if statistic == "cv":
        out = (g.std() / g.mean()).rename("spread")
    else:
        out = (g.quantile(0.75) - g.quantile(0.25)).rename("spread")
    return out.reindex(pd.date_range(out.index.min(), out.index.max(), freq="MS"))


def pick_examples(df, id_col, value, ids, n_auto):
    """Configured SDG11UIDs, or the lowest and highest by median over the record."""
    if ids:
        return list(ids)
    med = df.groupby(id_col)[value].median().dropna().sort_values()
    # require reasonably complete records
    counts = df.groupby(id_col)[value].count()
    med = med[counts.reindex(med.index) >= counts.quantile(0.75)]
    return list(med.index[:n_auto // 2 or 1]) + list(med.index[-(n_auto - n_auto // 2):])


def coverage_shares(df, column, min_coverage):
    """Per month: number of entries and the share that are complete / partial.

    complete: entry coverage >= min_coverage (its calendars cover the month);
    partial: below. Months without any entry are kept as NaN (no calendar).
    """
    if not column or column not in df.columns:
        return None
    cov = pd.to_numeric(df[column], errors="coerce")
    g = pd.DataFrame({"date": df["date"], "complete": cov >= min_coverage,
                      "partial": cov < min_coverage}).groupby("date")
    out = pd.DataFrame({"n": g.size(), "complete": g["complete"].mean(),
                        "partial": g["partial"].mean()})
    out["n_partial"] = g["partial"].sum()
    return out.reindex(pd.date_range(out.index.min(), out.index.max(), freq="MS"))
