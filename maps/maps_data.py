"""Data loading and month selection for the neighborhood maps (Figures 3-4)."""
import geopandas as gpd
import pandas as pd


def load_summary(path, cols, start, end):
    """Monthly SDG11 summary, read as written by the pipeline."""
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    df[cols["id"]] = df[cols["id"]].astype(str).str.strip()
    df["date"] = pd.to_datetime(df[cols["year_month"]].astype(str),
                                format=cols.get("year_month_format", "%Y-%m"))
    window = df["date"].between(pd.Timestamp(start), pd.Timestamp(end))
    return df[window].copy()


def load_polygons(path, layer, id_col, crs):
    gdf = gpd.read_file(path, layer=layer) if layer else gpd.read_file(path)
    if id_col not in gdf.columns:
        raise SystemExit(f"Column '{id_col}' not found in {path}. Columns: {list(gdf.columns)}")
    gdf[id_col] = gdf[id_col].astype(str).str.strip()
    return gdf.to_crs(crs) if crs else gdf


def city_means(df, variable):
    """City-wide monthly mean: mean over neighborhoods."""
    return df.groupby("date")[variable].mean().rename("city_mean")


def partial_months(df, column, min_coverage):
    """Months whose calendars cover less than min_coverage of the month.

    Coverage of a month = highest coverage of any row (the calendar coverage).
    """
    if not column or min_coverage is None or column not in df.columns:
        return []
    cov = pd.to_numeric(df[column], errors="coerce").groupby(df["date"]).max()
    return sorted(cov.index[cov < min_coverage])


def resolve_panels(df, panels_cfg, selection_variable, fmt, exclude_months=()):
    """Turn panel specs into (type, month or None) using city-wide monthly means.

    exclude_months (e.g. partial months) cannot be selected as lowest/highest.
    """
    cm = city_means(df, selection_variable)
    cm_sel = cm[~cm.index.isin(list(exclude_months))]
    out = []
    for p in panels_cfg:
        t = p["type"]
        if t == "period_mean":
            out.append((t, None))
        elif t == "lowest":
            out.append((t, cm_sel.idxmin()))
        elif t == "highest":
            out.append((t, cm_sel.idxmax()))
        elif t == "month":
            out.append((t, pd.to_datetime(p["month"], format=fmt)))
        else:
            raise SystemExit(f"Unknown panel type: {t!r}")
    return out, cm


def panel_layer(gdf, df, id_col, panel, variable):
    """Polygons with the panel's value (NaN where no schedule).

    period_mean: mean of the variable over all months of the period, per neighborhood.
    """
    kind, date = panel
    if kind == "period_mean":
        vals = df.groupby(id_col, as_index=False)[variable].mean()
    else:
        vals = df[df["date"] == date][[id_col, variable]]
    return gdf.merge(vals, on=id_col, how="left")
