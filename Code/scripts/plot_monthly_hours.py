#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly Distribution of Piped Water Availability Across Neighborhoods
---------------------------------------------------------------------

Reads the consolidated hours file (all_hours.csv) and renders one boxplot per
month of neighborhood-level hours_with_water, colored by a global
mean +/- 1 std classification of the monthly median (low / normal / high),
with the median trend line and the 720-hour (max) reference line.

Usage
-----
  python plot_monthly_hours.py --input data/consolidated/all_hours.csv
  python plot_monthly_hours.py --input all_hours.csv --output figs/monthly_hours.html
  python plot_monthly_hours.py --input all_hours.csv --png figs/monthly_hours.png

Notes
-----
- YearMonth: if the input has no "YearMonth" column, it is derived from the
  "year" and "month" columns as YYYY_MM.
- HTML output always works. PNG export additionally requires the `kaleido`
  package (pip install kaleido); if it is missing, the PNG step is skipped
  with a warning and the HTML is still written.
"""

import argparse
import logging
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# --------------------------------------------------
# Look & feel: built-in defaults, overridable via --style plot_style.yaml.
# Any key omitted from the YAML falls back to these values.
# --------------------------------------------------
DEFAULT_STYLE = {
    # Which column of the input to plot. Everything else adapts to it.
    "value_column": "hours_with_water",
    "title": {
        "text": "a) Monthly Distribution of Piped Water Hours Availability Across Neighborhoods",
        "x": 0.5,
        "size": 18,
        "color": "black",
    },
    "figure": {
        "width": 1000,
        "height": 600,
        "plot_bgcolor": "white",
        "paper_bgcolor": "white",
        "margin": {"l": 90, "r": 40, "t": 80, "b": 160},
    },
    "class_colors": {
        "low": "#FF1744",
        "normal": "#FFDB6D",
        "high": "#00E5FF",
    },
    "legend": {
        "title": "Monthly Condition",
        "labels": {
            "low": "Low (Below Mean)",
            "normal": "Typical (Mean)",
            "high": "High (Above Mean)",
        },
        # Legend entry explaining the whiskers; shown when whiskers == minmax.
        "range_label": "Neighborhood range (min\u2013max)",
        "font_color": "black",
        "bgcolor": "rgba(0,0,0,0)",
    },
    "boxes": {"opacity": 0.7, "line_color": "black", "line_width": 1, "whiskers": "minmax"},
    "jitter": {"enabled": True, "size": 1.5, "opacity": 0.15},
    "trend_line": {"show": True, "name": "Median Trend", "color": "black", "width": 1},
    "reference_line": {
        "show": True, "value": 720, "name": "720 Hours (Max)",
        "color": "gray", "width": 2, "dash": "dash",
    },
    "xaxis": {
        "title": "Year\u2013Month", "tickangle": -90, "tick_size": 10,
        "tick_color": "black", "linecolor": "black",
    },
    "yaxis": {
        "title": "Monthly Hours of Water", "title_size": 14,
        "dtick": 50,          # set null in YAML for automatic ticks
        "range": None,         # e.g. [0, 30] for days; None -> [0, auto]
        "tick_color": "black", "gridcolor": "rgba(0,0,0,0.1)",
        "zerolinecolor": "rgba(0,0,0,0.2)",
    },
}


def _merge_style(base: dict, override: dict) -> dict:
    """One-level-deep merge: nested dicts merge key-by-key, scalars replace."""
    out = {}
    for k, v in base.items():
        if isinstance(v, dict) and isinstance(override.get(k), dict):
            out[k] = {**v, **override[k]}
            # second level (legend.labels, figure.margin)
            for kk, vv in v.items():
                if isinstance(vv, dict) and isinstance(override[k].get(kk), dict):
                    out[k][kk] = {**vv, **override[k][kk]}
        elif k in override:
            out[k] = override[k]
        else:
            out[k] = v
    return out


def load_style(style_path: str | None) -> dict:
    """Load look-&-feel YAML merged over DEFAULT_STYLE."""
    if not style_path:
        return DEFAULT_STYLE
    import yaml
    with open(style_path, "r", encoding="utf-8") as f:
        user = yaml.safe_load(f) or {}
    return _merge_style(DEFAULT_STYLE, user)


# --------------------------------------------------
# Data preparation
# --------------------------------------------------
def load_data(input_file: Path, value_column: str) -> pd.DataFrame:
    df = pd.read_csv(input_file, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()

    if value_column not in df.columns:
        raise ValueError(
            f"'{value_column}' column not found in {input_file}. "
            f"Columns: {list(df.columns)}"
        )

    # Derive YearMonth (YYYY_MM) from year/month when not already present.
    if "YearMonth" not in df.columns:
        if not {"year", "month"}.issubset(df.columns):
            raise ValueError(
                "Input needs either a 'YearMonth' column or 'year' + 'month' "
                f"columns; found: {list(df.columns)}"
            )
        y = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        m = pd.to_numeric(df["month"], errors="coerce").astype("Int64")
        df["YearMonth"] = (
            y.astype(str) + "_" + m.astype(str).str.zfill(2)
        )

    df[value_column] = pd.to_numeric(df[value_column], errors="coerce")
    df = df.dropna(subset=[value_column, "YearMonth"])

    df["year_month_dt"] = pd.to_datetime(df["YearMonth"], format="%Y_%m", errors="coerce")
    df = df.dropna(subset=["year_month_dt"]).sort_values("year_month_dt")
    return df


def monthly_median_trend(df_plot: pd.DataFrame, style: dict) -> pd.DataFrame:
    vc = style["value_column"]
    df_trend = (
        df_plot
        .groupby("YearMonth", as_index=False)[vc]
        .median()
    )
    df_trend["year_month_dt"] = pd.to_datetime(df_trend["YearMonth"], format="%Y_%m")
    df_trend = df_trend.sort_values("year_month_dt")

    # Global classification: monthly median vs overall mean +/- 1 std.
    overall_mean = df_trend[vc].mean()
    overall_std = df_trend[vc].std()

    def classify_month(row):
        if row[vc] < overall_mean - overall_std:
            return "low"
        elif row[vc] > overall_mean + overall_std:
            return "high"
        return "normal"

    df_trend["trend_class"] = df_trend.apply(classify_month, axis=1)
    df_trend["trend_color"] = df_trend["trend_class"].map(style["class_colors"])
    return df_trend


# --------------------------------------------------
# Figure
# --------------------------------------------------
def build_figure(df_plot: pd.DataFrame, df_trend: pd.DataFrame, style: dict) -> go.Figure:
    vc = style["value_column"]
    month_order = df_plot["YearMonth"].unique()

    df_plot = df_plot.merge(
        df_trend[["YearMonth", "trend_color"]],
        on="YearMonth",
        how="left",
    )

    fig = go.Figure()

    # --- Colored boxplots per month ---
    # whiskers mode:
    #   "minmax": whiskers span the true minimum and maximum of the month
    #             (precomputed stats), so a caption may state "whiskers
    #             indicate minimum and maximum values" and be literally true.
    #   "tukey":  plotly default (1.5 x IQR fences).
    whisker_mode = style["boxes"].get("whiskers", "minmax")
    for ym in month_order:
        df_m = df_plot[df_plot["YearMonth"] == ym]
        if df_m.empty:
            continue
        color = df_m["trend_color"].iloc[0]
        common = dict(
            boxpoints=False,
            fillcolor=color,
            line=dict(color=style["boxes"]["line_color"],
                      width=style["boxes"]["line_width"]),
            opacity=style["boxes"]["opacity"],
            showlegend=False,
        )
        if whisker_mode == "minmax":
            v = df_m[vc]
            fig.add_trace(
                go.Box(
                    x=[ym],
                    lowerfence=[float(v.min())],
                    q1=[float(v.quantile(0.25))],
                    median=[float(v.median())],
                    q3=[float(v.quantile(0.75))],
                    upperfence=[float(v.max())],
                    **common,
                )
            )
        else:
            fig.add_trace(
                go.Box(x=[ym] * len(df_m), y=df_m[vc], **common)
            )

    # --- Median trend line ---
    tl = style["trend_line"]
    if tl.get("show", True):
        fig.add_trace(
            go.Scatter(
                x=df_trend["YearMonth"],
                y=df_trend[vc],
                mode="lines",
                line=dict(color=tl["color"], width=tl["width"]),
                name=tl["name"],
            )
        )

    # --- Reference line (e.g. 720 h = 24 h x 30 days ceiling) ---
    rl = style["reference_line"]
    if rl.get("show", True):
        fig.add_trace(
            go.Scatter(
                x=df_trend["YearMonth"],
                y=[rl["value"]] * len(df_trend),
                mode="lines",
                line=dict(color=rl["color"], width=rl["width"], dash=rl["dash"]),
                name=rl["name"],
            )
        )

    # --- Custom legend (color meaning). Swatches pull from class_colors so the
    # legend can never drift out of sync with the box colors. ---
    for cls in ("low", "normal", "high"):
        fig.add_trace(
            go.Scatter(
                x=[None], y=[None],
                mode="markers",
                marker=dict(size=10, color=style["class_colors"][cls]),
                name=style["legend"]["labels"][cls],
            )
        )

    # --- Legend entry for the whiskers (min-max range), only when whiskers are
    # true min/max so the label is literally accurate. ---
    if whisker_mode == "minmax" and style["legend"].get("range_label"):
        fig.add_trace(
            go.Scatter(
                x=[None], y=[None],
                mode="lines",
                line=dict(color=style["boxes"]["line_color"],
                          width=style["boxes"]["line_width"]),
                name=style["legend"]["range_label"],
            )
        )

    # --- Jittered points overlay (same color as each month's box) ---
    jt = style["jitter"]
    if jt.get("enabled", True):
        for ym in month_order:
            df_m = df_plot[df_plot["YearMonth"] == ym]
            if df_m.empty:
                continue
            color = df_m["trend_color"].iloc[0]
            fig.add_trace(
                go.Scatter(
                    x=[ym] * len(df_m),
                    y=df_m[vc],
                    mode="markers",
                    marker=dict(size=jt["size"], color=color, opacity=jt["opacity"]),
                    showlegend=False,
                )
            )

    # --- Layout (all values from style config) ---
    ti, fg, xa, ya, lg = (style["title"], style["figure"],
                          style["xaxis"], style["yaxis"], style["legend"])
    fig.update_layout(
        title=dict(
            text=ti["text"],
            x=ti.get("x", 0.5),
            font=dict(size=ti["size"], color=ti["color"]),
        ),
        width=fg["width"],
        height=fg["height"],
        plot_bgcolor=fg["plot_bgcolor"],
        paper_bgcolor=fg["paper_bgcolor"],
        xaxis=dict(
            title=xa["title"],
            type="category",
            categoryorder="array",
            categoryarray=month_order,
            tickangle=xa["tickangle"],
            tickfont=dict(color=xa["tick_color"], size=xa["tick_size"]),
            showgrid=False,
            linecolor=xa["linecolor"],
        ),
        yaxis=dict(
            title=dict(text=ya["title"],
                       font=dict(color=ya["tick_color"], size=ya["title_size"])),
            tickfont=dict(color=ya["tick_color"]),
            showgrid=True,
            gridcolor=ya["gridcolor"],
            zerolinecolor=ya["zerolinecolor"],
            range=ya.get("range") or [0, None],
            **(
                dict(tickmode="linear", tick0=0, dtick=ya["dtick"])
                if ya.get("dtick") else {}
            ),
        ),
        legend=dict(
            title=lg["title"],
            font=dict(color=lg["font_color"]),
            bgcolor=lg["bgcolor"],
        ),
        margin=dict(**fg["margin"]),
    )
    return fig


# --------------------------------------------------
# CLI
# --------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="b) Monthly boxplots of neighborhood hours with water."
    )
    p.add_argument("--input", required=True,
                   help="Consolidated hours CSV (e.g. data/consolidated/all_hours.csv).")
    p.add_argument("--output", default="",
                   help="Output HTML file (default: <input_dir>/monthly_hours.html).")
    p.add_argument("--png", default="",
                   help="Optional PNG output path (requires the kaleido package).")
    p.add_argument("--style", default="",
                   help="Optional look-&-feel YAML (see plot_style.yaml). "
                        "Omitted keys fall back to built-in defaults.")
    p.add_argument("--show", action="store_true",
                   help="Also open the figure in a browser (fig.show()).")
    return p.parse_args()


def main():
    args = parse_args()
    input_file = Path(args.input)

    style = load_style(args.style or None)
    if args.style:
        logging.info(f"Loaded style overrides from {args.style}")

    df_plot = load_data(input_file, style["value_column"])
    logging.info(f"Loaded {len(df_plot)} rows of {style['value_column']}, "
                 f"{df_plot['YearMonth'].nunique()} months from {input_file}")

    df_trend = monthly_median_trend(df_plot, style)
    fig = build_figure(df_plot, df_trend, style)

    output_html = Path(args.output) if args.output else input_file.parent / "monthly_hours.html"
    output_html.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(output_html)
    logging.info(f"Wrote {output_html}")

    if args.png:
        png_path = Path(args.png)
        png_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fig.write_image(png_path, scale=2)
            logging.info(f"Wrote {png_path}")
        except Exception as exc:
            logging.warning(
                f"PNG export skipped ({type(exc).__name__}: {exc}). "
                f"Install kaleido for static export: pip install kaleido"
            )

    if args.show:
        fig.show()


if __name__ == "__main__":
    main()
