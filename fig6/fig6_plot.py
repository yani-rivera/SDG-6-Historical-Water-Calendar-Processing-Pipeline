"""Plotting functions for Figure 6 and the supplementary example-neighborhood figure."""
from datetime import timedelta

import matplotlib.dates as mdates
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

WIDTH_DAYS = 20  # box width on a monthly time axis


def _month_axis(ax, tick_months=(1, 5, 9), fmt="%Y_%m"):
    """Month labels like the original figures: selected months labelled,
    every month marked with a minor tick."""
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=list(tick_months)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    for lab in ax.get_xticklabels():
        lab.set_rotation(90)
        lab.set_fontsize(7)


def _box_panel(ax, q, colors, ref_per_day, ylabel, title, ref_mode="fixed", ref_days=30):
    for d, r in q.dropna(subset=["median"]).iterrows():
        partial = r["availability"] == "partial"
        c = colors.get("partial", "#D9D9D9") if partial else colors[r["availability"]]
        ax.vlines(d, r["min"], r["max"], color="#555555", lw=0.5)
        ax.bar(d, r["q3"] - r["q1"], bottom=r["q1"], width=WIDTH_DAYS,
               color=c, edgecolor="#333333", lw=0.3, align="center",
               hatch="////" if partial else None)
    # median line breaks at partial months (drawn as points instead)
    med = q["median"].where(q["availability"] != "partial")
    ax.plot(q.index, med, color=colors["median"], lw=1.1)
    pm = q[q["availability"] == "partial"]
    ax.plot(pm.index, pm["median"], ls="none", marker="o", ms=2.5, mfc="white",
            mec=colors["median"], mew=0.8)
    if ref_mode == "monthly":
        ax.step(q.index, q["days_in_month"] * ref_per_day, where="mid",
                color=colors["reference"], lw=1, ls="--")
    else:
        ax.axhline(ref_days * ref_per_day, color=colors["reference"], lw=1, ls="--")
    ax.set_ylabel(ylabel)
    ax.set_title(title, loc="left", fontsize=10, fontweight="bold")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="#000000", alpha=0.08)


def _partial_spans(ax, idx, colors):
    """Partially covered months: light tint in the panel-(d) 'partial' colour (v1.3;
    was a diagonal hatch, easily read as 'no calendar')."""
    for d in idx:
        ax.axvspan(d - timedelta(days=14), d + timedelta(days=14),
                   facecolor=colors.get("cov_partial", "#DD8452"), alpha=0.18, lw=0, zorder=0)


def _missing_spans(ax, idx, colors):
    """Months without a calendar: same grey + 'xxx' hatch as panel (d) and the legend (v1.3)."""
    for d in idx:
        ax.axvspan(d - timedelta(days=14), d + timedelta(days=14),
                   facecolor=colors.get("missing", "#EEEEEE"), edgecolor="#777777",
                   hatch="xxx", lw=0, zorder=0)


def _band_panel(ax, q, colors, ref_per_day, ylabel, title, ref_mode="fixed", ref_days=30,
                show_minmax=False):
    """Median line with interquartile band; partial months as hollow points with IQR bars;
    low/high months marked on the median; months without data leave a gap."""
    full = q.where(~q["availability"].isin(["partial"]))
    if show_minmax:
        ax.fill_between(full.index, full["min"], full["max"], color=colors["band"],
                        alpha=0.15, lw=0)
    ax.fill_between(full.index, full["q1"], full["q3"], color=colors["band"], alpha=0.35, lw=0)
    ax.plot(full.index, full["median"], color=colors["median"], lw=1.0)
    m = full["median"]
    alone = m.notna() & m.shift(1).isna() & m.shift(-1).isna()   # a month between two gaps
    ax.plot(m[alone].index, m[alone], ls="none", marker="_", ms=6, color=colors["median"])
    for cls in ("low", "high"):
        m = q[q["availability"] == cls]
        ax.plot(m.index, m["median"], ls="none", marker="o", ms=3.5, color=colors[cls],
                mec="#333333", mew=0.4, zorder=3)
    pm = q[q["availability"] == "partial"]
    ax.vlines(pm.index, pm["q1"], pm["q3"], color=colors["median"], lw=0.8)
    ax.plot(pm.index, pm["median"], ls="none", marker="o", ms=3.5, mfc="white",
            mec=colors["median"], mew=0.8, zorder=3)
    if ref_mode == "monthly":
        ax.step(q.index, q["days_in_month"] * ref_per_day, where="mid",
                color=colors["reference"], lw=1, ls="--")
    else:
        ax.axhline(ref_days * ref_per_day, color=colors["reference"], lw=1, ls="--")
    ax.set_ylabel(ylabel)
    ax.set_title(title, loc="left", fontsize=10, fontweight="bold")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="#000000", alpha=0.08)


def _spread_panel(ax, spread, partial_idx, colors, stat_label, title):
    full = spread.copy()
    full.loc[full.index.isin(partial_idx)] = float("nan")
    ax.plot(full.index, full.values, color=colors["dispersion"], lw=1.1)
    pts = spread[spread.index.isin(partial_idx)]
    ax.plot(pts.index, pts.values, ls="none", marker="o", ms=3, mfc="white",
            mec=colors["dispersion"], mew=0.8)
    ax.set_ylabel(stat_label)
    ax.set_title(title, loc="left", fontsize=10, fontweight="bold")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="#000000", alpha=0.08)


def _coverage_panel(ax, cov, colors, title, entry_label):
    """Share of entries per month whose calendars cover the whole month / part of it;
    months without a calendar drawn as a grey bar."""
    w = 25
    ax.bar(cov.index, cov["complete"].fillna(0), width=w, color=colors["cov_complete"], lw=0)
    ax.bar(cov.index, cov["partial"].fillna(0), bottom=cov["complete"].fillna(0), width=w,
           color=colors["cov_partial"], lw=0)
    miss = cov.index[cov["n"].isna()]
    ax.bar(miss, [1] * len(miss), width=w, color=colors.get("missing", "#EEEEEE"),
           edgecolor="#777777", hatch="xxx", lw=0)
    ax.set_ylim(0, 1)
    ax.set_yticks([0, 0.5, 1])
    ax.set_yticklabels(["0", "50%", "100%"])
    ax.set_ylabel("Share of entries")
    ax.set_title(title, loc="left", fontsize=10, fontweight="bold")


def figure6(q_hours, q_days, spread, cov, stat_label, colors, ref_hours_label, ref_mode="fixed",
            ref_days=30, nb_label="neighborhood", tick_months=(1, 5, 9), date_fmt="%Y_%m",
            entry_label="calendar entry", partial_label="Partially covered month (not classified)",
            show_spread=True, show_minmax=False, min_cov=0.9):
    panels = ["hours", "days"] + (["spread"] if show_spread else []) + (["coverage"] if cov is not None else [])
    ratios = {"hours": 1.3, "days": 1.3, "spread": 0.8, "coverage": 0.6}
    fig, axes = plt.subplots(len(panels), 1, figsize=(10, 2.6 * len(panels) + 0.8), sharex=True,
                             gridspec_kw={"height_ratios": [ratios[p] for p in panels]})
    ax = dict(zip(panels, axes))
    letters = iter("abcdefg")
    _band_panel(ax["hours"], q_hours, colors, 24, "Scheduled hours per month",
                f"{next(letters)}) Hours of scheduled piped supply per {entry_label}",
                ref_mode, ref_days, show_minmax)
    _band_panel(ax["days"], q_days, colors, 1, "Scheduled days per month",
                f"{next(letters)}) Days of scheduled piped supply per {entry_label}",
                ref_mode, ref_days, show_minmax)
    partial_idx = q_hours.index[q_hours["availability"] == "partial"]
    missing_idx = q_hours.index[q_hours["availability"] == "missing"]
    if show_spread:
        _spread_panel(ax["spread"], spread, partial_idx, colors, stat_label,
                      f"{next(letters)}) Spread of scheduled hours between {nb_label}")
    if cov is not None:
        _coverage_panel(ax["coverage"], cov, colors,
                        f"{next(letters)}) Coverage: share of entries whose calendars cover the whole month",
                        entry_label)
    for p, a in ax.items():
        if p != "coverage":
            _partial_spans(a, partial_idx, colors)
            _missing_spans(a, missing_idx, colors)
        for s in ("top", "right"):
            a.spines[s].set_visible(False)
    fig.align_ylabels(axes)
    last = axes[-1]
    _month_axis(last, tick_months, date_fmt)
    last.set_xlabel("Year–Month")
    pad = timedelta(days=20)
    last.set_xlim(q_hours.index.min() - pad, q_hours.index.max() + pad)

    handles = [
        Line2D([], [], color=colors["median"], lw=1.0, label="Monthly median"),
        Patch(facecolor=colors["band"], alpha=0.35, label="Interquartile range"),
        Line2D([], [], ls="none", marker="o", color=colors["low"], mec="#333333",
               label="Low month (median below mean − 1 SD)"),
        Line2D([], [], ls="none", marker="o", color=colors["high"], mec="#333333",
               label="High month (median above mean + 1 SD)"),
        (Patch(facecolor=colors.get("cov_partial", "#DD8452"), alpha=0.18),
         Line2D([], [], ls="none", marker="o", mfc="white", mec=colors["median"])),
        Line2D([], [], color=colors["reference"], lw=1, ls="--", label=ref_hours_label),
    ]
    if show_minmax:
        handles.insert(2, Patch(facecolor=colors["band"], alpha=0.15, label="Min–max"))
    if cov is not None:
        handles += [
            Patch(facecolor=colors["cov_complete"], label=f"Whole month covered (≥ {min_cov:.0%})"),
            Patch(facecolor=colors["cov_partial"], label="Part of the month covered"),
            Patch(facecolor=colors.get("missing", "#EEEEEE"), edgecolor="#777777", hatch="xxx",
                  label="No calendar obtained"),
        ]
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    labels = [partial_label if isinstance(h, tuple) else h.get_label() for h in handles]
    fig.legend(handles=handles, labels=labels, fontsize=8, loc="upper center", ncol=3,
               frameon=False, bbox_to_anchor=(0.5, 1.0))
    return fig


def example_neighborhoods(df, id_col, hours_col, ids, colors, tick_months=(1, 5, 9), date_fmt="%Y_%m",
                          cov_col=None, min_cov=None, missing_months=(), sharey=False, xlim=None):
    """Monthly scheduled hours for a few neighborhoods (supplementary figure).

    1.4: the line breaks at months without a value (no calendar, or neighborhood not
    listed); months without any calendar are hatched as in Figure 6; partially covered
    months (cov_col < min_cov) are drawn as open points, off the line, on a light tint.
    """
    lo = xlim[0] if xlim else df["date"].min()
    hi = xlim[1] if xlim else df["date"].max()
    full = pd.date_range(lo, hi, freq="MS")
    fig, axes = plt.subplots(len(ids), 1, figsize=(10, 2.2 * len(ids)), sharex=True,
                             sharey=sharey, squeeze=False)
    any_partial = False
    for ax, uid in zip(axes[:, 0], ids):
        s = (df[df[id_col] == uid].drop_duplicates("date").set_index("date")
             .sort_index().reindex(full))
        y = s[hours_col]
        partial = (s[cov_col] < min_cov) if (cov_col and min_cov is not None and cov_col in s) \
            else pd.Series(False, index=s.index)
        _missing_spans(ax, missing_months, colors)
        _partial_spans(ax, s.index[partial.fillna(False)], colors)
        ax.plot(s.index, y.where(~partial), color=colors["dispersion"], lw=1, marker="o", ms=2)
        pm = partial.fillna(False)
        if pm.any():
            any_partial = True
            ax.plot(s.index[pm], y[pm], ls="none", marker="o", ms=3.5, mfc="white",
                    mec=colors["dispersion"], mew=0.9)
        ax.set_title(f"{uid}", loc="left", fontsize=9)
        ax.set_ylabel("Hours / month")
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", color="#000000", alpha=0.08)
        for sp in ("top", "right"):
            ax.spines[sp].set_visible(False)
    _month_axis(axes[-1, 0], tick_months, date_fmt)
    axes[-1, 0].set_xlabel("Year–Month")
    pad = timedelta(days=20)
    axes[-1, 0].set_xlim(full.min() - pad, full.max() + pad)
    handles = [Line2D([], [], color=colors["dispersion"], marker="o", ms=3, lw=1,
                      label="Scheduled hours (mean across distribution groups)")]
    if len(missing_months):
        handles.append(Patch(facecolor=colors.get("missing", "#EEEEEE"), edgecolor="#777777",
                             hatch="xxx", label="No calendar"))
    if any_partial:
        handles.append(Line2D([], [], ls="none", marker="o", ms=4, mfc="white",
                              mec=colors["dispersion"], label="Partially covered month"))
    fig.legend(handles=handles, fontsize=8, loc="upper center", ncol=len(handles), frameon=False,
               bbox_to_anchor=(0.5, 1.03))
    fig.tight_layout()
    return fig
