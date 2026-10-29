"""Step 3: all_hours record -> calproc_wcuid (raw group) -> calproc (parser output).

For each sampled record:
  check_3a  the record exists in calproc_wcuid (same calendar, wc_uid, group_uid, schedule)
      and points to exactly ONE raw group
  check_3b  that raw group, with the same schedule and parser neighbourhood, exists in calproc
  check_3c  the days with water in calproc = days_with_water in all_hours
  check_3d  hours_with_water = days_with_water x hours_planned
"""
import pandas as pd

from .stages import read_stage


def _days(rows, c):
    d = pd.to_numeric(rows[c["day"]], errors="coerce")
    w = pd.to_numeric(rows[c["has_water"]], errors="coerce").fillna(0) > 0
    return sorted({int(x) for x in d[w].dropna()})


def trace(sample, cfg, c):
    out, cache = [], {}
    for code in sorted(sample["calendar"].unique()):
        cache[code] = (read_stage(cfg, "calproc_wcuid", code), read_stage(cfg, "calproc", code))
    for _, r in sample.iterrows():
        wc, cp = cache[r["calendar"]]
        rec = {"record_id": r["record_id"], "selected_because": r["selected_because"],
               "calendar": r["calendar"], "wc_uid": r[c["wc_uid"]], "group_uid": r[c["group_uid"]],
               "neighborhood_final": r.get(c["neighborhood"], ""), "schedule": r[c["schedule"]],
               "days_with_water": r[c["days_with_water"]], "days_observed": r[c["days_observed"]],
               "hours_planned": r[c["hours_planned"]], "hours_with_water": r[c["hours_with_water"]]}
        if wc is None or cp is None:
            rec.update(check_3a="stage file missing")
            out.append(rec)
            continue
        m = ((wc[c["wc_uid"]].str.strip() == r[c["wc_uid"]]) &
             (wc[c["group_uid"]].str.strip() == r[c["group_uid"]]) &
             (wc[c["schedule"]].str.strip() == r[c["schedule"]]))
        rows = wc[m]
        raws = sorted(rows[c["raw_group"]].dropna().str.strip().unique())
        parser_nb = sorted(rows[c["parser_neighborhood"]].dropna().str.strip().unique())
        rec["raw_group"] = " || ".join(raws)
        rec["parser_neighborhood"] = " || ".join(parser_nb)
        rec["name_resolved_manually"] = "yes" if rows.get(c["exception"], pd.Series(dtype=str)).isin(["fix", "ok"]).any() else ""
        if rows.empty:
            rec.update(check_3a="not found in calproc_wcuid")
            out.append(rec)
            continue
        rec["check_3a"] = "ok" if len(raws) == 1 else f"{len(raws)} raw groups (group split over rows?)"
        # 3b: same raw group + schedule + parser neighbourhood in calproc
        mp = (cp[c["raw_group"]].str.strip().isin(raws) &
              (cp[c["schedule"]].str.strip() == r[c["schedule"]]) &
              cp[c["parser_neighborhood"]].str.strip().isin(parser_nb))
        prow = cp[mp]
        rec["check_3b"] = "ok" if not prow.empty else "raw group + schedule not in calproc"
        # 3c: days
        days = _days(prow, c) if not prow.empty else []
        rec["days_with_water_list"] = ", ".join(map(str, days))
        dww = pd.to_numeric(r[c["days_with_water"]], errors="coerce")
        rec["check_3c"] = "ok" if len(days) == dww else f"days differ: calproc {len(days)}, all_hours {dww:g}"
        # 3d: hours
        hp = pd.to_numeric(r[c["hours_planned"]], errors="coerce")
        hww = pd.to_numeric(r[c["hours_with_water"]], errors="coerce")
        ok = abs(dww * hp - hww) < 1e-6
        rec["check_3d"] = "ok" if ok else f"hours differ: {dww:g} days x {hp:g} h != {hww:g} h"
        out.append(rec)
    return pd.DataFrame(out)
