"""Step 3 selection: strata + specified ('at will') + random per year. Reproducible (seed)."""
import re

import pandas as pd


def strata_flags(ah, c, prep, wc_keys, scfg):
    cal = ah["calendar"]
    fmt = cal.map(lambda k: (prep.get(k) or {}).get("source_format", ""))
    exc_col = c["exception"]
    resolved_ids = set()
    if exc_col in wc_keys.columns:
        r = wc_keys[wc_keys[exc_col].isin(["fix", "ok"])]
        resolved_ids = set(r["calendar"] + "|" + r[c["wc_uid"]] + "|" + r[c["group_uid"]] + "|" + r[c["schedule"]])
    return {
        "pdf": fmt.eq("pdf"),
        "excel": fmt.eq("xlsx"),
        "paired": cal.str.match(r"^\d{4}_\d{2}[ab]$"),
        "cross_month": cal.isin(scfg.get("cross_month_calendars") or []),
        "partial": cal.isin(scfg.get("partial_calendars") or []),
        "resolved_names": ah["record_id"].isin(resolved_ids),
    }


def _take(pool, n, seed, reason, picked):
    pool = pool[~pool["record_id"].isin(picked)]
    if pool.empty or n <= 0:
        return pd.DataFrame()
    out = pool.sample(n=min(n, len(pool)), random_state=seed).copy()
    out["selected_because"] = reason
    picked.update(out["record_id"])
    return out


def select(ah, c, flags, scfg):
    seed, picked, parts = scfg.get("seed", 2026), set(), []
    for spec in scfg.get("specified") or []:                     # at will
        m = ah["calendar"] == spec["calendar"]
        if spec.get("wc_uid"):
            m &= ah[c["wc_uid"]] == spec["wc_uid"]
        if spec.get("group_uid"):
            m &= ah[c["group_uid"]] == spec["group_uid"]
        if spec.get("neighborhood_contains"):
            m &= ah[c["neighborhood"]].str.contains(re.escape(spec["neighborhood_contains"]),
                                                    case=False, na=False)
        got = _take(ah[m], spec.get("n", 1), seed, f"specified:{spec['calendar']}", picked)
        if got.empty:
            print(f"WARNING: specified record not found: {spec}")
        parts.append(got)
    for name, n in (scfg.get("strata") or {}).items():          # reviewer's strata
        if name not in flags:
            raise SystemExit(f"Unknown stratum '{name}'. Available: {sorted(flags)}")
        parts.append(_take(ah[flags[name]], n, seed, f"stratum:{name}", picked))
    for year, grp in ah.groupby(c["year"]):                    # random, every year
        parts.append(_take(grp, scfg.get("random_per_year", 5), seed, f"random:{year}", picked))
    parts = [x for x in parts if not x.empty]
    out = pd.concat(parts, ignore_index=True) if parts else ah.head(0)
    return out.sort_values(["calendar", c["wc_uid"]]).reset_index(drop=True)
