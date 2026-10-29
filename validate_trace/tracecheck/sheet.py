"""Review sheet (original calendar check by the author) and summary counts."""
import pandas as pd

ORIG = ["original_group_ok", "original_neighborhood_ok", "original_schedule_ok", "original_days_ok"]
AUTO = ["check_3a", "check_3b", "check_3c", "check_3d"]
LABELS = {
    "check_3a": "3a. record traces to one raw group",
    "check_3b": "3b. raw group + schedule in parser output",
    "check_3c": "3c. days with water = parser's marked days",
    "check_3d": "3d. hours = days x scheduled hours",
}


def review_sheet(traced, prep):
    s = traced.copy()
    s.insert(3, "original_file", s["calendar"].map(lambda k: (prep.get(k) or {}).get("source_file", "")))
    s.insert(4, "format", s["calendar"].map(lambda k: (prep.get(k) or {}).get("source_format", "")))
    for col in ORIG + ["discrepancy", "correction"]:
        s[col] = ""
    return s


def summarize_reviewed(sheet):
    s = sheet.fillna("")
    done = s[s[ORIG].apply(lambda r: all(str(v).strip() for v in r), axis=1)]
    res = {"records_in_sample": int(len(s)), "records_reviewed_against_original": int(len(done)),
           "calendars": int(s["calendar"].nunique()), "years": sorted({k[:4] for k in s["calendar"]}),
           "selected_because": s["selected_because"].str.split(":").str[0].value_counts().to_dict(),
           "checks": {}}
    for col in AUTO:
        v = s[col].astype(str).str.strip()
        res["checks"][LABELS[col]] = v[v.ne("")].value_counts().to_dict()
    for col in ORIG:
        v = done[col].astype(str).str.strip().str.lower()
        res["checks"][col.replace("_", " ")] = {"ok": int(v.eq("ok").sum()),
                                                 "not ok": int((v.ne("ok") & v.ne("")).sum())}
    disc = done[done["discrepancy"].astype(str).str.strip() != ""]
    res["records_with_discrepancy"] = int(len(disc))
    res["records_corrected"] = int((done["correction"].astype(str).str.strip() != "").sum())
    res["discrepancies"] = disc[["record_id", "discrepancy", "correction"]].to_dict("records")
    return res
