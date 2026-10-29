"""Human-in-the-loop exceptions for neighborhood text (parse_calendar_excel).

Some cells cannot be split by commas alone, e.g. "LAURELES 1, 2, 3",
"VILLA NUEVA NORTE Y SUR S: 1,2,7,8", "..., SECTOR 6 Y CASAS NUEVAS".
EVERY cell is first looked up in ONE reviewed CSV; cells not in the CSV that
match one of the review `patterns` are listed as pending:

    raw_text,action,converted,note
    "LAURELES 1, 2, 3",fix,"LAURELES (1, 2, 3)",Laureles 1-7 share one SDG11UID
    "...",ok,,matches the pattern but splits correctly

  fix     -> the cell is parsed as `converted` (parentheses protect the commas)
  ok      -> parsed as written
  missing -> parsed as written AND listed as pending, for review; once the
             reviewer adds it to the CSV, the next run applies the decision.
The published text is never overwritten: raw_group keeps the original.
"""
import re
from pathlib import Path

import pandas as pd

DEFAULT_PATTERNS = [r"\d+\s*,\s*\d+"]          # numbers separated by commas


def normalise_key(text):
    """Key used to look up a cell: upper case, single spaces, trimmed."""
    return re.sub(r"\s+", " ", str(text).upper()).strip()


def load_exceptions(path):
    """Reviewed decisions as {key: (action, converted)}. Missing file -> {}."""
    if not path or not Path(path).exists():
        return {}
    t = pd.read_csv(path, encoding="utf-8-sig", dtype=str).fillna("")
    t.columns = t.columns.str.strip().str.lower()
    out = {}
    for _, r in t.iterrows():
        action = r.get("action", "").strip().lower()
        if action not in ("fix", "ok"):
            raise ValueError(f"exceptions CSV {path}: action must be fix or ok, got {action!r} "
                             f"for {r.get('raw_text')!r}")
        if action == "fix" and not r.get("converted", "").strip():
            raise ValueError(f"exceptions CSV {path}: 'fix' needs a converted text for "
                             f"{r.get('raw_text')!r}")
        out[normalise_key(r["raw_text"])] = (action, r.get("converted", "").strip())
    return out


def resolve(text, exceptions, patterns=None):
    """Return (text_to_parse, status). status: '' (nothing to do), fix, ok, pending.

    1. a cell listed in the CSV gets the reviewed decision, whatever it contains;
    2. otherwise, a cell matching any review pattern is pending (parsed as written).
    """
    if not isinstance(text, str):
        return text, ""
    hit = exceptions.get(normalise_key(text))
    if hit is not None:
        action, converted = hit
        return (converted, "fix") if action == "fix" else (text, "ok")
    pats = DEFAULT_PATTERNS if patterns is None else patterns
    if isinstance(pats, str):
        pats = [pats]
    if any(re.search(p, text, flags=re.IGNORECASE) for p in pats):
        return text, "pending"
    return text, ""
