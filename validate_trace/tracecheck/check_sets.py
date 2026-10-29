"""Steps 1 and 2: every identifier in all_hours exists in its stage, per calendar (all rows)."""
import pandas as pd


def ids_present(ah, keys, id_col, step_name):
    """Per calendar: ids of all_hours missing from the stage keys."""
    have = set(zip(keys["calendar"], keys[id_col]))
    pairs = ah[["calendar", id_col]].drop_duplicates()
    pairs["found"] = [(a, b) in have for a, b in zip(pairs["calendar"], pairs[id_col])]
    per_cal = pairs.groupby("calendar")["found"].agg(ids="size", found="sum").reset_index()
    per_cal["missing"] = per_cal["ids"] - per_cal["found"]
    per_cal.insert(0, "step", step_name)
    missing = pairs[~pairs["found"]].drop(columns="found").assign(step=step_name)
    return per_cal, missing
