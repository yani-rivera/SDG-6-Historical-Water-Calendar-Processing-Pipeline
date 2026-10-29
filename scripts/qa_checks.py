"""ID-based checks for the NO-GIS validation. No name matching."""
import pandas as pd


def flag_nogis(ids, raw, no_gis_values, treat_missing):
    """True where the SDG11UID is a NO-GIS placeholder (or empty)."""
    mask = ids.isin([str(v) for v in no_gis_values])
    if treat_missing:
        mask |= raw.isna() | ids.eq("")
    return mask


def wc_mixed(df, wc_col, nogis, ids, no_gis_values):
    """Check 1: wc_uids that are NO-GIS in some rows and have an SDG11UID in others."""
    skip = {str(v) for v in no_gis_values} | {""}
    t = df.assign(_nogis=nogis, _id=ids)
    g = (t.groupby(wc_col)
          .agg(nogis_rows=("_nogis", "sum"), rows=("_nogis", "size"),
               sdg11uids=("_id", lambda x: ";".join(sorted(set(x) - skip))))
          .reset_index())
    return g[(g["nogis_rows"] > 0) & (g["sdg11uids"] != "")]


def wc_in_catalog(df, wc_col, nogis, catalog, cat_wc, cat_id, no_gis_values):
    """Check 2: NO-GIS wc_uids that the catalog assigns to a real SDG11UID."""
    skip = {str(v) for v in no_gis_values} | {""}
    cat = catalog[[cat_wc, cat_id]].copy()
    cat[cat_id] = cat[cat_id].fillna("").astype(str).str.strip()
    cat = cat[~cat[cat_id].isin(skip)].drop_duplicates()
    ng = (df[nogis].groupby(wc_col).size().rename("nogis_rows").reset_index())
    return ng.merge(cat, left_on=wc_col, right_on=cat_wc, how="inner")


def ids_without_polygon(ids, nogis, poly_ids):
    """Check 3: SDG11UIDs used in all_hours that are not in the polygon layer."""
    return sorted(set(ids[~nogis]) - set(poly_ids))


def nogis_list(df, wc_col, nogis):
    """All NO-GIS wc_uids with their row counts (for the record)."""
    return df[nogis].groupby(wc_col).size().rename("rows").reset_index()
