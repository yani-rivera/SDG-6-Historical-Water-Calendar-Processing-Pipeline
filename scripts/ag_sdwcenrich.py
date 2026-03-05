import pandas as pd

# Load enriched unified file
df = pd.read_csv("allfiles_WC.csv", encoding="utf-8-sig")
df.columns = df.columns.str.strip()

# Numeric safety
df["hours_with_water"] = pd.to_numeric(
    df["hours_with_water"], errors="coerce"
).fillna(0)

df["has_water"] = pd.to_numeric(
    df["has_water"], errors="coerce"
).fillna(0)

# Aggregate
agg = (
    df.groupby(
        [
            "SDG11UID",
            "neighborhood_clean",
            "wc_uid",
            "water_sector",
            "water_source",
            "YearMonth"
        ],
        dropna=False
    )
    .agg(
        total_hours=("hours_with_water", "sum"),
        total_days=("has_water", "sum")
    )
    .reset_index()
    .sort_values(["SDG11UID", "YearMonth", "wc_uid"])
)

agg.to_csv(
    "agg_sdg11_wc_sector_source_monthly.csv",
    index=False,
    encoding="utf-8-sig"
)

print("Rows:", len(agg))