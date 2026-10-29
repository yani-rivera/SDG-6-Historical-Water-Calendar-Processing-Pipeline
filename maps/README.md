# Figures 3-4: neighborhood maps (SDG-6 water calendars)

Two figures, one for hours and one for days, each with three map panels:
(a) mean of the record 2016-2025, (b) lowest-availability month, (c) highest-availability
month, plus a panel with scale bar and legend. Months are selected automatically from the
city-wide monthly mean (mean over neighborhoods) of `selection_variable`.

    conda install -c conda-forge geopandas pyyaml    # once
    python maps/make_maps.py --config maps/config/maps.yaml

Inputs (config): Summaries/Merged_SDG11.csv (aggregate_pipeline 1.1) and the neighborhood
polygons with SDG11UID; optional background layers. Classes, colours, labels, panel order
and titles are set in the config. Neighborhoods without a schedule are not coloured; the full neighborhood layer is drawn as
a light grey base, and the maps zoom to the neighborhoods with schedules.
Outputs (output/maps): PNG + PDF per figure, each map on its own named by period
(e.g. hours_2020-02_lowest.png, days_2016-2025_period_mean.png),
city_monthly_means.csv, provenance_maps.json (input hashes, panels and months, versions).
