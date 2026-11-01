# Figure 6 (SDG-6 water calendars)

Builds Figure 6 (1.2: a: hours, b: days per calendar entry as monthly median with
interquartile band; c: spread between calendar neighborhoods, wc_uid by default;
d: coverage, share of entries whose calendars cover the whole month) and a supplementary figure of example neighborhoods.

    pip install pandas matplotlib pyyaml
    python make_figure6.py --config config/figure6.yaml

Edit `config/figure6.yaml` for input paths, column names, period, colours
and the example SDG11UIDs. Outputs (PNG, PDF, summary CSVs and
`provenance_figure6.json` with input hashes and versions) go to `output/figures`.
The script never filters or changes pipeline outputs; data fixes belong in the
pipeline. Rows outside the configured period are plotted as found and listed in
`rows_outside_period.csv`.

## 1.4 Supplementary figure (example neighborhoods)

Reads `inputs.examples` (Merged_SDG11, one row per SDG11UID and month), so the
examples are neighborhoods with a polygon; ids starting with `examples.exclude_prefix`
(NOGIS) are never picked. Each panel has its own y scale (`examples.sharey: false`).
The line breaks at months without a value; months without any calendar are hatched
as in Figure 6, and partially covered months (coverage < `coverage.min`) are drawn
as open points on a light tint. The picked ids and the months without a calendar are
recorded in `provenance_figure6.json`.
