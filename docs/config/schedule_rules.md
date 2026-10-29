# Configuration File 14 — schedule_rules.yaml

## Purpose
Defines ordered normalization rules for irregular water-distribution schedule strings before extraction of scheduled hours.

## Intended integration
- Intended consumer: `process_hours.py`
- Input: raw schedule strings
- Output: normalized schedule text / special literal parse values
- Format: YAML
- External dependency if loaded by Python: `PyYAML` (already inventoried)

## Sections
1. `literals`: whole-string mappings; `ONE-DAY` maps to start 6, end 6, 12 hours, no midnight crossing.
2. `char_normalize`: replace en/em dashes with hyphens and carriage returns with newlines.
3. `simple_replace`: normalize `12MD`, `MD`, `12MN`, and `MN`.
4. `regex_replace`: repair shorthand AM/PM, missing interval separators, parenthesized intervals, and line-break interruptions.
5. `strip_chars`: remove parentheses.

Intended order: literals → char_normalize → simple_replace → regex_replace → strip_chars.

## Integration review
**Not currently connected to uploaded `process_hours.py` v3.0.** That script embeds the equivalent replacements and regular expressions directly in `parse_schedule_hours()` and does not read this YAML file. Editing the YAML therefore will not affect the reviewed parser version.

## Review flags
- YAML documentation promises malformed-rule warnings and built-in fallback, but these behaviors require a loader absent from the reviewed Python script.
- The comment about the `12MD` replacement order is misleading; the configured replacement order itself is correct.
- Special `ONE-DAY` mapping is a fixed 12-hour convention and merits domain validation.
- Add a loader and tests in a future improvement pass; do not modify the current scripts yet.

## Cumulative third-party dependencies
`pandas`, `openpyxl`, `PyYAML` — unchanged.
