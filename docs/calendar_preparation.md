# Calendar Preparation — Stage 0 (HITL)

## Purpose

Prepare each water-distribution calendar as a clean, source-faithful Excel workbook **before** running `parse_excel` in the SDG-6 pipeline. This is a human-in-the-loop (HITL) preparation and validation step, not the downstream automated neighborhood-normalization or UID-matching stage.

## Input and output

- **Input:** Calendar converted from its original PDF or other source to Excel; retain the original source and unedited conversion separately.
- **Output:** One validated `.xlsx` workbook in `calendar/`, named `YYYY_MM.xlsx` (full month), `YYYY_MMa.xlsx` (first half), or `YYYY_MMb.xlsx` (second half), as applicable.
- **Worksheet:** Exactly **one worksheet** containing the calendar table; no images.

## Required worksheet structure

The **first row must be the actual column headers**. Data begin in row 2. The first five columns, in this exact order, are:

`Sector` | `source` | `dist` | `Barrio o Colonia` | `Horario`

Follow these with the applicable **day-number columns in ascending order** (`1`, `2`, …, up to `31`); do not add days that are not part of the source calendar. A full 31-day month has 36 columns in total.

- `source` means **water source**; `dist` means **distributor**. Keep them separate.
- Remove introductory titles, dates/timestamps outside the table, extra heading rows, repeated headers within the data, trailing notes mistakenly imported as columns, and all **additional columns**.
- Remove images and extraneous worksheets. Unmerge cells and verify that values have not been lost or incorrectly propagated.
- Preserve the original distribution schedules, day markers, dates, and substantive water-availability information. Do not guess missing or ambiguous values.
- Confirm that the headers are on Excel row 1 and that no blank rows precede them.

## HITL neighborhood review

Review `Barrio o Colonia` for common OCR, transcription, punctuation, and formatting errors. Apply the **confirmed replacements** below as exact-value corrections where appropriate; do not perform speculative substitutions. When a name or variant is uncertain, **consult `consolidated/all_hours.csv` (`all_hours`) for previously used neighborhood variants** and compare against the original calendar. `all_hours` is a reference for consistency, **not** an authority to overwrite the original source without verification. Escalate unresolved cases for human review.

### Confirmed correction examples

| Original | Prepared value |
|---|---|
| `ALDEA LA CAÑADA, ZONA 1` | `ALDEA LA CAÑADA ZONA 1` |
| `ROBLE OESTE VI ETAPA, ZONA 1 Y 2` | `ROBLE OESTE VI ETAPA ZONA 1-2` |
| `HATO DE ENMEDIO S: 1 - 9` | `HATO DE ENMEDIO S1-9` |
| `VILLA NUEVA NORTE Y SUR S: 1,2,7,8 (OPERADO POR SECTORES)` | `VILLA NUEVA NORTE Y SUR` |
| `CENTRO AMERICA  OESTE ZONA 3, 4 Y 5` | `CENTRO AMERICA  OESTE ZONA 3-5` |
| `3 DE MAYO SECTOR 1, 2 Y 3` | `3 DE MAYO SECTOR 1-3` |
| `ALTOS DEL TRAPICHE  (1,2 y 3 ETAPA)` | `ALTOS DEL TRAPICHE  (1-3 ETAPA)` |
| `ALTOS DEL TRAPICHE (V ETAPA) BLOUES: H, I, J, K, L, M, N, O, P` | `ALTOS DEL TRAPICHE (V ETAPA) BLOUES: H -I- J- K- L- M- N- O- P` |
| `ALTOS DEL TRAPICHE (V ETAPA) BLOUES: A, B, C, D, E, F, G` | `ALTOS DEL TRAPICHE (V ETAPA) BLOUES: A-B- C-D- E- F- G` |

`ROBLE OESTE` appeared twice in the supplied examples and is listed once. **`BLOUES` is retained exactly as supplied**, pending human confirmation; do not silently change it to `BLOQUES`. Double spaces in examples are shown where supplied; confirm the intended canonical spacing during review.

## Validation checklist

- [ ] Original calendar/source and unedited conversion retained.
- [ ] Workbook contains **exactly one worksheet** and **no images**.
- [ ] Row 1 contains only the required headers; row 2 begins the data.
- [ ] Five descriptive columns appear in the required order.
- [ ] Only applicable day-number columns are present, in ascending order.
- [ ] No extra columns, repeated headers, introductory rows, or unintended merged cells remain.
- [ ] Sector, water source, distributor, neighborhood, and schedule fields are correctly aligned.
- [ ] Common neighborhood transcription/formatting problems have been checked and confirmed corrections applied.
- [ ] Uncertain neighborhood variants checked against `all_hours` **and** the original source, or flagged for HITL.
- [ ] Day markers and schedules have been checked against the source; ambiguous cases are flagged, not invented.
- [ ] Output saved as correctly named `.xlsx` in `calendar/`.

## Audit trail

For every manual correction, record at minimum: workbook filename, worksheet/row, field, original value, corrected value, reason, reviewer, and review status. Store this correction log **outside** the calendar workbook so the validated workbook remains a single-sheet parser input. Keep unresolved items in the review log until confirmed.

## Pipeline handoff

`Original calendar → Excel conversion → Stage 0 preparation and HITL validation → calendar/*.xlsx → parse_excel → remaining pipeline`

Stage 0 corrects source-preparation issues. Later `clean_neighborhood` and UID-matching stages perform systematic standardization and catalog matching; they do not replace source-level HITL verification.
