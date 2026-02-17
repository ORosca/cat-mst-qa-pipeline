# CAT/MST Quality Assurance Pipeline (R)
A config-driven QA pipeline for CAT/MST item-response datasets in R, designed to support reliable calibration, equating, and drift monitoring

A reproducible quality-assurance (QA) workflow for computer-adaptive testing (CAT) / multistage testing (MST) item-response datasets. The pipeline standardizes identifiers, supports optional item-ID mapping to stable QIDs, applies low-effort filters (timing and minimum-items rules), and produces a compact set of QA artifacts (tables + plots) that help validate the technical quality of an operational assessment dataset before calibration, equating, or reporting.

## What this repo contains
- **`qa_cat_mst_pipeline.R`** — main pipeline (functions + runner + dataset config pattern)
- **`examples/`** — small example outputs (CSV + PNG) demonstrating the artifacts produced by the pipeline

> **Data are not included** in this repository.
Example outputs are included for demonstration; do not commit sensitive artifacts.

## Inputs (expected structure)
The pipeline assumes two files per dataset:
1. **Student-level file** (RDS): includes `DAACS_ID` and (optionally) start/end timestamps for the assessment.
2. **Item-level file** (RDS): long-format item responses with `DAACS_ID`, item identifier (e.g., `qid`), response/score (e.g., `score`), and (optionally) `attempt`.

Optional:
- **Mapping file** (XLSX) to map dataset-specific item IDs (e.g., `qid`) to a stable cross-wave identifier (`QID`).

## Global IDs (recommended practice)
To prevent ID collisions across institutions and collection waves, the pipeline creates:
- `DAACS_ID_raw` — preserved original `DAACS_ID` (as character)
- `DAACS_ID_global` — deterministic global identifier created from `college_id | wave | DAACS_ID_raw`

This avoids ad hoc “add 10,000” / “add 100,000” ID-offset conventions and supports cross-wave linking and drift analyses.

## Key QA checks produced
The pipeline generates:
- **Duplicate ID checks** (student file; response matrix ID column)
- **Identical response-pattern checks** (possible copy/paste / test integrity flags)
- **Non-discriminative item flags** (items with no observed variation)
- **Item value ranges + response counts** (min/max, n per item)
- **Response-count distributions by difficulty label** (E/M/H parsed from QID naming convention, when present)
- **Density plots** of item response counts + minimum-count reference line

These artifacts are designed as *investigation triggers* for operational QA—not as universal pass/fail cut scores.

## Outputs
For each dataset, the pipeline writes a compact QA bundle to `output_dir`, including:
- `<dataset>_item_counts.csv`
- `<dataset>_item_ranges.csv`
- `<dataset>_counts_by_difficulty.csv`
- `<dataset>_density_counts.png`
- (optional) `<dataset>_qa_results.rds` (recommended to keep local; do not commit if sensitive)

## How to run
1) Open `qa_cat_mst_pipeline.R` and edit the `configs` list for your dataset(s):
- `college_id`: e.g., `wgu`, `ua`, `umgc`
- `wave`: e.g., `2017`, `2022`, `2023`, `2024`
- file paths for student/item RDS and (optional) mapping XLSX
- column mappings (ID, item id, score, attempt)
- filters (first attempt, minimum time, minimum items)

2) Run:
```r
source("qa_cat_mst_pipeline.R")
results <- lapply(configs, run_qa_pipeline, write_outputs = TRUE)
```
## Notes and limitations
- **CAT/MST data are sparse by design.** Low per-item response counts are expected and should be interpreted in the context of the routing design (planned missingness).
- **QA artifacts are investigation triggers, not universal pass/fail rules.** Thresholds such as `min_time_min` and `min_items` should be treated as policy choices informed by the assessment purpose and administration context.
- **Response-count plots reflect delivery patterns.** Multimodal distributions can indicate stage/module routing, content constraints, or differential exposure—not necessarily data problems.
- **Dimensionality checks (if added) must respect the design.** Conventional full-test EFA/CFA can be misleading under extreme planned missingness; consider design-aware approaches or proceed directly to IRT with appropriate model checks.
- **No validity claims from QA alone.** QA supports the technical quality of the dataset and the defensibility of downstream scoring/modeling; it does not, by itself, support interpretations or uses of test scores.

