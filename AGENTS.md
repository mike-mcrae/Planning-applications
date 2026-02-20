# Repository Guidelines

## Project Structure & Module Organization
- `0. data/`: source datasets (CSV/XLSX) used as scrape input.
- `1. scripts/`: operational pipeline scripts, shell launchers, logs, and outputs.
- `1. scripts/outputs/`: worker CSVs, rerun inputs, and merged final dataset.
- `1. scripts/downloads/`: downloaded observation PDFs.
- `1. scripts/logs/`: per-worker runtime logs.
- `1. scripts/old/`: legacy scripts/artifacts kept for reference; do not extend this folder for new work.

Core flow is numeric: `0.scrape.py` -> `1.collect_failures.py` -> `2a/2b reruns` -> `3a.merge_outputs.py` (optional map check via `4.testing_map.py`).

## Build, Test, and Development Commands
Run from `1. scripts/` unless noted.

- `python3 0.tester.py`: quick smoke test against known application numbers.
- `bash launch.sh`: start parallel scraping workers in `screen` sessions.
- `bash kill.sh`: stop `obs_worker_*` sessions.
- `python3 1.collect_failures.py`: generate page/download failure lists.
- `python3 2a.rerun_failed_observations.py` and `python3 2b.rerun_download_failures.py`: retry failures.
- `python3 3a.merge_outputs.py`: build `outputs/third_party_obs_merged.csv`.
- `python3 4.testing_map.py`: render `dublin_city_objections_map.html` for spot checks.

Environment setup (root): `python3 -m venv .venv && source .venv/bin/activate && pip install pandas requests playwright pyproj folium && playwright install chromium`.

## Coding Style & Naming Conventions
- Python, 4-space indentation, `snake_case` for variables/functions, `UPPER_CASE` for constants.
- Keep scripts single-purpose and numbered by pipeline stage (`N.description.py`).
- Prefer `pathlib.Path` for file paths and append-only CSV writes for resumable jobs.
- Preserve existing log naming patterns like `third_party_obs_worker_<id>.log`.

## Testing Guidelines
- Primary validation is script-level smoke/integration testing (no formal `pytest` suite currently).
- Before large runs, execute `python3 0.tester.py` and confirm PDFs appear in `downloads_test/`.
- After reruns/merge, verify row counts and failure reductions in `1. scripts/outputs/`.

## Commit & Pull Request Guidelines
- No Git history is available in this directory snapshot; use clear imperative commits (for example, `scripts: harden download retry backoff`).
- Keep commits scoped to one pipeline concern (scrape, rerun, merge, or QA).
- PRs should include: purpose, scripts changed, commands run, and before/after output evidence (row counts or sample records).
- If output schemas change, document column impacts explicitly in the PR description.
