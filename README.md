# News Events — Data Quality Assurance

This repo is ready to push to GitHub and submit. It includes **working code**, **sample data**, **SQLite DB**, **metrics**, and a **dashboard figure**.

## Quickstart (Windows)
1. Install Python 3.10+ and run:
   ```bat
   py -m pip install pandas numpy matplotlib
   ```
2. Run ETL (point `--input` to your JSONL shards folder):
   ```bat
   set SHARDS_DIR=%CD%\data\raw
   py src\etl_clean.py --input %SHARDS_DIR% --db db\news_dq.sqlite --sample-out data\clean\news_events_clean_sample_100k.csv
   ```
3. Compute metrics and generate chart:
   ```bat
   py src\dq_metrics.py --db db\news_dq.sqlite --logs logs --chart dashboard\dq_metrics_comparison_full.png
   ```
4. (Optional) Schedule daily monitoring:
   ```bat
   py src\monitor.py --db db\news_dq.sqlite
   ```

## Contents
- `src/etl_clean.py` — robust ETL (schema mapping, validation, cleaning, dedupe).
- `src/dq_metrics.py` — computes DQ metrics, exports CSVs and a PNG chart.
- `src/monitor.py` — daily checks + alert hooks (stdout or SMTP).
- `sql/schema.sql` — DDL for cleaned table and metrics.
- `db/news_dq.sqlite` — populated from the sample shards in `data/raw`.
- `logs/` — before/after/delta CSVs.
- `dashboard/` — `dq_metrics_comparison_full.png` figure.
- `data/raw/` — 3 JSONL shards (synthetic but realistic).
- `data/clean/news_events_clean_sample_100k.csv` — sample clean export.

## IDE Used
Visual Studio Code + Jupyter.
