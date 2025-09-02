#!/usr/bin/env python3
import argparse, re, json, sqlite3
from pathlib import Path
import pandas as pd, numpy as np

CANON_COLS = ["event_id","source","title","summary","url","published_at","ingested_at",
              "category","language","location","author","entities","sentiment","relevance_score"]

def to_snake(name: str) -> str:
    import re
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', str(name))
    name = re.sub(r'[\s\-]+', '_', name)
    return name.lower()

def map_columns(cols):
    import re
    mapping = {}
    candidates = {
        "event_id": [r"^event_?id$", r"^id$"],
        "title": [r"^title$"],
        "summary": [r"^summary$","^description$","^abstract$"],
        "url": [r"^url$","^link$"],
        "source": [r"^source(_name)?$", r"^publisher$","^provider$","^source$"],
        "category": [r"^category$","^topic$","^section$"],
        "language": [r"^language$","^lang$","^locale$"],
        "location": [r"^location$","^country(_code)?$","^geo$","^country$"],
        "author": [r"^author(s)?$","^byline$"],
        "entities": [r"^entities$","^tags$","^keywords$"],
        "published_at": [r"^published(_at|_time|_ts)?$", r"^publish(ed)?_date$","^date$","^created_at$","^publishdate$"],
        "ingested_at": [r"^ingest(ed)?(_at|_time|_ts)?$", r"^received_at$","^indexed_at$","^updated_at$","^ingestedat$"],
        "sentiment": [r"^sentiment$","^sentiment_score$"],
        "relevance_score": [r"^relevance(_score)?$","^score$"]
    }
    for standard, pats in candidates.items():
        for p in pats:
            hit = next((c for c in cols if re.match(p, c)), None)
            if hit:
                mapping[hit] = standard
                break
    return mapping

def is_valid_url(x):
    import re
    if not isinstance(x, str): return False
    return bool(re.match(r"^https?://[^\s/$.?#].[^\s]*$", x))

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.select_dtypes(include=["object"]).columns:
        out[c] = out[c].apply(lambda x: x.strip() if isinstance(x, str) else x)
    if "language" in out.columns:
        out["language"] = out["language"].astype(str).str.lower().replace({
            "english":"en","eng":"en","en-us":"en","en_us":"en","en-gb":"en"
        })
        mask = ~out["language"].str.match(r"^[a-z]{2}(-[a-z]{2})?$", na=False)
        out.loc[mask, "language"] = np.nan
    if "category" in out.columns:
        cat_map = {"biz":"business","business":"business","tech":"technology","politic":"politics",
                   "world news":"world","sci":"science","healthcare":"health","fin":"finance","economics":"economy"}
        out["category"] = out["category"].astype(str).str.lower().replace(cat_map)
        allowed = {"politics","business","technology","sports","entertainment","world","science","health","finance","economy"}
        out.loc[~out["category"].isin(allowed), "category"] = np.nan
    if "url" in out.columns:
        out.loc[~out["url"].apply(is_valid_url), "url"] = np.nan
    for col in ["published_at","ingested_at"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=True)
    if {"published_at","ingested_at"}.issubset(out.columns):
        swap = out["published_at"] > out["ingested_at"]
        out.loc[swap, ["published_at","ingested_at"]] = out.loc[swap, ["ingested_at","published_at"]].values
    for col, lo, hi in [("sentiment",-1,1), ("relevance_score",0,1)]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            out.loc[~out[col].between(lo,hi), col] = np.nan
    return out

def main(args):
    input_dir = Path(args.input)
    shards = sorted([p for p in input_dir.iterdir() if str(p).lower().endswith(".jsonl")])
    db_path = Path(args.db); db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(\"\"\"\nCREATE TABLE IF NOT EXISTS news_events_clean (\n    event_id TEXT,\n    source TEXT,\n    title TEXT,\n    summary TEXT,\n    url TEXT,\n    published_at TIMESTAMP,\n    ingested_at TIMESTAMP,\n    category TEXT,\n    language TEXT,\n    location TEXT,\n    author TEXT,\n    entities TEXT,\n    sentiment REAL,\n    relevance_score REAL\n);\n\"\"\")\n    sample_rows = []\n    for p in shards:\n        try:\n            df = pd.read_json(p, lines=True, dtype=False)\n        except ValueError:\n            recs = [json.loads(l) for l in p.read_text(encoding='utf-8').splitlines() if l.strip()]\n            df = pd.DataFrame.from_records(recs)\n        df.columns = [to_snake(c) for c in df.columns]\n        df = df.rename(columns=map_columns(df.columns))\n        for c in CANON_COLS:\n            if c not in df.columns:\n                df[c] = np.nan\n        if \"entities\" in df.columns:\n            df[\"entities\"] = df[\"entities\"].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list,dict)) else x)\n        df = clean_df(df)\n        if {\"title\",\"url\"}.issubset(df.columns):\n            df = df.drop_duplicates(subset=[\"title\",\"url\"])  \n        df = df[CANON_COLS]\n        df.to_sql(\"news_events_clean\", conn, if_exists=\"append\", index=False)\n        if len(sample_rows) < args.sample_max:\n            take = min(args.sample_max - len(sample_rows), len(df))\n            sample_rows.extend(df.head(take).to_dict(orient=\"records\"))\n    conn.commit(); conn.close()\n    if args.sample_out and sample_rows:\n        pd.DataFrame(sample_rows).to_csv(args.sample_out, index=False)\n\nif __name__ == \"__main__\":\n    import argparse\n    ap = argparse.ArgumentParser()\n    ap.add_argument(\"--input\", required=True)\n    ap.add_argument(\"--db\", default=\"db/news_dq.sqlite\")\n    ap.add_argument(\"--sample-out\", default=\"data/clean/news_events_clean_sample_100k.csv\")\n    ap.add_argument(\"--sample-max\", type=int, default=100000)\n    main(ap.parse_args())\n