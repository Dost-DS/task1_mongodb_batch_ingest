"""
Enhancements:
- Structured logging (console + file)
- Retry mechanism for transient database errors
- Idempotent batch inserts with unique IDs
- Lightweight metrics collection (JSON summary)
- Chunked data loading for memory efficiency
"""

import os
import argparse
import time
import math
import hashlib
import logging
import json
from typing import Dict, Any, Iterable, List, Optional

import pandas as pd
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, PyMongoError


# =====================================================
# Setup Logging and Metrics Directories
# =====================================================
LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "ingestion.log")),
        logging.StreamHandler()
    ]
)


# =====================================================
# Configuration Constants
# =====================================================
NUMERIC_KEYS = ("temp", "humidity", "co", "smoke", "lpg")
BOOL_KEYS = ("light", "motion")
MAX_RETRIES = 3  # Retry attempts for transient errors


# =====================================================
# Helper Functions
# =====================================================
def normalize(name: str) -> str:
    """Normalize column names to lowercase and underscores."""
    return name.strip().lower().replace(" ", "_")


def normalize_columns(cols: Iterable[str]) -> List[str]:
    return [normalize(c) for c in cols]


def coerce_bool(x):
    """Convert values to boolean if possible."""
    if pd.isna(x):
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x)):
        return bool(int(x))
    s = str(x).strip().lower()
    if s in {"true", "t", "1", "yes", "y"}:
        return True
    if s in {"false", "f", "0", "no", "n"}:
        return False
    return None


def coerce_float(x):
    """Convert numeric-like strings to float, return None if invalid."""
    try:
        return float(x)
    except Exception:
        return None


def parse_ts_to_timestamp(val, epoch_unit: str = "auto") -> Optional[pd.Timestamp]:
    """Parse numeric epoch timestamp into UTC datetime."""
    if val is None or str(val).strip() == "":
        return None
    try:
        f = float(val)
        unit = "s" if f < 1_000_000_000_000 else "ms" if epoch_unit == "auto" else epoch_unit
        return pd.to_datetime(int(f), unit=unit, utc=True)
    except Exception:
        return None


def build_id(device: str, ts_seconds: Optional[int]) -> Optional[str]:
    """Generate unique hash ID for each record (device + timestamp)."""
    if not device or ts_seconds is None:
        return None
    base = f"{device}|{ts_seconds}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


# =====================================================
# Row Transformation
# =====================================================
def row_to_doc(row: pd.Series, epoch_unit: str, keep_raw: bool) -> Dict[str, Any]:
    """Convert a CSV row into a MongoDB document."""
    device = str(row.get("device")).strip() if pd.notnull(row.get("device")) else "unknown"
    ts = parse_ts_to_timestamp(row.get("ts"), epoch_unit)
    ts_seconds = int(ts.timestamp()) if ts is not None else None

    doc = {
        "_id": build_id(device, ts_seconds),
        "device": device,
        "timestamp": ts.to_pydatetime() if ts is not None else None,
    }

    for k in NUMERIC_KEYS:
        if k in row:
            v = coerce_float(row.get(k))
            if v is not None:
                doc[k] = v

    for k in BOOL_KEYS:
        if k in row:
            vb = coerce_bool(row.get(k))
            if vb is not None:
                doc[k] = vb

    if keep_raw:
        exclude = {"_id", "device", "timestamp", "ts", *NUMERIC_KEYS, *BOOL_KEYS}
        raw = {k: v for k, v in row.items() if k not in exclude and pd.notna(v)}
        if raw:
            doc["raw"] = raw

    return {k: v for k, v in doc.items() if v is not None}


# =====================================================
# Batch Processing with Fault Tolerance
# =====================================================
def load_csv_in_batches(csv_path: str,
                        client: MongoClient,
                        db_name: str,
                        coll_name: str,
                        chunk_size: int,
                        sep: str,
                        encoding: str,
                        epoch_unit: str,
                        keep_raw: bool):
    """Load data in chunks and insert into MongoDB with retries and metrics."""
    db = client[db_name]
    coll = db[coll_name]

    total_rows = total_inserted = total_duplicates = 0
    start_time = time.time()

    df_iter = pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        dtype=str,
        keep_default_na=False,
        na_values=["", "NA", "NaN", "null", "None"],
        sep=sep,
        encoding=encoding,
        on_bad_lines="skip",
        low_memory=False,
    )

    for chunk_idx, df in enumerate(df_iter, start=1):
        df.columns = normalize_columns(df.columns)
        if not {"ts", "device"}.issubset(df.columns):
            logging.error(f"[Chunk {chunk_idx}] Missing required columns. Skipping.")
            continue

        ops: List[InsertOne] = []
        for _, row in df.iterrows():
            doc = row_to_doc(row, epoch_unit, keep_raw)
            if doc.get("_id"):
                ops.append(InsertOne(doc))

        total_rows += len(df)
        if not ops:
            logging.warning(f"[Chunk {chunk_idx}] No valid records to insert.")
            continue

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = coll.bulk_write(ops, ordered=False)
                inserted = getattr(result, "inserted_count", len(ops))
                total_inserted += inserted
                logging.info(f"[Chunk {chunk_idx}] Inserted {inserted}/{len(ops)} docs.")
                break
            except BulkWriteError as bwe:
                write_errors = bwe.details.get("writeErrors", [])
                dup = sum(1 for e in write_errors if e.get("code") == 11000)
                total_duplicates += dup
                inserted = len(ops) - len(write_errors)
                total_inserted += inserted
                logging.warning(f"[Chunk {chunk_idx}] Inserted {inserted}/{len(ops)} docs; {dup} duplicates skipped.")
                break
            except PyMongoError as e:
                logging.error(f"[Chunk {chunk_idx}] Attempt {attempt} failed: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(2)
                else:
                    logging.critical(f"[Chunk {chunk_idx}] Failed after {MAX_RETRIES} attempts.")
            except Exception as e:
                logging.exception(f"[Chunk {chunk_idx}] Unexpected error: {e}")
                break

    duration = round(time.time() - start_time, 2)

    # Save metrics to JSON
    metrics = {
        "rows_seen": total_rows,
        "inserted": total_inserted,
        "duplicates": total_duplicates,
        "duration_sec": duration,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    }
    with open(os.path.join(LOG_DIR, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=4)

    logging.info("=" * 60)
    logging.info(f"TOTAL rows seen:      {total_rows}")
    logging.info(f"TOTAL inserted:       {total_inserted}")
    logging.info(f"TOTAL duplicates:     {total_duplicates}")
    logging.info(f"TOTAL duration (sec): {duration}")
    logging.info("Metrics saved to logs/metrics.json")
    logging.info("=" * 60)


# =====================================================
# Command-Line Interface
# =====================================================
def main():
    parser = argparse.ArgumentParser(description="Batch-load cleaned IoT data into MongoDB")

    parser.add_argument("--file", required=True, help="Path to CSV file (e.g., data/cleaned_IoT_data.csv)")
    parser.add_argument("--mongodb-uri", default=os.getenv("MONGODB_URI", "mongodb://localhost:27017/?authSource=admin"))
    parser.add_argument("--db", default=os.getenv("MONGODB_DB", "iot"))
    parser.add_argument("--collection", default=os.getenv("MONGODB_COLLECTION", "measurements"))
    parser.add_argument("--chunk-size", type=int, default=int(os.getenv("CHUNK_SIZE", "50000")))
    parser.add_argument("--sep", default=os.getenv("CSV_SEP", ","), help="CSV separator")
    parser.add_argument("--encoding", default=os.getenv("CSV_ENCODING", "utf-8"))
    parser.add_argument("--epoch-unit", choices=["s", "ms", "auto"], default=os.getenv("EPOCH_UNIT", "auto"))
    parser.add_argument("--keep-raw", action="store_true", help="Include unmapped columns in a 'raw' field")

    args = parser.parse_args()

    logging.info("Starting IoT batch ingestion process...")
    logging.info(f"Connecting to MongoDB at {args.mongodb_uri}")
    client = MongoClient(args.mongodb_uri)

    load_csv_in_batches(
        csv_path=args.file,
        client=client,
        db_name=args.db,
        coll_name=args.collection,
        chunk_size=args.chunk_size,
        sep=args.sep,
        encoding=args.encoding,
        epoch_unit=args.epoch_unit,
        keep_raw=args.keep_raw,
    )

    logging.info("Ingestion process completed successfully.")


if __name__ == "__main__":
    main()
