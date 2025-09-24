import os
import argparse
import time
import hashlib
import math
from typing import Dict, Any, Iterable, List, Optional

import pandas as pd
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError


# =========================
# Config tuned to your CSV
# =========================
# cleaned_IoT_data.csv columns:
# ts, device, co, humidity, light, lpg, motion, smoke, temp

DEVICE_COLS = ("device",)            # exact match
TIME_COLS = ("ts",)                  # exact match
NUMERIC_KEYS = ("temp", "humidity", "co", "smoke", "lpg")
BOOL_KEYS = ("light", "motion")

# Default ID uses device + ts (seconds) to make re-runs idempotent
ID_KEYS = ("device", "ts")


# ================
# Helper functions
# ================
def normalize(name: str) -> str:
    # Keep it simple but resilient: lowercase + underscores
    return name.strip().lower().replace(" ", "_")


def normalize_columns(cols: Iterable[str]) -> List[str]:
    return [normalize(c) for c in cols]


def coerce_bool(x):
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
    try:
        return float(x)
    except Exception:
        return None


def parse_ts_to_timestamp(val, epoch_unit: str = "auto") -> Optional[pd.Timestamp]:
    """
    Parse numeric epoch seconds/milliseconds into UTC timestamp.
    """
    if val is None or str(val).strip() == "":
        return None
    try:
        f = float(val)
        unit = "s"
        if epoch_unit == "ms":
            unit = "ms"
        elif epoch_unit == "auto":
            if f > 1_000_000_000_000:  # looks like ms
                unit = "ms"
        return pd.to_datetime(int(f), unit=unit, utc=True)
    except Exception:
        return None


def build_id(device: str, ts_seconds: Optional[int]) -> Optional[str]:
    if not device or ts_seconds is None:
        return None
    base = f"{device}|{ts_seconds}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


# =================
# Row transformation
# =================
def row_to_doc(row: pd.Series, epoch_unit: str, keep_raw: bool) -> Dict[str, Any]:
    # columns are normalized to lowercase underscores already
    device = str(row.get("device")).strip() if pd.notnull(row.get("device")) else "unknown"

    ts = parse_ts_to_timestamp(row.get("ts"), epoch_unit=epoch_unit)
    ts_seconds = int(ts.timestamp()) if ts is not None and pd.notna(ts) else None

    doc: Dict[str, Any] = {
        "_id": build_id(device, ts_seconds),
        "device": device,
        "timestamp": ts.to_pydatetime() if ts is not None and pd.notna(ts) else None,
    }

    # numeric fields
    for k in NUMERIC_KEYS:
        if k in row:
            v = coerce_float(row.get(k))
            if v is not None:
                doc[k] = v

    # boolean fields
    for k in BOOL_KEYS:
        if k in row:
            vb = coerce_bool(row.get(k))
            if vb is not None:
                doc[k] = vb

    if keep_raw:
        exclude = set(["_id", "device", "timestamp", "ts"] + list(NUMERIC_KEYS) + list(BOOL_KEYS))
        raw = {}
        for k, v in row.items():
            if k not in exclude:
                if isinstance(v, float) and math.isnan(v):
                    continue
                raw[k] = v
        if raw:
            doc["raw"] = raw

    # Drop Nones for cleanliness
    return {k: v for k, v in doc.items() if v is not None}


# ============
# Batch loader
# ============
def iter_batches(df_iter: Iterable[pd.DataFrame]):
    for df in df_iter:
        yield df


def load_csv_in_batches(csv_path: str,
                        client: MongoClient,
                        db_name: str,
                        coll_name: str,
                        chunk_size: int,
                        sep: str,
                        encoding: str,
                        epoch_unit: str,
                        keep_raw: bool):
    db = client[db_name]
    coll = db[coll_name]

    total_rows = total_inserted = total_duplicates = 0

    df_iter = pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        dtype=str,                 # read as strings; we coerce types ourselves
        keep_default_na=False,
        na_values=["", "NA", "NaN", "null", "None"],
        sep=sep,
        encoding=encoding,
        on_bad_lines="skip",
        low_memory=False,
    )

    for chunk_idx, df in enumerate(iter_batches(df_iter), start=1):
        df.columns = normalize_columns(df.columns)

        # Ensure our expected columns exist after normalization
        required = {"ts", "device"}
        missing = [c for c in required if c not in df.columns]
        if missing:
            print(f"[Chunk {chunk_idx}] Missing required columns: {missing}. Skipping chunk.")
            continue

        ops: List[InsertOne] = []
        for _, row in df.iterrows():
            doc = row_to_doc(row, epoch_unit=epoch_unit, keep_raw=keep_raw)
            if not doc.get("_id"):
                # If device or ts missing, skip row
                continue
            ops.append(InsertOne(doc))

        total_rows += len(df)
        if not ops:
            print(f"[Chunk {chunk_idx}] No valid documents to insert.")
            continue

        try:
            result = coll.bulk_write(ops, ordered=False)
            inserted = getattr(result, "inserted_count", None) or len(ops)
            total_inserted += inserted
            print(f"[Chunk {chunk_idx}] Inserted {inserted}/{len(ops)} docs.")
        except BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            dup = sum(1 for e in write_errors if e.get("code") == 11000)
            total_duplicates += dup
            inserted = len(ops) - len(write_errors)
            total_inserted += inserted
            print(f"[Chunk {chunk_idx}] Inserted {inserted}/{len(ops)} docs; {dup} duplicates skipped.")

    print("=" * 60)
    print(f"TOTAL rows seen:      {total_rows}")
    print(f"TOTAL inserted:       {total_inserted}")
    print(f"TOTAL duplicates:     {total_duplicates}")
    print("=" * 60)


# ===
# CLI
# ===
def main():
    p = argparse.ArgumentParser(description="Batch-load cleaned_IoT_data.csv into MongoDB")

    # IO / connection
    p.add_argument("--file", required=True, help="Path to CSV file (e.g., data/cleaned_IoT_data.csv)")
    p.add_argument("--mongodb-uri", default=os.getenv("MONGODB_URI", "mongodb://localhost:27017/?authSource=admin"))
    p.add_argument("--db", default=os.getenv("MONGODB_DB", "iot"))
    p.add_argument("--collection", default=os.getenv("MONGODB_COLLECTION", "measurements"))

    # CSV reading
    p.add_argument("--chunk-size", type=int, default=int(os.getenv("CHUNK_SIZE", "50000")))
    p.add_argument("--sep", default=os.getenv("CSV_SEP", ","), help="CSV separator, e.g. ',' or ';'")
    p.add_argument("--encoding", default=os.getenv("CSV_ENCODING", "utf-8"))

    # Epoch unit for ts (seconds/ms/auto)
    p.add_argument("--epoch-unit", choices=["s", "ms", "auto"], default=os.getenv("EPOCH_UNIT", "auto"))

    # Keep non-mapped columns
    p.add_argument("--keep-raw", action="store_true", help="Store remaining columns under 'raw'")

    args = p.parse_args()

    print(f"Connecting to MongoDB at {args.mongodb_uri} ...")
    client = MongoClient(args.mongodb_uri)

    t0 = time.time()
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
    print(f"Done in {time.time() - t0:.2f}s.")


if __name__ == "__main__":
    main()
 