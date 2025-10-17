# etl/etl.py
import pandas as pd
import sqlalchemy as sa

CSV_PATH = "data/311_2023_01.csv"
ENGINE = sa.create_engine("mysql+pymysql://root:rootpass@db:3306/nyc311", pool_recycle=3600)
CHUNKSIZE = 5000
TARGET_TABLE = "service_requests"

# CSV -> DB column mapping to match your schema
MAPPING = {
    "unique_key": "unique_key",
    "created_date": "created_date",
    "closed_date": "closed_date",
    "agency": "agency",
    "complaint_type": "complaint_type",
    "descriptor": "descriptor",
    "borough": "borough",
    "latitude": "latitude",
    "longitude": "longitude",
}

TARGET_ORDER = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "complaint_type",
    "descriptor",
    "borough",
    "latitude",
    "longitude",
]

def clean(df: pd.DataFrame) -> pd.DataFrame:
    # keep only mapping keys that exist in this chunk
    src_cols = [c for c in MAPPING.keys() if c in df.columns]
    if not src_cols:
        return pd.DataFrame(columns=TARGET_ORDER)

    df = df[src_cols].rename(columns={c: MAPPING[c] for c in src_cols})

    # types/cleaning
    if "created_date" in df:
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
    if "closed_date" in df:
        df["closed_date"] = pd.to_datetime(df["closed_date"], errors="coerce")
    if "borough" in df:
        df["borough"] = df["borough"].fillna("UNKNOWN")

    # lat/long to floats
    for col in ("latitude", "longitude"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # primary key must be present and int-like
    if "unique_key" in df:
        df = df.dropna(subset=["unique_key"])
        # NYC data can have large ints; cast safely
        df["unique_key"] = pd.to_numeric(df["unique_key"], errors="coerce").astype("Int64")

    # ensure columns order (missing ones will be added empty)
    for col in TARGET_ORDER:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[TARGET_ORDER]

    return df

def load():
    total = 0
    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNKSIZE, low_memory=False):
        df = clean(chunk)
        # drop rows with NA PK after cleaning
        df = df.dropna(subset=["unique_key"])
        if df.empty:
            continue
        with ENGINE.begin() as conn:
            # stage then idempotent insert (ignore duplicates on PK)
            df.to_sql("_stg_sr", conn, if_exists="replace", index=False)
            conn.exec_driver_sql(f"""
                INSERT IGNORE INTO {TARGET_TABLE} ({", ".join(TARGET_ORDER)})
                SELECT {", ".join(TARGET_ORDER)}
                FROM _stg_sr
            """)
            conn.exec_driver_sql("DROP TABLE _stg_sr")
        total += len(df)
        print(f"Inserted so far: {total}")
    print(f"Done. Inserted ~{total} rows.")

if __name__ == "__main__":
    load()
