"""
etl_pipeline.py - raw scanner logs + weather -> SQLite star schema.

    Extract    data/raw_deliveries.csv, data/weather/sydney_daily_max.csv
    Transform  clean, deduplicate, derive fields, join ambient temperature
    Load       warehouse/cold_chain.db (dim_suburbs, dim_trucks,
               fact_deliveries) using upserts, in a single transaction

Safe to run repeatedly: unchanged rows are not rewritten.

Usage:
    python etl_pipeline.py
    python etl_pipeline.py --rebuild   # replace all rows, e.g. after
                                       # regenerating the synthetic data
"""

import argparse
import logging
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from fetch_weather import SUBURB_COORDINATES  # one source of truth for suburbs

PROJECT_ROOT = Path(__file__).resolve().parent
RAW_PATH = PROJECT_ROOT / "data" / "raw_deliveries.csv"
WEATHER_PATH = PROJECT_ROOT / "data" / "weather" / "sydney_daily_max.csv"
DB_PATH = PROJECT_ROOT / "warehouse" / "cold_chain.db"

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
MAX_SAFE_TEMP_C = 5.0  # at or below this is compliant

RAW_COLUMNS = {
    "scan_id", "truck_id", "suburb", "product_type", "pickup_time",
    "delivery_time", "temp_reading", "temp_unit", "cargo_value_aud",
    "delivery_revenue_aud", "scanned_at",
}
WEATHER_COLUMNS = {"date", "suburb", "temp_max_c"}

# Columns loaded into the fact table (besides loaded_at)
FACT_COLUMNS = [
    "scan_id", "truck_key", "suburb_key", "delivery_date", "product_type",
    "pickup_time", "delivery_time", "delivery_duration_mins", "temp_celsius",
    "original_temp_unit", "is_spoiled", "cargo_value_aud",
    "delivery_revenue_aud", "ambient_temp_max_c", "scanned_at",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_suburbs (
    suburb_key   INTEGER PRIMARY KEY,
    suburb_name  TEXT NOT NULL UNIQUE,
    latitude     REAL NOT NULL,
    longitude    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_trucks (
    truck_key        INTEGER PRIMARY KEY,
    truck_id         TEXT NOT NULL UNIQUE,
    first_seen_date  TEXT NOT NULL
);

-- Grain: one row per delivery scan_id
CREATE TABLE IF NOT EXISTS fact_deliveries (
    scan_id                 TEXT NOT NULL PRIMARY KEY,
    truck_key               INTEGER NOT NULL REFERENCES dim_trucks (truck_key),
    suburb_key              INTEGER NOT NULL REFERENCES dim_suburbs (suburb_key),
    delivery_date           TEXT NOT NULL,
    product_type            TEXT,
    pickup_time             TEXT,
    delivery_time           TEXT,
    delivery_duration_mins  REAL CHECK (delivery_duration_mins >= 0),
    temp_celsius            REAL,
    original_temp_unit      TEXT,
    is_spoiled              INTEGER CHECK (is_spoiled IN (0, 1)),
    cargo_value_aud         REAL CHECK (cargo_value_aud >= 0),
    delivery_revenue_aud    REAL CHECK (delivery_revenue_aud >= 0),
    ambient_temp_max_c      REAL,
    scanned_at              TEXT NOT NULL,
    loaded_at               TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_deliveries (delivery_date);
CREATE INDEX IF NOT EXISTS idx_fact_truck ON fact_deliveries (truck_key);
CREATE INDEX IF NOT EXISTS idx_fact_suburb ON fact_deliveries (suburb_key);
"""

log = logging.getLogger("etl")


# --------------------------------------------------------------------------
# Extract
# --------------------------------------------------------------------------
def read_csv(path: Path, required: set, hint: str) -> pd.DataFrame:
    """Read every column as text so that all type conversion is explicit."""
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run {hint} first.")
    frame = pd.read_csv(path, dtype=str, encoding="utf-8")
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")
    return frame


# --------------------------------------------------------------------------
# Transform
# --------------------------------------------------------------------------
def clean_text(series: pd.Series) -> pd.Series:
    """Trim, collapse repeated spaces and turn blank strings into nulls."""
    cleaned = series.str.strip().str.replace(r"\s+", " ", regex=True)
    return cleaned.mask(cleaned == "")


def parse_timestamp(series: pd.Series) -> pd.Series:
    """Bad values such as 'invalid_date' become NaT instead of crashing."""
    return pd.to_datetime(series, format=TIMESTAMP_FORMAT, errors="coerce")


def non_negative(series: pd.Series) -> pd.Series:
    """Convert to numbers; negative or unreadable values become null."""
    numbers = pd.to_numeric(series, errors="coerce")
    return numbers.mask(numbers < 0)


def to_celsius(reading: pd.Series, unit: pd.Series) -> pd.Series:
    """Convert to Celsius. Unknown units give null rather than a guess."""
    value = pd.to_numeric(reading, errors="coerce")
    unit = unit.str.replace("°", "", regex=False).str.strip().str.upper()
    celsius = value.where(unit == "C")
    celsius = celsius.mask(unit == "F", (value - 32) * 5 / 9)
    # Round BEFORE applying the threshold to avoid floating point surprises
    return celsius.round(2)


def deduplicate(raw: pd.DataFrame, stats: dict) -> pd.DataFrame:
    """Drop null keys, exact copies, then superseded re-scans."""
    frame = raw.assign(scan_id=clean_text(raw["scan_id"]))

    before = len(frame)
    frame = frame.dropna(subset=["scan_id"])
    stats["null_scan_id_dropped"] = before - len(frame)

    before = len(frame)
    frame = frame.drop_duplicates()
    stats["exact_duplicates_dropped"] = before - len(frame)

    # For a repeated scan_id keep the most recent upload
    frame = frame.assign(scanned_at=parse_timestamp(frame["scanned_at"]))
    before = len(frame)
    frame = frame.sort_values(
        "scanned_at", kind="stable", na_position="first"
    ).drop_duplicates("scan_id", keep="last")
    stats["older_rescans_dropped"] = before - len(frame)
    return frame


def transform(raw: pd.DataFrame, weather: pd.DataFrame):
    """Return (clean_frame, data_quality_stats)."""
    stats = {"rows_read": len(raw)}
    df = deduplicate(raw, stats)

    df["truck_id"] = clean_text(df["truck_id"]).str.upper()
    df["suburb"] = clean_text(df["suburb"]).str.title()
    df["product_type"] = clean_text(df["product_type"])

    pickup = parse_timestamp(df["pickup_time"])
    delivered = parse_timestamp(df["delivery_time"])
    stats["rows_with_invalid_timestamp"] = int(
        (pickup.isna() | delivered.isna()).sum()
    )

    duration = (delivered - pickup).dt.total_seconds() / 60
    df["delivery_duration_mins"] = duration.mask(duration < 0).round(1)
    # Business date: pickup, else delivery, else upload time
    df["delivery_date"] = (
        pickup.fillna(delivered).fillna(df["scanned_at"]).dt.strftime("%Y-%m-%d")
    )
    df["pickup_time"] = pickup.dt.strftime(TIMESTAMP_FORMAT)
    df["delivery_time"] = delivered.dt.strftime(TIMESTAMP_FORMAT)

    df["temp_celsius"] = to_celsius(df["temp_reading"], df["temp_unit"])
    df["original_temp_unit"] = clean_text(df["temp_unit"])
    # Nullable integer: a missing temperature is "unknown", not "compliant"
    df["is_spoiled"] = (
        (df["temp_celsius"] > MAX_SAFE_TEMP_C)
        .astype("Int64")
        .mask(df["temp_celsius"].isna())
    )

    cargo = pd.to_numeric(df["cargo_value_aud"], errors="coerce")
    stats["negative_cargo_nulled"] = int((cargo < 0).sum())
    df["cargo_value_aud"] = non_negative(df["cargo_value_aud"])
    df["delivery_revenue_aud"] = non_negative(df["delivery_revenue_aud"])

    # Reject rows that cannot be linked to the dimensions
    usable = (
        df["truck_id"].notna()
        & df["suburb"].isin(list(SUBURB_COORDINATES))
        & df["scanned_at"].notna()
    )
    stats["rejected_bad_keys"] = int((~usable).sum())
    df = df[usable].copy()
    df["scanned_at"] = df["scanned_at"].dt.strftime(TIMESTAMP_FORMAT)

    ambient = pd.DataFrame({
        "delivery_date": weather["date"],
        "suburb": clean_text(weather["suburb"]).str.title(),
        "ambient_temp_max_c": pd.to_numeric(weather["temp_max_c"], errors="coerce"),
    })
    # validate= raises if duplicate weather rows would multiply deliveries
    df = df.merge(
        ambient, on=["delivery_date", "suburb"], how="left", validate="many_to_one"
    )

    stats["clean_rows"] = len(df)
    stats["spoiled_rows"] = int(df["is_spoiled"].sum())
    stats["rows_with_weather"] = int(df["ambient_temp_max_c"].notna().sum())
    return df, stats


# --------------------------------------------------------------------------
# Load
# --------------------------------------------------------------------------
def build_upsert_sql() -> str:
    """Insert new scans; update existing ones only if something changed."""
    updatable = [column for column in FACT_COLUMNS if column != "scan_id"]
    all_columns = FACT_COLUMNS + ["loaded_at"]
    assignments = ", ".join(f"{c} = excluded.{c}" for c in updatable + ["loaded_at"])
    changed = " OR ".join(f"fact_deliveries.{c} IS NOT excluded.{c}" for c in updatable)
    return f"""
        INSERT INTO fact_deliveries ({", ".join(all_columns)})
        VALUES ({", ".join(f":{c}" for c in all_columns)})
        ON CONFLICT (scan_id) DO UPDATE SET {assignments}
        WHERE excluded.scanned_at >= fact_deliveries.scanned_at  -- no stale data
          AND ({changed})
    """


def load(df: pd.DataFrame, db_path: Path, rebuild: bool = False) -> dict:
    """Upsert dimensions then facts. Any error rolls the whole load back."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    loaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    with closing(sqlite3.connect(db_path, timeout=30)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(SCHEMA)

        with conn:  # one transaction: commit on success, rollback on error
            if rebuild:
                for table in ("fact_deliveries", "dim_trucks", "dim_suburbs"):
                    conn.execute(f"DELETE FROM {table}")

            conn.executemany(
                """
                INSERT INTO dim_suburbs (suburb_name, latitude, longitude)
                VALUES (?, ?, ?)
                ON CONFLICT (suburb_name) DO UPDATE SET
                    latitude = excluded.latitude, longitude = excluded.longitude
                """,
                [(name, *coords) for name, coords in SUBURB_COORDINATES.items()],
            )

            first_seen = df.groupby("truck_id")["delivery_date"].min()
            conn.executemany(
                """
                INSERT INTO dim_trucks (truck_id, first_seen_date) VALUES (?, ?)
                ON CONFLICT (truck_id) DO UPDATE SET first_seen_date =
                    MIN(first_seen_date, excluded.first_seen_date)
                """,
                list(first_seen.items()),
            )

            # Swap business keys for surrogate keys
            truck_keys = dict(conn.execute("SELECT truck_id, truck_key FROM dim_trucks"))
            suburb_keys = dict(
                conn.execute("SELECT suburb_name, suburb_key FROM dim_suburbs")
            )
            facts = df.assign(
                truck_key=df["truck_id"].map(truck_keys),
                suburb_key=df["suburb"].map(suburb_keys),
                loaded_at=loaded_at,
            )[FACT_COLUMNS + ["loaded_at"]]

            # SQLite needs None, not NaN / NaT / <NA>
            facts = facts.astype(object).where(facts.notna(), None)
            cursor = conn.executemany(build_upsert_sql(), facts.to_dict("records"))
            changed = cursor.rowcount

        total = conn.execute("SELECT COUNT(*) FROM fact_deliveries").fetchone()[0]

    return {"rows_inserted_or_updated": changed, "warehouse_rows": total}


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def run_pipeline(raw_path: Path = RAW_PATH, weather_path: Path = WEATHER_PATH,
                 db_path: Path = DB_PATH, rebuild: bool = False) -> dict:
    """Run the full ETL and return a summary (also used by the dashboard)."""
    raw = read_csv(raw_path, RAW_COLUMNS, "generate_data.py")
    weather = read_csv(weather_path, WEATHER_COLUMNS, "fetch_weather.py")
    clean, stats = transform(raw, weather)
    if clean.empty:
        raise ValueError("No usable rows after cleaning; warehouse not modified.")
    stats.update(load(clean, db_path, rebuild))

    for name, value in stats.items():
        log.info("%-28s %s", name, value)
    missing = stats["clean_rows"] - stats["rows_with_weather"]
    if missing:
        log.warning("%d rows have no matching weather record", missing)
    return stats


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Run the cold-chain ETL.")
    parser.add_argument("--raw", type=Path, default=RAW_PATH)
    parser.add_argument("--weather", type=Path, default=WEATHER_PATH)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--rebuild", action="store_true",
                        help="replace all warehouse rows with this input")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run_pipeline(args.raw, args.weather, args.db, args.rebuild)
    except (OSError, ValueError, sqlite3.Error) as error:
        log.error("ETL failed: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
