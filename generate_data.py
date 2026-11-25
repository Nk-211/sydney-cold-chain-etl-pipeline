"""
generate_data.py - synthetic raw scanner logs for a Sydney cold-chain operator.

Writes data/raw_deliveries.csv with deliberately injected data quality issues
that etl_pipeline.py must handle. All data is SYNTHETIC.

If a weather file exists (written later by fetch_weather.py, with columns
date, suburb, temp_max_c), the chance of a cooling breach is raised on days
of 30 C or more. That relationship is a modelling assumption planted here,
not a real-world finding.

Usage:
    python generate_data.py
    python generate_data.py --rows 800 --end-date 2026-11-25 --seed 7
"""

import argparse
import csv
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "raw_deliveries.csv"
DEFAULT_WEATHER = PROJECT_ROOT / "data" / "weather" / "sydney_daily_max.csv"

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
HOT_DAY_THRESHOLD_C = 30.0
HOT_DAY_BREACH_UPLIFT = 0.25  # added to a truck's breach probability

# suburb -> (min trip minutes, max trip minutes, base freight fee AUD)
SUBURBS = {
    "Parramatta": (30, 75, 120),
    "Pendle Hill": (35, 80, 130),
    "Blacktown": (40, 95, 150),
    "Penrith": (60, 140, 220),
    "Liverpool": (45, 110, 170),
}

# truck_id -> baseline probability of a breach. Two trucks have weak cooling
# units so the "worst performing truck" analysis has a signal to find.
TRUCKS = {
    "TRK-01": 0.03, "TRK-02": 0.04, "TRK-03": 0.18, "TRK-04": 0.03,
    "TRK-05": 0.05, "TRK-06": 0.04, "TRK-07": 0.14, "TRK-08": 0.03,
}

PRODUCTS = [
    "Fresh Whole Chicken", "Chicken Breast Fillets", "Chicken Thigh Fillets",
    "Marinated Poultry", "Chilled Ready Meals", "Chilled Dairy",
]

COLUMNS = [
    "scan_id", "truck_id", "suburb", "product_type", "pickup_time",
    "delivery_time", "temp_reading", "temp_unit", "cargo_value_aud",
    "delivery_revenue_aud", "scanned_at",
]

# Share of base rows affected by each row-level defect. The groups never
# overlap, so the ETL results can be reconciled exactly against these counts.
DEFECT_RATES = {
    "null_scan_id": 0.020,
    "invalid_timestamp": 0.030,
    "negative_cargo": 0.025,
    "rescan": 0.015,
    "exact_duplicate": 0.035,
}
MESSY_SUBURB_RATE = 0.30
FAHRENHEIT_RATE = 0.20


def load_hot_days(weather_path: Path) -> set:
    """Return {(date_str, suburb)} pairs at or above the hot-day threshold."""
    if not weather_path.exists():
        return set()
    with weather_path.open(newline="", encoding="utf-8") as handle:
        return {
            (row["date"], row["suburb"])
            for row in csv.DictReader(handle)
            if row["temp_max_c"] and float(row["temp_max_c"]) >= HOT_DAY_THRESHOLD_C
        }


def build_clean_records(rng, n_rows, start_date, n_days, hot_days) -> list:
    """Create well-formed records, ordered by pickup time."""
    trips = []
    for i in range(n_rows):
        day = start_date + timedelta(days=i % n_days)  # every day gets data
        pickup = datetime.combine(day, time(4, 0)) + timedelta(
            seconds=rng.randrange(11 * 3600)  # between 04:00 and 15:00
        )
        trips.append(pickup)
    trips.sort()

    records = []
    for number, pickup in enumerate(trips, start=1):
        suburb = rng.choice(list(SUBURBS))
        truck_id = rng.choice(list(TRUCKS))
        min_trip, max_trip, base_fee = SUBURBS[suburb]
        delivered = pickup + timedelta(minutes=rng.randint(min_trip, max_trip))

        breach_probability = TRUCKS[truck_id]
        if (pickup.date().isoformat(), suburb) in hot_days:
            breach_probability += HOT_DAY_BREACH_UPLIFT

        if rng.random() < breach_probability:
            temp_c = round(rng.uniform(5.1, 11.0), 1)
        else:
            temp_c = round(rng.uniform(0.5, 5.0), 1)  # 5.0 is still compliant

        records.append({
            "scan_id": f"SCN-{number:06d}",
            "truck_id": truck_id,
            "suburb": suburb,
            "product_type": rng.choice(PRODUCTS),
            "pickup_time": pickup.strftime(TIMESTAMP_FORMAT),
            "delivery_time": delivered.strftime(TIMESTAMP_FORMAT),
            "temp_reading": temp_c,
            "temp_unit": "°C",
            "cargo_value_aud": round(rng.uniform(800, 9000), 2),
            "delivery_revenue_aud": round(base_fee * rng.uniform(0.85, 1.35), 2),
            "scanned_at": (
                delivered + timedelta(seconds=rng.randint(5, 180))
            ).strftime(TIMESTAMP_FORMAT),
        })
    return records


def messy_suburb(rng, name: str) -> str:
    """Return a badly formatted version of a suburb name."""
    variants = [
        name.lower(),
        name.upper(),
        f"  {name} ",
        f"{name.lower()}   ",
        name.swapcase(),
    ]
    if " " in name:
        variants.append(name.replace(" ", "  "))  # double internal space
    return rng.choice(variants)


def apply_format_noise(rng, records: list) -> dict:
    """Mix suburb formatting and temperature units (fixable, not droppable)."""
    counts = {"messy_suburb": 0, "fahrenheit": 0}
    for record in records:
        if rng.random() < MESSY_SUBURB_RATE:
            record["suburb"] = messy_suburb(rng, record["suburb"])
            counts["messy_suburb"] += 1

        if rng.random() < FAHRENHEIT_RATE:
            record["temp_reading"] = round(record["temp_reading"] * 9 / 5 + 32, 1)
            record["temp_unit"] = rng.choice(["°F", "F"])
            counts["fahrenheit"] += 1
        else:
            record["temp_unit"] = rng.choice(["°C", "°C", "°C", "C"])
    return counts


def inject_defects(rng, records: list):
    """Inject row-level defects. Returns (all_rows, counts)."""
    counts = {name: round(len(records) * rate) for name, rate in DEFECT_RATES.items()}

    # Slice one shuffled index list so each row gets at most one defect.
    indexes = list(range(len(records)))
    rng.shuffle(indexes)
    groups, cursor = {}, 0
    for name, size in counts.items():
        groups[name] = indexes[cursor:cursor + size]
        cursor += size

    for i in groups["null_scan_id"]:
        records[i]["scan_id"] = ""

    for i in groups["invalid_timestamp"]:
        field = rng.choice(["pickup_time", "delivery_time"])
        records[i][field] = "invalid_date"

    for i in groups["negative_cargo"]:
        records[i]["cargo_value_aud"] = -records[i]["cargo_value_aud"]

    extra_rows = []

    # Re-scan: same scan_id, corrected delivery time, later scanned_at.
    # The ETL should keep the row with the latest scanned_at.
    for i in groups["rescan"]:
        rescan = dict(records[i])
        delay = timedelta(minutes=rng.randint(4, 15))
        for field in ("delivery_time", "scanned_at"):
            original = datetime.strptime(rescan[field], TIMESTAMP_FORMAT)
            rescan[field] = (original + delay).strftime(TIMESTAMP_FORMAT)
        extra_rows.append(rescan)

    # Exact duplicate: the scanner uploaded the identical row twice.
    extra_rows.extend(dict(records[i]) for i in groups["exact_duplicate"])

    all_rows = records + extra_rows
    all_rows.sort(key=lambda row: row["scanned_at"])  # arrival order
    return all_rows, counts


def write_csv(rows: list, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def parse_args(argv=None) -> argparse.Namespace:
    yesterday = datetime.now(ZoneInfo("Australia/Sydney")).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Generate raw delivery logs.")
    parser.add_argument("--rows", type=int, default=600, help="unique base records")
    parser.add_argument("--days", type=int, default=30, help="days of history")
    parser.add_argument("--end-date", type=date.fromisoformat, default=yesterday,
                        help="last delivery date, YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--weather", type=Path, default=DEFAULT_WEATHER)
    args = parser.parse_args(argv)
    if args.rows < 500:
        parser.error("--rows must be at least 500")
    if args.days < 1:
        parser.error("--days must be at least 1")
    return args


def main(argv=None) -> None:
    args = parse_args(argv)
    rng = random.Random(args.seed)  # same seed + inputs = same file
    start_date = args.end_date - timedelta(days=args.days - 1)
    hot_days = load_hot_days(args.weather)

    records = build_clean_records(rng, args.rows, start_date, args.days, hot_days)
    noise_counts = apply_format_noise(rng, records)
    rows, defect_counts = inject_defects(rng, records)
    write_csv(rows, args.output)

    print(f"Wrote {len(rows)} rows to {args.output}")
    print(f"Date range: {start_date} to {args.end_date}")
    if args.weather.exists():
        print(f"Weather file found: {len(hot_days)} hot suburb-days applied")
    else:
        print("No weather file yet: no heat effect applied")
    print("Injected issues:")
    for name, count in {**defect_counts, **noise_counts}.items():
        print(f"  {name:<18} {count}")
    print(f"Expected rows after ETL: {args.rows - defect_counts['null_scan_id']}")


if __name__ == "__main__":
    main()
