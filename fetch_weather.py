"""
fetch_weather.py - daily maximum temperatures for Sydney delivery suburbs.

Source: Open-Meteo forecast API (no API key; free tier is for non-commercial
use; data licensed CC BY 4.0, so credit "Weather data by Open-Meteo.com").

Values are weather MODEL output for each suburb's coordinates, not weather
station observations. Today's value is a forecast until the day is over.

Writes:
    data/weather/sydney_daily_max.csv   (read by generator and ETL)
    data/weather/sydney_daily_max.json

Each run upserts on (date, suburb), so history older than the requested
window is kept.

Usage:
    python fetch_weather.py
    python fetch_weather.py --past-days 92
"""

import argparse
import csv
import json
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "weather"
CSV_NAME = "sydney_daily_max.csv"
JSON_NAME = "sydney_daily_max.json"

API_URL = "https://api.open-meteo.com/v1/forecast"
TIMEZONE = "Australia/Sydney"
REQUEST_TIMEOUT_SECONDS = 30
MAX_PAST_DAYS = 92  # API limit for the past_days parameter

# Approximate suburb centres: (latitude, longitude)
SUBURB_COORDINATES = {
    "Parramatta": (-33.8150, 151.0011),
    "Pendle Hill": (-33.8010, 150.9560),
    "Blacktown": (-33.7710, 150.9063),
    "Penrith": (-33.7511, 150.6942),
    "Liverpool": (-33.9200, 150.9239),
}

COLUMNS = ["date", "suburb", "temp_max_c", "data_type", "fetched_at"]


class WeatherFetchError(Exception):
    """Raised when the API cannot supply usable data."""


def build_session() -> requests.Session:
    """HTTP session that retries temporary failures with exponential backoff."""
    retry = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,  # let us report the final response ourselves
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def request_weather(past_days: int) -> list:
    """Fetch all suburbs in one call. Returns one payload per suburb."""
    latitudes, longitudes = zip(*SUBURB_COORDINATES.values())
    params = {
        "latitude": ",".join(map(str, latitudes)),
        "longitude": ",".join(map(str, longitudes)),
        "daily": "temperature_2m_max",
        "timezone": TIMEZONE,  # day boundaries in Sydney local time
        "past_days": past_days,
        "forecast_days": 1,  # today only
    }
    try:
        with build_session() as session:
            response = session.get(
                API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
        if not response.ok:
            raise WeatherFetchError(
                f"HTTP {response.status_code}: {response.text[:200]}"
            )
        payload = response.json()
    except (requests.RequestException, ValueError) as error:
        raise WeatherFetchError(f"Request failed: {error}") from error

    # A list is returned for several locations, a single object for one.
    return payload if isinstance(payload, list) else [payload]


def parse_payload(payload: list, today: date, fetched_at: str) -> list:
    """Validate the API response and flatten it to one row per suburb-day."""
    if len(payload) != len(SUBURB_COORDINATES):
        raise WeatherFetchError(
            f"Expected {len(SUBURB_COORDINATES)} locations, got {len(payload)}"
        )

    records = []
    # Results come back in the same order as the coordinates we sent.
    for suburb, location in zip(SUBURB_COORDINATES, payload):
        try:
            unit = location["daily_units"]["temperature_2m_max"]
            days = location["daily"]["time"]
            temps = location["daily"]["temperature_2m_max"]
        except (KeyError, TypeError) as error:
            raise WeatherFetchError(f"{suburb}: missing field {error}") from error

        if unit != "°C":
            raise WeatherFetchError(f"{suburb}: unexpected unit {unit!r}")
        if len(days) != len(temps):
            raise WeatherFetchError(f"{suburb}: dates and values do not align")

        suburb_records = [
            {
                "date": day,
                "suburb": suburb,
                "temp_max_c": float(temp),
                "data_type": "past" if date.fromisoformat(day) < today else "forecast",
                "fetched_at": fetched_at,
            }
            for day, temp in zip(days, temps)
            if temp is not None  # never overwrite good history with a gap
        ]
        if not suburb_records:
            raise WeatherFetchError(f"{suburb}: no temperature values returned")
        records.extend(suburb_records)
    return records


def read_existing(csv_path: Path) -> list:
    if not csv_path.exists():
        return []
    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        row["temp_max_c"] = float(row["temp_max_c"])
    return rows


def upsert(existing: list, new: list) -> list:
    """Merge on (date, suburb). Newly fetched rows replace older ones."""
    merged = {(row["date"], row["suburb"]): row for row in existing}
    merged.update({(row["date"], row["suburb"]): row for row in new})
    return [merged[key] for key in sorted(merged)]


def save(records: list, output_dir: Path) -> None:
    """Write to temporary files, then rename, so no file is left half-written."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / CSV_NAME
    json_path = output_dir / JSON_NAME

    csv_temp = csv_path.with_suffix(".csv.tmp")
    with csv_temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(records)

    json_temp = json_path.with_suffix(".json.tmp")
    document = {
        "source": "Weather data by Open-Meteo.com (CC BY 4.0)",
        "timezone": TIMEZONE,
        "records": records,
    }
    json_temp.write_text(json.dumps(document, indent=2), encoding="utf-8")

    csv_temp.replace(csv_path)
    json_temp.replace(json_path)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Sydney daily max temps.")
    parser.add_argument("--past-days", type=int, default=35,
                        help=f"days of history, 1-{MAX_PAST_DAYS} (default: 35)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)
    if not 1 <= args.past_days <= MAX_PAST_DAYS:
        parser.error(f"--past-days must be between 1 and {MAX_PAST_DAYS}")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    now = datetime.now(ZoneInfo(TIMEZONE))
    try:
        payload = request_weather(args.past_days)
        new_records = parse_payload(
            payload, now.date(), now.isoformat(timespec="seconds")
        )
    except WeatherFetchError as error:
        # Existing files are untouched, so the ETL can use the last good data.
        print(f"Weather fetch failed: {error}", file=sys.stderr)
        return 1

    records = upsert(read_existing(args.output_dir / CSV_NAME), new_records)
    save(records, args.output_dir)

    hot = sum(row["temp_max_c"] >= 30 for row in records)
    print(f"Fetched {len(new_records)} rows; file now holds {len(records)}")
    print(f"Date range: {records[0]['date']} to {records[-1]['date']}")
    print(f"Suburb-days at 30 C or above: {hot}")
    print(f"Saved to {args.output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
