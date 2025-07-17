import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import httpx

# Constants
BASE_URL = "https://opendata.aemet.es/opendata/api"
BATCH_SIZE_DAYS = 15


def create_client(api_token: str) -> httpx.Client:
    """Create configured httpx client with API token."""
    return httpx.Client(
        timeout=httpx.Timeout(60.0),
        params={"api_key": api_token},
        follow_redirects=True,
    )


def fetch_data(client: httpx.Client, url: str) -> Any:
    """Execute AEMET's two-step API fetch with retry on rate limits."""
    # Step 1: Get the data URL
    while True:
        try:
            response = client.get(url)
            response.raise_for_status()
            break
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                print("Rate limited, waiting 60 seconds...", file=sys.stderr)
                time.sleep(60)
                continue
            raise

    data = response.json()

    # Check if this is the two-step response
    if isinstance(data, dict) and "datos" in data:
        # Step 2: Fetch actual data from the provided URL
        while True:
            try:
                data_response = client.get(data["datos"])
                data_response.raise_for_status()
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    print("Rate limited, waiting 60 seconds...", file=sys.stderr)
                    time.sleep(60)
                    continue
                raise

        # Handle different encodings
        try:
            return data_response.json()
        except UnicodeDecodeError:
            # Try with latin-1 encoding
            content = data_response.content.decode("latin-1")
            return json.loads(content)

    # Direct response (like station info)
    return data


def fetch_station_info(client: httpx.Client) -> dict:
    """Fetch all station metadata."""
    url = f"{BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones"
    stations = fetch_data(client, url)
    return {s["indicativo"]: s for s in stations}


def fetch_daily_batch(client: httpx.Client, start_date: str, end_date: str) -> list:
    """Fetch climate data for date range."""
    url = f"{BASE_URL}/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/todasestaciones"
    return fetch_data(client, url)


def save_station_info(stations: dict, output_dir: Path) -> int:
    """Save station info to estaciones/[indicativo].json."""
    stations_dir = output_dir / "estaciones"
    stations_dir.mkdir(parents=True, exist_ok=True)

    saved_count = 0
    for indicativo, station_data in stations.items():
        file_path = stations_dir / f"{indicativo}.json"
        if not file_path.exists():
            file_path.write_text(
                json.dumps(station_data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            saved_count += 1

    return saved_count


def save_daily_data(date_str: str, data: list, output_dir: Path) -> int:
    """Save data to valores-climatologicos/YYYY/MM/DD/[indicativo].json."""
    # Parse date and create directory
    date = datetime.strptime(date_str, "%Y-%m-%d")
    day_dir = (
        output_dir
        / "valores-climatologicos"
        / f"{date.year:04d}"
        / f"{date.month:02d}"
        / f"{date.day:02d}"
    )
    day_dir.mkdir(parents=True, exist_ok=True)

    # Group by station and save
    saved_count = 0
    for record in data:
        if indicativo := record.get("indicativo"):
            file_path = day_dir / f"{indicativo}.json"
            if not file_path.exists():
                file_path.write_text(
                    json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                saved_count += 1

    return saved_count


def process_date_range(
    client: httpx.Client, start_date: datetime, end_date: datetime, output_dir: Path
):
    """Process date range in batches."""
    current = start_date

    while current <= end_date:
        # Calculate batch end (max 15 days)
        batch_end = min(current + timedelta(days=BATCH_SIZE_DAYS - 1), end_date)

        # Check if any dates in batch need fetching
        dates_needed = []
        temp_date = current
        while temp_date <= batch_end:
            date_str = temp_date.strftime("%Y-%m-%d")
            day_dir = (
                output_dir
                / "valores-climatologicos"
                / f"{temp_date.year:04d}"
                / f"{temp_date.month:02d}"
                / f"{temp_date.day:02d}"
            )
            if not (day_dir.exists() and any(day_dir.glob("*.json"))):
                dates_needed.append(date_str)
            temp_date += timedelta(days=1)

        if dates_needed:
            # Fetch batch
            start_str = current.strftime("%Y-%m-%dT00:00:00UTC")
            end_str = batch_end.strftime("%Y-%m-%dT23:59:59UTC")

            print(f"Fetching {current.date()} to {batch_end.date()}", file=sys.stderr)
            batch_data = fetch_daily_batch(client, start_str, end_str)

            # Group by date and save
            grouped = {}
            for record in batch_data:
                if date := record.get("fecha"):
                    grouped.setdefault(date, []).append(record)

            for date_str, day_data in grouped.items():
                saved_count = save_daily_data(date_str, day_data, output_dir)
                if saved_count > 0:
                    print(
                        f"  Saved {saved_count} stations for {date_str}",
                        file=sys.stderr,
                    )

        # Move to next batch
        current = batch_end + timedelta(days=1)


def cmd_estaciones(args):
    """Fetch and save station information."""
    api_token = os.environ.get("AEMET_API_TOKEN")
    if not api_token:
        print("Error: AEMET_API_TOKEN environment variable not set", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output)

    # Check if stations already exist
    stations_dir = output_dir / "estaciones"
    if stations_dir.exists() and any(stations_dir.glob("*.json")):
        print("Station data already exists, skipping", file=sys.stderr)
        return

    with create_client(cast(str, api_token)) as client:
        print("Fetching station information...", file=sys.stderr)
        stations = fetch_station_info(client)
        saved_count = save_station_info(stations, output_dir)
        print(f"Saved {saved_count} stations", file=sys.stderr)


def cmd_valores_climatologicos(args):
    """Fetch and save climate data."""
    api_token = os.environ.get("AEMET_API_TOKEN")
    if not api_token:
        print("Error: AEMET_API_TOKEN environment variable not set", file=sys.stderr)
        sys.exit(1)

    # Set date range
    if args.start and args.end:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
    else:
        # Default: last 30 days
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=30)

    print(f"Date range: {start_date.date()} to {end_date.date()}", file=sys.stderr)
    output_dir = Path(args.output)

    with create_client(cast(str, api_token)) as client:
        process_date_range(client, start_date, end_date, output_dir)
        print(f"\nData saved to {output_dir}/valores-climatologicos/", file=sys.stderr)


def main():
    """AEMET climate data export tool."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="aemet",
        description="Export AEMET climate data",
        epilog="Examples:\n  aemet estaciones -o data\n  aemet valores-climatologicos --start 2025-01-01 --end 2025-01-31 -o data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Estaciones subcommand
    estaciones_parser = subparsers.add_parser(
        "estaciones", help="Fetch and save station information"
    )
    estaciones_parser.add_argument(
        "--output",
        "-o",
        default=".",
        help="Output directory (default: current directory)",
    )

    # Valores climatológicos subcommand
    valores_parser = subparsers.add_parser(
        "valores-climatologicos", help="Fetch and save climate data"
    )
    valores_parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    valores_parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    valores_parser.add_argument(
        "--output",
        "-o",
        default=".",
        help="Output directory (default: current directory)",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "estaciones":
        cmd_estaciones(args)
    elif args.command == "valores-climatologicos":
        cmd_valores_climatologicos(args)


if __name__ == "__main__":
    main()
