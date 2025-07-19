import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import httpx

BASE_URL = "https://opendata.aemet.es/opendata/api"
BATCH_SIZE_DAYS = 15


def get_client():
    """Get API token and create configured httpx client."""
    api_token = os.environ.get("AEMET_API_TOKEN")
    if not api_token:
        print("Error: AEMET_API_TOKEN environment variable not set", file=sys.stderr)
        sys.exit(1)

    return httpx.Client(
        timeout=httpx.Timeout(60.0, connect=30.0),
        params={"api_key": api_token},
        verify=True,
        follow_redirects=True,
    )


def fetch_data(client, url):
    """Fetch data from AEMET API with retry logic and two-step handling."""
    while True:
        try:
            response = client.get(url)

            # Check for errors before processing
            if response.status_code == 429:
                print("Rate limited, waiting 60 seconds...", file=sys.stderr)
                time.sleep(60)
                continue
            elif response.status_code >= 400:
                print(
                    f"HTTP {response.status_code} error: {response.url}",
                    file=sys.stderr,
                )
                print("Waiting 60 seconds before retrying...", file=sys.stderr)
                time.sleep(60)
                continue

            data = response.json()

            # Handle AEMET's two-step response
            if isinstance(data, dict) and "datos" in data:
                data_response = client.get(data["datos"])

                # Check for errors in the second request
                if data_response.status_code == 429:
                    print("Rate limited, waiting 60 seconds...", file=sys.stderr)
                    time.sleep(60)
                    continue
                elif data_response.status_code >= 400:
                    print(
                        f"HTTP {data_response.status_code} error: {data_response.url}",
                        file=sys.stderr,
                    )
                    print("Waiting 60 seconds before retrying...", file=sys.stderr)
                    time.sleep(60)
                    continue

                try:
                    return data_response.json()
                except UnicodeDecodeError:
                    return json.loads(data_response.content.decode("latin-1"))

            return data

        except httpx.RequestError as e:
            print(f"Connection error: {type(e).__name__}", file=sys.stderr)
            print("Waiting 60 seconds before retrying...", file=sys.stderr)
            time.sleep(60)


def get_day_file_path(output_dir, date):
    """Get the file path for a given date."""
    return (
        output_dir
        / "valores-climatologicos"
        / f"{date.year:04d}"
        / f"{date.month:02d}"
        / f"{date.day:02d}.json"
    )


def save_json(path, data):
    """Save data as JSON if file doesn't exist."""
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def fetch_stations(client, output_dir):
    """Fetch and save station information."""
    stations_dir = output_dir / "estaciones"
    if stations_dir.exists() and any(stations_dir.glob("*.json")):
        print("Station data already exists, skipping", file=sys.stderr)
        return

    print("Fetching station information...", file=sys.stderr)
    stations = fetch_data(
        client,
        f"{BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones",
    )

    saved = sum(
        save_json(stations_dir / f"{station['indicativo']}.json", station)
        for station in stations
    )
    print(f"Saved {saved} stations", file=sys.stderr)


def fetch_climate_data(client, output_dir, start_date, end_date):
    """Fetch and save climate data for date range."""
    current = start_date

    while current <= end_date:
        batch_end = min(current + timedelta(days=BATCH_SIZE_DAYS - 1), end_date)

        # Check if any dates in batch need fetching
        needs_fetch = any(
            not get_day_file_path(output_dir, current + timedelta(days=i)).exists()
            for i in range((batch_end - current).days + 1)
        )

        if needs_fetch:
            start_str = current.strftime("%Y-%m-%dT00:00:00UTC")
            end_str = batch_end.strftime("%Y-%m-%dT23:59:59UTC")
            url = f"{BASE_URL}/valores/climatologicos/diarios/datos/fechaini/{start_str}/fechafin/{end_str}/todasestaciones"

            print(f"Fetching {current.date()} to {batch_end.date()}", file=sys.stderr)
            batch_data = fetch_data(client, url)

            # Group by date and save
            records_by_date = defaultdict(list)
            for record in batch_data:
                if fecha := record.get("fecha"):
                    records_by_date[fecha].append(record)

            for fecha, records in records_by_date.items():
                date = datetime.strptime(fecha, "%Y-%m-%d")
                save_json(get_day_file_path(output_dir, date), records)

        current = batch_end + timedelta(days=1)


def main():
    """AEMET climate data export tool."""
    parser = argparse.ArgumentParser(
        prog="aemet",
        description="Export AEMET climate data",
        epilog="Examples:\n  aemet estaciones -o data\n  aemet valores-climatologicos --start 2025-01-01 --end 2025-01-31 -o data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Estaciones
    estaciones = subparsers.add_parser("estaciones", help="Fetch station information")
    estaciones.add_argument("-o", "--output", default=".", help="Output directory")

    # Valores climatológicos
    valores = subparsers.add_parser("valores-climatologicos", help="Fetch climate data")
    valores.add_argument("--start", help="Start date (YYYY-MM-DD)")
    valores.add_argument("--end", help="End date (YYYY-MM-DD)")
    valores.add_argument("-o", "--output", default=".", help="Output directory")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    output_dir = Path(args.output)

    with get_client() as client:
        if args.command == "estaciones":
            fetch_stations(client, output_dir)
        else:
            # Parse dates with defaults
            if args.start and args.end:
                start_date = datetime.strptime(args.start, "%Y-%m-%d")
                end_date = datetime.strptime(args.end, "%Y-%m-%d")
            else:
                end_date = datetime.now() - timedelta(days=1)
                start_date = end_date - timedelta(days=30)

            print(
                f"Date range: {start_date.date()} to {end_date.date()}", file=sys.stderr
            )
            fetch_climate_data(client, output_dir, start_date, end_date)
            print(
                f"\nData saved to {output_dir}/valores-climatologicos/", file=sys.stderr
            )


if __name__ == "__main__":
    main()
