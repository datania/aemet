import json
import os
import sys
import time
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
        timeout=httpx.Timeout(60.0),
        params={"api_key": api_token},
        follow_redirects=True,
    )


def fetch_with_retry(client, url):
    """Fetch URL with retry on rate limit."""
    while True:
        try:
            response = client.get(url)
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                print("Rate limited, waiting 60 seconds...", file=sys.stderr)
                time.sleep(60)
            else:
                raise


def fetch_data(client, url):
    """Execute AEMET's two-step API fetch."""
    response = fetch_with_retry(client, url)
    data = response.json()

    # Handle two-step response
    if isinstance(data, dict) and "datos" in data:
        data_response = fetch_with_retry(client, data["datos"])
        try:
            return data_response.json()
        except UnicodeDecodeError:
            return json.loads(data_response.content.decode("latin-1"))

    return data


def save_json(path, data):
    """Save data as JSON if file doesn't exist."""
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return True
    return False


def fetch_stations(client, output_dir):
    """Fetch and save station information."""
    stations_dir = output_dir / "estaciones"
    if stations_dir.exists() and any(stations_dir.glob("*.json")):
        print("Station data already exists, skipping", file=sys.stderr)
        return

    print("Fetching station information...", file=sys.stderr)
    url = f"{BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones"
    stations = fetch_data(client, url)

    saved = 0
    for station in stations:
        if save_json(stations_dir / f"{station['indicativo']}.json", station):
            saved += 1

    print(f"Saved {saved} stations", file=sys.stderr)


def fetch_climate_data(client, output_dir, start_date, end_date):
    """Fetch and save climate data for date range."""
    current = start_date

    while current <= end_date:
        batch_end = min(current + timedelta(days=BATCH_SIZE_DAYS - 1), end_date)

        # Check which dates need fetching
        dates_needed = []
        for i in range((batch_end - current).days + 1):
            date = current + timedelta(days=i)
            day_dir = (
                output_dir
                / "valores-climatologicos"
                / f"{date.year:04d}"
                / f"{date.month:02d}"
                / f"{date.day:02d}"
            )
            if not (day_dir.exists() and any(day_dir.glob("*.json"))):
                dates_needed.append(date)

        if dates_needed:
            # Fetch batch
            start_str = current.strftime("%Y-%m-%dT00:00:00UTC")
            end_str = batch_end.strftime("%Y-%m-%dT23:59:59UTC")
            url = f"{BASE_URL}/valores/climatologicos/diarios/datos/fechaini/{start_str}/fechafin/{end_str}/todasestaciones"

            print(f"Fetching {current.date()} to {batch_end.date()}", file=sys.stderr)
            batch_data = fetch_data(client, url)

            # Save by date and station
            for record in batch_data:
                if fecha := record.get("fecha"):
                    date = datetime.strptime(fecha, "%Y-%m-%d")
                    day_dir = (
                        output_dir
                        / "valores-climatologicos"
                        / f"{date.year:04d}"
                        / f"{date.month:02d}"
                        / f"{date.day:02d}"
                    )
                    if indicativo := record.get("indicativo"):
                        save_json(day_dir / f"{indicativo}.json", record)

        current = batch_end + timedelta(days=1)


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
            # Set date range
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
