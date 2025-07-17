import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import httpx

# Constants
BASE_URL = "https://opendata.aemet.es/opendata/api"
BATCH_SIZE_DAYS = 15
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
REQUEST_DELAY = 0.0

# Type aliases
StationInfo = Dict[str, Any]
WeatherData = Dict[str, Any]


def create_client(api_token: str) -> httpx.Client:
    """Create configured httpx client with API token."""
    return httpx.Client(
        timeout=httpx.Timeout(60.0, connect=30.0),
        params={"api_key": api_token},
        verify=True,
        follow_redirects=True,
    )


def fetch_with_retry(
    client: httpx.Client, url: str, max_retries: int = MAX_RETRIES
) -> Any:
    """Execute AEMET's two-step API fetch with exponential backoff."""
    backoff = INITIAL_BACKOFF

    for attempt in range(max_retries):
        try:
            response = client.get(url)

            if response.status_code == 200:
                data = response.json()

                # Check if this is the two-step response with datos URL
                if isinstance(data, dict) and "datos" in data:
                    datos_url = data["datos"]

                    # Step 2: Fetch actual data
                    time.sleep(REQUEST_DELAY)
                    data_response = client.get(datos_url)

                    if data_response.status_code == 200:
                        # Handle different encodings
                        try:
                            return data_response.json()
                        except UnicodeDecodeError:
                            # Try with latin-1 encoding
                            content = data_response.content.decode("latin-1")
                            return json.loads(content)
                    else:
                        raise Exception(
                            f"Data fetch failed: {data_response.status_code}"
                        )
                else:
                    # Direct response (like station info)
                    return data

            elif response.status_code in (429, 500, 502, 503):
                time.sleep(backoff)
                backoff *= 2
                continue
            else:
                response.raise_for_status()

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Retry {attempt + 1}/{max_retries}: {str(e)}", file=sys.stderr)
                time.sleep(backoff)
                backoff *= 2
            else:
                raise

    raise Exception(f"Failed after {max_retries} attempts")


def fetch_station_info(client: httpx.Client) -> Dict[str, StationInfo]:
    """Fetch all station metadata and return as dict keyed by station ID."""
    url = f"{BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones"
    stations = fetch_with_retry(client, url)
    return {s["indicativo"]: s for s in stations}


def fetch_daily_batch(
    client: httpx.Client, start_date: str, end_date: str
) -> List[WeatherData]:
    """Fetch climate data for date range (max 15 days)."""
    url = f"{BASE_URL}/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/todasestaciones"
    return fetch_with_retry(client, url)


def group_by_date(data: List[WeatherData]) -> Dict[str, List[WeatherData]]:
    """Group weather records by date."""
    grouped: Dict[str, List[WeatherData]] = {}
    for record in data:
        date = record.get("fecha")
        if date:
            grouped.setdefault(date, []).append(record)
    return grouped


def save_station_info(stations: Dict[str, StationInfo], output_dir: Path) -> int:
    """Save station info to estaciones/[indicativo].json."""
    stations_dir = output_dir / "estaciones"
    stations_dir.mkdir(parents=True, exist_ok=True)

    saved_count = 0
    for indicativo, station_data in stations.items():
        file_path = stations_dir / f"{indicativo}.json"
        if not file_path.exists():
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(station_data, f, ensure_ascii=False, indent=2)
            saved_count += 1

    return saved_count


def save_daily_data_by_station(
    date_str: str, data: List[WeatherData], output_dir: Path
) -> int:
    """Save data to valores-climatologicos/YYYY/MM/DD/[indicativo].json."""
    # Parse date and create directory structure
    date = datetime.strptime(date_str, "%Y-%m-%d")
    day_dir = (
        output_dir
        / "valores-climatologicos"
        / f"{date.year:04d}"
        / f"{date.month:02d}"
        / f"{date.day:02d}"
    )
    day_dir.mkdir(parents=True, exist_ok=True)

    # Group data by station
    stations_data = {}
    for record in data:
        indicativo = record.get("indicativo")
        if indicativo:
            stations_data[indicativo] = record

    # Save each station's data (only if not exists)
    saved_count = 0
    for indicativo, station_record in stations_data.items():
        file_path = day_dir / f"{indicativo}.json"
        if not file_path.exists():
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(station_record, f, ensure_ascii=False, indent=2)
            saved_count += 1

    return saved_count


def check_date_exists(date_str: str, output_dir: Path) -> bool:
    """Check if data for a specific date already exists."""
    date = datetime.strptime(date_str, "%Y-%m-%d")
    day_dir = (
        output_dir
        / "valores-climatologicos"
        / f"{date.year:04d}"
        / f"{date.month:02d}"
        / f"{date.day:02d}"
    )
    return day_dir.exists() and any(day_dir.glob("*.json"))


def process_date_range(
    client: httpx.Client,
    start_date: datetime,
    end_date: datetime,
    output_dir: Path,
) -> None:
    """Process date range in 15-day batches."""
    current = start_date

    while current <= end_date:
        # Calculate batch end (max 15 days)
        batch_end = min(current + timedelta(days=BATCH_SIZE_DAYS - 1), end_date)

        # Check which dates in this batch already exist
        dates_needed = []
        temp_date = current
        while temp_date <= batch_end:
            date_str = temp_date.strftime("%Y-%m-%d")
            if not check_date_exists(date_str, output_dir):
                dates_needed.append(temp_date)
            temp_date += timedelta(days=1)

        if not dates_needed:
            print(
                f"Data already exists for {current.date()} to {batch_end.date()}, skipping",
                file=sys.stderr,
            )
        else:
            # Format dates for API
            start_str = current.strftime("%Y-%m-%dT00:00:00UTC")
            end_str = batch_end.strftime("%Y-%m-%dT23:59:59UTC")

            print(
                f"Fetching data from {current.date()} to {batch_end.date()}",
                file=sys.stderr,
            )

            # Fetch and process batch
            batch_data = fetch_daily_batch(client, start_str, end_str)
            daily_groups = group_by_date(batch_data)

            # Save each day's data by station
            for date_str, day_data in daily_groups.items():
                saved_count = save_daily_data_by_station(date_str, day_data, output_dir)
                if saved_count > 0:
                    print(
                        f"  Saved {saved_count} stations for {date_str}",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"  Data already exists for {date_str}, skipped",
                        file=sys.stderr,
                    )

        # Move to next batch
        current = batch_end + timedelta(days=1)

        # Rate limiting between batches - longer delay for historical data
        if current <= end_date:
            if current.year < 2000:
                time.sleep(2.0)  # Longer delay for historical data
            else:
                time.sleep(REQUEST_DELAY)


def cmd_estaciones(args) -> None:
    """Fetch and save station information."""
    api_token = os.environ.get("AEMET_API_TOKEN")
    if not api_token:
        print("Error: AEMET_API_TOKEN environment variable not set", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output)
    client = create_client(api_token)

    try:
        # Check if stations already exist
        stations_dir = output_dir / "estaciones"
        if stations_dir.exists() and any(stations_dir.glob("*.json")):
            print(
                f"Station data already exists in {output_dir}/estaciones/, skipping fetch",
                file=sys.stderr,
            )
        else:
            print("Fetching station information...", file=sys.stderr)
            stations = fetch_station_info(client)
            saved_count = save_station_info(stations, output_dir)
            print(
                f"Saved {saved_count} new stations to {output_dir}/estaciones/",
                file=sys.stderr,
            )

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()


def cmd_valores_climatologicos(args) -> None:
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
    client = create_client(api_token)

    try:
        process_date_range(client, start_date, end_date, output_dir)
        print(
            f"\nClimate data saved to {output_dir}/valores-climatologicos/",
            file=sys.stderr,
        )

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()


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
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
