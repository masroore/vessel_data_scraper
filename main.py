import os
import json
import sqlite3
import requests
import time
import logging
import argparse
from pathlib import Path
from typing import Optional
import re

# Configuration (can be overridden via CLI args or environment variables)
BASE_URL = os.environ.get(
    "SHIPXPLORER_BASE_URL", "https://www.shipxplorer.com/search/vessels"
)
START_PAGE = int(os.environ.get("SHIPXPLORER_START_PAGE", 1))
END_PAGE = int(os.environ.get("SHIPXPLORER_END_PAGE", 2357))
DB_NAME = os.environ.get("SHIPXPLORER_DB", "shipxplorer_vessels.db")
JSON_DIR = os.environ.get("SHIPXPLORER_JSON_DIR", "jsons")
REQUEST_TIMEOUT = float(os.environ.get("SHIPXPLORER_TIMEOUT", 15.0))
SLEEP_SECONDS = float(os.environ.get("SHIPXPLORER_SLEEP", 0.1))
MAX_FETCH_RETRIES = int(os.environ.get("SHIPXPLORER_MAX_RETRIES", 3))

# Headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def setup_environment(
    json_dir: str = JSON_DIR, db_name: str = DB_NAME
) -> sqlite3.Connection:
    """Ensures the JSON storage directory and Database exist and returns a DB connection."""
    Path(json_dir).mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS vessels (
            mmsi TEXT PRIMARY KEY,
            imo TEXT,
            name TEXT NOT NULL,
            vessel_type TEXT,
            callsign TEXT,
            flag_country_code TEXT,
            flag_country TEXT,
            origin_country TEXT,
            origin_country_code TEXT,
            origin_port_code TEXT,
            origin_port TEXT,
            nav_status TEXT,
            status TEXT,
            length REAL,
            beam REAL,
            latitude REAL,
            longitude REAL
        )
    """
    )
    conn.commit()
    return conn


def normalize_name(s: str) -> str | None:
    """Normalize vessel names by:
    - removing certain punctuation characters (quotes, backticks, exclamation marks)
    - collapsing multiple whitespace into a single space
    - stripping leading/trailing whitespace
    - converting to title case

    Returns an empty string for falsy inputs.
    """
    if not s:
        return None

    # Remove quotes, backticks, exclamation marks (anywhere in the name)
    cleaned = re.sub(r"[\"'`!*\-._]+", "", s)

    # Replace any sequence of whitespace (including tabs/newlines) with a single space
    cleaned = re.sub(r"\s+", " ", cleaned)

    cleaned = cleaned.strip().upper()
    return cleaned or None


def save_json_to_disk(file_path: str, data: dict) -> None:
    """Write JSON data to disk. Overwrites atomically where possible."""
    tmp_path = f"{file_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, file_path)


def fetch_page_from_network(
    page_number: int,
    base_url: str = BASE_URL,
    headers: dict = HEADERS,
    timeout: float = REQUEST_TIMEOUT,
    max_retries: int = MAX_FETCH_RETRIES,
) -> Optional[dict]:
    """Fetch a page from the remote API with simple retry/backoff logic."""
    params = {"vessel_list": "true", "page": page_number}
    backoff = 0.5
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(
                base_url, params=params, headers=headers, timeout=timeout
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning(
                "Request error on page %s attempt %s/%s: %s",
                page_number,
                attempt,
                max_retries,
                e,
            )
        except json.JSONDecodeError as e:
            logger.error("Failed to decode JSON for page %s: %s", page_number, e)
            return None

        time.sleep(backoff)
        backoff *= 2

    logger.error(
        "Giving up fetching page %s after %s attempts", page_number, max_retries
    )
    return None


def get_page_data(
    page_number: int,
    json_dir: str = JSON_DIR,
    base_url: str = BASE_URL,
    use_cache: bool = True,
    timeout: float = REQUEST_TIMEOUT,
) -> Optional[dict]:
    """Loads from disk if available, otherwise fetches from API."""
    file_path = os.path.join(json_dir, f"page_{page_number}.json")

    # 1. Check if file already exists on disk
    if use_cache and os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.warning(
                "Failed to read cached JSON %s: %s (will try network)", file_path, e
            )

    # 2. Fetch from HTTP if not on disk or cache invalid
    data = fetch_page_from_network(
        page_number, base_url=base_url, headers=HEADERS, timeout=timeout
    )
    if data is None:
        return None

    try:
        save_json_to_disk(file_path, data)
    except OSError as e:
        logger.warning("Failed to save JSON to disk %s: %s", file_path, e)

    # Rate limiting only applied on actual HTTP requests
    time.sleep(SLEEP_SECONDS)
    return data


def parse_and_save(conn: sqlite3.Connection, data: dict, batch_size: int = 500) -> bool:
    """Maps obfuscated JSON keys to database columns and saves them in batches."""
    if not data:
        logger.debug("No 'vessels' key in data")
        return False

    # vessels = data.get("vessels")
    vessels = data
    if not vessels:
        logger.debug("Empty vessels list")
        return False

    query = """
        INSERT OR REPLACE INTO vessels (
            mmsi, imo, name, vessel_type, origin_country, 
            origin_country_code, origin_port_code, origin_port, 
            flag_country, flag_country_code, length, beam,
            nav_status, status, callsign, latitude, longitude
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    entries = []
    for v in vessels:
        if not isinstance(v, dict):
            logger.debug("Skipping non-dict vessel entry: %s", type(v))
            continue

        mmsi = v.get("susi")
        if not mmsi:
            # Skip records without a primary key
            logger.debug("Skipping vessel without MMSI: %s", v)
            continue

        def to_float(x):
            try:
                return float(x) if x is not None else None
            except (TypeError, ValueError):
                return None

        lat = to_float(v.get("la"))
        lon = to_float(v.get("lo"))
        length = to_float(v.get("slen"))
        beam = to_float(v.get("bea"))

        entries.append(
            (
                str(mmsi),  # mmsi
                v.get("simo"),  # imo
                normalize_name(v.get("snam")),  # name (normalized)
                v.get("scgtdec"),  # vessel_type
                v.get("sorgco"),  # origin_country
                v.get("sorgcc"),  # origin_country_code
                v.get("sorg"),  # origin_port_code
                v.get("sorgna"),  # origin_port
                v.get("say"),  # flag_country
                v.get("sayc"),  # flag_country_code
                length,  # length
                beam,  # beam
                v.get("snas"),  # nav_status
                v.get("status"),  # status
                v.get("scal"),  # callsign
                lat,  # latitude
                lon,  # longitude
            )
        )

        # Flush batches periodically
        if len(entries) >= batch_size:
            try:
                with conn:
                    conn.executemany(query, entries)
            except sqlite3.DatabaseError as e:
                logger.error("Database error during batch insert: %s", e)
                return False
            entries = []

    # Final flush
    if entries:
        try:
            with conn:
                conn.executemany(query, entries)
        except sqlite3.DatabaseError as e:
            logger.error("Database error during final insert: %s", e)
            return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="ShipXplorer vessels scraper (safe mode)"
    )
    parser.add_argument(
        "--start", type=int, default=START_PAGE, help="Start page (inclusive)"
    )
    parser.add_argument(
        "--end", type=int, default=END_PAGE, help="End page (inclusive)"
    )
    parser.add_argument("--db", default=DB_NAME, help="SQLite DB file path")
    parser.add_argument("--json-dir", default=JSON_DIR, help="Directory for JSON cache")
    parser.add_argument("--base-url", default=BASE_URL, help="Base API URL")
    parser.add_argument(
        "--no-network",
        action="store_true",
        help="Do not perform network requests; only use cached JSONs",
    )
    parser.add_argument(
        "--timeout", type=float, default=REQUEST_TIMEOUT, help="HTTP timeout seconds"
    )
    args = parser.parse_args()

    conn = setup_environment(json_dir=args.json_dir, db_name=args.db)
    logger.info(
        "Processing pages %s..%s (cache: %s)", args.start, args.end, args.json_dir
    )

    try:
        for page in range(args.start, args.end + 1):
            # Using '\r' to update the same line in the console
            print(f"Working on page {page}/{args.end}...", end="\r", flush=True)

            data = get_page_data(
                page,
                json_dir=args.json_dir,
                base_url=args.base_url,
                use_cache=not args.no_network,
                timeout=args.timeout,
            )
            if data:
                parse_and_save(conn, data)
            else:
                logger.warning(
                    "No data found or failed to fetch page %s; stopping.", page
                )
                break

    finally:
        conn.close()
        print()  # ensure newline after progress

    logger.info("Process finished. Data is stored in SQLite and JSON cache.")


if __name__ == "__main__":
    main()
