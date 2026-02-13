import logging
import sqlite3
from string import ascii_uppercase

import requests_cache
from selectolax.lexbor import LexborHTMLParser as Parser

session = requests_cache.CachedSession("_vesseltrqcker_cache")

DB_NAME = "vesseltracker.db"
REQUEST_TIMEOUT = 15.0
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


def setup_environment(db_name: str = DB_NAME) -> sqlite3.Connection:
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS vessels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mmsi TEXT NOT NULL UNIQUE,
            imo TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            vessel_type TEXT,
            callsign TEXT,
            flag_country_code TEXT,
            flag_country TEXT,
            length REAL,
            beam REAL
        )
    """
    )
    conn.commit()
    return conn


def scrape_html(html: str, conn: sqlite3.Connection):
    query = """
        INSERT OR REPLACE INTO vessels (
            mmsi, imo, name, vessel_type, flag_country, callsign, length, beam
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    entries = []
    batch_size = 100
    tree = Parser(html)
    rows = tree.css("div.results-table > div.row")
    for row in rows:
        flag_country = row.css_first("div.flag-icon").attrs.get("title")
        name = row.css_first("div.name-type > a.name").text(strip=True)
        vessel_type = row.css_first("div.name-type > span.type").text(strip=True)
        imo = row.css_first("div.imo > span").text(strip=True)
        callsign = row.css_first("div.callsign > span").text(strip=True)
        mmsi = row.css_first("div.mmsi > span").text(strip=True)
        sizes = row.css_first("div.sizes > span").text(strip=True)
        length, beam = (
            [float(x) for x in sizes.split(" x ")] if " x " in sizes else (None, None)
        )

        entries.append(
            (mmsi, imo, name, vessel_type, flag_country, callsign, length, beam)
        )
        # print(flag_country, name, vessel_type)
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


def fetch_vesseltracker_urls(char: str, page_max: int, conn: sqlite3.Connection):
    for page in range(1, page_max + 1):
        url = f"https://www.vesseltracker.com/en/vessels.html?page={page}&search={char}"
        response = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            print(f"{url} : OK")
            scrape_html(response.text, conn)
        else:
            print(f"{url} : ERR Status code: {response.status_code}")
            return


conn = setup_environment()
for c in ascii_uppercase:
    fetch_vesseltracker_urls(c, 300, conn)
