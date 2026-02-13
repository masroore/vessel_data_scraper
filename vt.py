import logging
import os
from string import ascii_uppercase

import requests_cache

session = requests_cache.CachedSession("_vesseltrqcker_cache")

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


def fetch_vesseltracker_urls(char: str, page_max: int):
    for page in range(1, page_max + 1):
        url = f"https://www.vesseltracker.com/en/vessels.html?page={page}&search={char}"
        response = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            print(f"{url} is accessible.")
        else:
            print(f"{url} is not accessible. Status code: {response.status_code}")
            return


for c in ascii_uppercase:
    fetch_vesseltracker_urls(c, 200)
