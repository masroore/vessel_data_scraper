import sqlite3
import json
import os
from collections import OrderedDict
from pathlib import Path
import csv
from typing import Any


def get_countries_file(merged_db_path: str) -> Path:
    return Path(merged_db_path).with_suffix(".countries.json")


def load_countries_mapping(cursor: sqlite3.Cursor) -> dict[Any, Any] | None:
    countries = dict()
    # 1) Try to read a CSV mapping (countries.csv) in the current working directory.
    #    Support files both with and without a header row. Each row should have at
    #    least two columns: name,code. We normalize the country name to UPPER/TRIM.
    csv_path = Path("countries.csv")
    if csv_path.exists():
        try:
            with csv_path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue

                    if len(row) < 2:
                        continue

                    c_name = row[0].strip().upper() if row[0].strip() else None
                    c_code = row[1].strip().upper() if row[1].strip() else None
                    if c_name and c_code:
                        countries[c_name] = c_code
        except Exception as e:
            print(f"Warning: failed to read countries.csv: {e}")

    # 2) Merge with an on-disk JSON mapping stored next to the merged DB (overrides CSV)
    countries_file = get_countries_file("merged_vessels.db")
    if countries_file.exists():
        try:
            with countries_file.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
                for k, v in loaded.items() if isinstance(loaded, dict) else []:
                    if not k:
                        continue
                    if v is None:
                        continue
                    nk = k.strip().upper()
                    if nk not in countries:
                        countries[nk] = v.strip().upper() if isinstance(v, str) else v
        except Exception as e:
            print(f"Warning: failed to load countries file {countries_file}: {e}")

        # 3) If still empty, attempt to derive mappings from the ShipXplorer DB
        # noinspection SqlDialectInspection
        cursor.execute(
            "SELECT DISTINCT flag_country_code, flag_country FROM vessels WHERE flag_country_code IS NOT NULL AND flag_country IS NOT NULL"
        )
        for r in cursor.fetchall():
            c_code = r[0].strip().upper() if r[0] else None
            c_name = r[1].strip().upper() if r[1] else None
            if c_name and c_code and c_name not in countries:
                countries[c_name] = c_code

        return countries


def save_countries_mapping(countries: dict[Any, Any]):
    countries_file = get_countries_file("merged_vessels.db")
    data = OrderedDict(countries.items())
    try:
        tmp_path = countries_file.with_suffix(".countries.json.tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
        # Atomic replace
        os.replace(str(tmp_path), str(countries_file))
    except Exception as e:
        print(f"Warning: failed to save countries file {countries_file}: {e}")


def merge_databases(sx_path: str, vt_path: str, merged_db_path: str):
    # Connect to the original databases
    sx_db = sqlite3.connect(sx_path)
    vt_db = sqlite3.connect(vt_path)

    # Create the merged database
    merged_conn = sqlite3.connect(merged_db_path)
    cursor = merged_conn.cursor()

    # Create the vessels table in the merged database
    # noinspection SqlDialectInspection
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS vessels (
            --id INTEGER PRIMARY KEY AUTOINCREMENT,
            --mmsi TEXT NOT NULL UNIQUE,
            mmsi TEXT PRIMARY KEY,
            imo TEXT NOT NULL,
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
    merged_conn.commit()

    vt_cur = vt_db.cursor()
    sx_cur = sx_db.cursor()

    # Countries mapping: normalized country name (UPPER, stripped) -> country code
    countries = load_countries_mapping(sx_cur)

    # noinspection SqlDialectInspection
    vt_cur.execute(
        "SELECT mmsi, imo, name, vessel_type, callsign, flag_country_code, flag_country, length, beam FROM vessels"
    )

    count = 0
    rows = vt_cur.fetchall()
    for row in rows:
        c_code = row[5].strip().upper() if row[5] else None
        c_name = row[6].strip().upper() if row[6] else None
        if not c_code and c_name in countries:
            c_code = countries[c_name]

        if not c_code:
            # noinspection SqlDialectInspection
            sx_cur.execute(
                "select mmsi, imo, name, vessel_type, callsign, flag_country_code, flag_country, length, beam from vessels where mmsi = ? or imo = ?",
                (
                    row[0],
                    row[1],
                ),
            )
            sx_row = sx_cur.fetchone()
            if sx_row:
                # Normalize the country code and country name for consistent handling
                c_code = sx_row[5].strip().upper() if sx_row[5] else None

                print(c_name, c_code)
                # Store mapping for future lookups (use normalized country name)
                if c_name and c_name not in countries and c_code:
                    countries[c_name] = c_code

        data = list(row)
        data[5] = c_code  # Update the country code in the row data
        if data[4] and len(data[4].strip()) == 0:
            data[4] = None  # Normalize empty callsign to NULL
        print(data)
        cursor.execute(
            """
            INSERT INTO vessels (
                mmsi, imo, name, vessel_type, callsign, flag_country_code, flag_country, length, beam
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            data,
        )
        count += 1
        if count % 100 == 0:
            print(f"Merged {count} vessels...", end="\r")
            merged_conn.commit()

    # Commit changes and close connections
    merged_conn.commit()
    # Persist countries mapping back to disk atomically
    save_countries_mapping(countries)
    sx_db.close()
    vt_db.close()
    merged_conn.close()


def main():
    merge_databases("shipxplorer_vessels.db", "vesseltracker.db", "merged_vessels.db")


main()
