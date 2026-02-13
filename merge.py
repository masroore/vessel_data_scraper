import sqlite3
import json
import os
from pathlib import Path


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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mmsi TEXT NOT NULL UNIQUE,
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

    # Countries mapping: normalized country name (UPPER, stripped) -> country code
    countries = dict()

    # Try to load previously saved countries mapping from disk. We store it next to the
    # merged DB using the same name but with the extension '.countries.json'. If loading
    # fails, continue with an empty mapping.
    countries_file = Path(merged_db_path).with_suffix(".countries.json")
    if countries_file.exists():
        try:
            with countries_file.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
                # Normalize keys to trimmed upper-case for consistent lookups
                countries = {
                    (k.strip().upper() if k else k): v for k, v in loaded.items()
                }
        except Exception as e:
            print(f"Warning: failed to load countries file {countries_file}: {e}")

    vt_cur = vt_db.cursor()
    sx_cur = sx_db.cursor()
    # noinspection SqlDialectInspection
    vt_cur.execute(
        "SELECT mmsi, imo, name, vessel_type, callsign, flag_country_code, flag_country, length, beam FROM vessels"
    )

    count = 0
    rows = vt_cur.fetchall()
    for row in rows:
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
            row = list(row)
            # Normalize the country code and country name for consistent handling
            cc = sx_row[5].strip().upper() if sx_row[5] else None
            country_key = row[6].strip().upper() if row[6] else None

            # If no country code from sx_row, try to look up by normalized country name
            if not cc and country_key and country_key in countries:
                cc = countries[country_key]

            # Fill in the flag_country_code if missing
            if not row[5]:
                row[5] = cc

            # Store mapping for future lookups (use normalized country name)
            if country_key and country_key not in countries and cc:
                countries[country_key] = cc

            # If the vessel exists in both databases, prefer ShipXplorer data
            cursor.execute(
                """
                INSERT OR REPLACE INTO vessels (
                    mmsi, imo, name, vessel_type, callsign, flag_country_code, flag_country, length, beam
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
            count += 1
            if count % 100 == 0:
                print(f"Merged {count} vessels...", end="\r")
                merged_conn.commit()

    # Commit changes and close connections
    merged_conn.commit()
    # Persist countries mapping back to disk atomically
    try:
        tmp_path = countries_file.with_suffix(".countries.json.tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(countries, f, indent=2, ensure_ascii=False)
        # Atomic replace
        os.replace(str(tmp_path), str(countries_file))
    except Exception as e:
        print(f"Warning: failed to save countries file {countries_file}: {e}")
    sx_db.close()
    vt_db.close()
    merged_conn.close()


def main():
    merge_databases("shipxplorer_vessels.db", "vesseltracker.db", "merged_vessels.db")


main()
