import sqlite3


def merge_databases(sx_path: str, vt_path: str, merged_db_path: str):
    # Connect to the original databases
    sx_db = sqlite3.connect(sx_path)
    vt_db = sqlite3.connect(vt_path)

    # Create the merged database
    merged_conn = sqlite3.connect(merged_db_path)
    cursor = merged_conn.cursor()

    # Create the vessels table in the merged database
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS vessels (
            mmsi TEXT PRIMARY KEY,
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
    merged_conn.commit()

    countries = dict()

    vt_cur = vt_db.cursor()
    sx_cur = sx_db.cursor()
    vt_cur.execute(
        "SELECT mmsi, imo, name, vessel_type, callsign, flag_country_code, flag_country, length, beam FROM vessels"
    )
    rows = vt_cur.fetchall()
    for row in rows:
        sx_cur.execute(
            "select mmsi, imo, name, vessel_type, callsign, flag_country_code, flag_country, length, beam from vessels where mmsi = ?",
            (row[0],),
        )
        sx_row = sx_cur.fetchone()
        if sx_row:
            row = list(row)
            cc = sx_row[5].strip().upper() if sx_row[5] else None
            if not cc and row[6] in countries:
                cc = countries[row[6]]
            if not row[5]:
                row[5] = cc

            if not row[6] in countries and cc:
                countries[row[6]] = cc.strip().upper()

            # If the vessel exists in both databases, prefer ShipXplorer data
            cursor.execute(
                """
                INSERT OR REPLACE INTO vessels (
                    mmsi, imo, name, vessel_type, callsign, flag_country_code, flag_country, length, beam
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )

    # Commit changes and close connections
    merged_conn.commit()
    sx_db.close()
    vt_db.close()
    merged_conn.close()


def main():
    merge_databases("shipxplorer_vessels.db", "vesseltracker.db", "merged_vessels.db")


main()
