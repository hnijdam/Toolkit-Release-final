from __future__ import annotations

import argparse
import csv
import datetime as dt
import getpass
import json
import os
import subprocess
import sys
import time
import zipfile
from decimal import Decimal
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv(*_args, **_kwargs):
        return None

import mysql.connector


TARGET_TABLES = [
    ("pulsecounterlog", ["pulsecounterlog", "pulseccounterlog"]),
    ("pulsecounteroffset", ["pulsecounteroffset"]),
    ("communicationlog", ["communicationlog", "comlog"]),
    ("alarm", ["alarm"]),
    ("alarmhistory", ["alarmhistory"]),
    ("failedcommunication", ["failedcommunication"]),
    ("measurementlog", ["measurementlog", "measurmentlog"]),
    ("powerchangelog", ["powerchangelog"]),
]

SYSTEM_DATABASES = {
    "information_schema",
    "mysql",
    "performance_schema",
    "sys",
}

TIMESTAMP_PRIORITY = [
    "changetimestamp",
    "measurementtimestamp",
    "measuredtimestamp",
    "alarmtimestamp",
    "createdtimestamp",
    "createtimestamp",
    "logtimestamp",
    "updatetimestamp",
    "timestamp",
    "datetime",
    "date",
    "time",
    "ts",
    "tsbegin",
    "tsend",
]

DEVICE_ID_PRIORITY = [
    "deviceid",
    "device_id",
]

SLAVE_DEVICE_ID_PRIORITY = [
    "slavedeviceid",
    "slave_deviceid",
    "slaveid",
    "slave_device_id",
]

INBRIDGE_ID_PRIORITY = [
    "inbridgeid",
    "inbridge_id",
]

DEVID_PRIORITY = [
    "devid",
]

ADDRESS_PRIORITY = [
    "address",
]

SLAVE_ADDRESS_PRIORITY = [
    "slaveaddress",
    "slave_address",
]

UNFILTERED_LOG_TABLES = {
    "communicationlog",
    "comlog",
}

NO_TIME_FILTER_TABLES = {
    "powerchangelog",
}

MAX_CONNECTION_ATTEMPTS = 3
RETRY_SLEEP_SECONDS = 1
FETCH_BATCH_SIZE = 1000


def load_environment() -> None:
    base_dir = Path(__file__).resolve().parent
    workspace_root = base_dir.parents[1] if len(base_dir.parents) >= 2 else base_dir
    env_candidates = [
        workspace_root / "DBscript" / ".env",
        base_dir / ".env",
        Path.cwd() / ".env",
    ]
    loaded = False
    for env_path in env_candidates:
        if env_path.exists():
            load_dotenv(env_path, override=True)
            loaded = True
    if not loaded:
        load_dotenv()


load_environment()

DB_HOST = os.getenv("DB_HOST")
DB_HOST2 = os.getenv("DB_HOST2")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def ensure_credentials() -> None:
    global DB_USER, DB_PASSWORD
    if not DB_USER:
        DB_USER = input("DB user: ").strip()
    if not DB_PASSWORD:
        DB_PASSWORD = getpass.getpass("DB password: ")


def get_hosts() -> list[str]:
    hosts = [h.strip() for h in (DB_HOST, DB_HOST2) if h and h.strip()]
    return hosts or ["localhost"]


def create_connection(database: Optional[str] = None, host: Optional[str] = None):
    ensure_credentials()
    hosts = [host] if host else get_hosts()
    last_error = None
    for db_host in hosts:
        for attempt in range(1, MAX_CONNECTION_ATTEMPTS + 1):
            try:
                conn = mysql.connector.connect(
                    host=db_host,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=database,
                    connect_timeout=10,
                    autocommit=True,
                )
                if conn.is_connected():
                    return conn
            except mysql.connector.Error as exc:
                last_error = exc
                print(f"Verbinding poging {attempt} naar {db_host} mislukt: {exc}")
                time.sleep(RETRY_SLEEP_SECONDS)
    raise RuntimeError(f"Kan geen verbinding maken. Laatste fout: {last_error}")


def fetch_databases(include_system: bool = False) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for host in get_hosts():
        conn = create_connection(host=host)
        try:
            cur = conn.cursor()
            cur.execute("SHOW DATABASES")
            for (name,) in cur.fetchall():
                if include_system or name not in SYSTEM_DATABASES:
                    result.append((host, name))
        finally:
            try:
                cur.close()
            except Exception:
                pass
            conn.close()
    return result


def database_matches(requested: str, host: str, database: str) -> bool:
    req = requested.strip().lower()
    return req in {database.lower(), f"{host}/{database}".lower()}


def pick_databases(requested: list[str] | None, include_system: bool = False) -> list[tuple[str, str]]:
    available = fetch_databases(include_system=include_system)
    if not requested:
        return available
    picked: list[tuple[str, str]] = []
    for host, database in available:
        if any(database_matches(item, host, database) for item in requested):
            picked.append((host, database))
    return picked


def split_csv_values(raw: str) -> list[str]:
    return [item.strip() for item in raw.replace(";", ",").split(",") if item.strip()]


def confirm_all_databases_backup() -> bool:
    """Require explicit confirmation before backing up every database."""
    while True:
        answer = input(
            "Geen database ingevuld = backup van ALLE databases. Typ 'yes' om door te gaan of 'q' om te stoppen: "
        ).strip().lower()
        if answer == "yes":
            return True
        if answer == "q":
            print("Backup geannuleerd. Terug naar het vorige menu.")
            return False
        print("Ongeldige invoer. Typ 'yes' om door te gaan of 'q' om te stoppen.")


def find_matching_column(columns: list[str], candidates: list[str]) -> Optional[str]:
    lower_map = {name.lower(): name for name in columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def build_lookup_filters(
    conn,
    database: str,
    actual_table: str,
    all_columns: list[str],
    device_id: Optional[str] = None,
    slave_device_ids: Optional[list[str]] = None,
) -> tuple[list[str], list[object], list[str]]:
    if not (device_id or slave_device_ids):
        return [], [], []

    device_table = find_existing_table(conn, database, "device")
    slave_table = find_existing_table(conn, database, "slavedevice")
    inbridge_column = find_matching_column(all_columns, INBRIDGE_ID_PRIORITY)
    table_device_column = find_matching_column(all_columns, DEVICE_ID_PRIORITY)
    table_slave_column = find_matching_column(all_columns, SLAVE_DEVICE_ID_PRIORITY)
    devid_column = find_matching_column(all_columns, DEVID_PRIORITY)
    address_column = find_matching_column(all_columns, ADDRESS_PRIORITY)
    slave_address_column = find_matching_column(all_columns, SLAVE_ADDRESS_PRIORITY)

    where_parts: list[str] = []
    params: list[object] = []
    notes: list[str] = []

    if device_id and not table_device_column:
        if inbridge_column and device_table:
            where_parts.append(
                f"`{inbridge_column}` IN (SELECT DISTINCT d.`inbridgeid` FROM `{database}`.`{device_table}` d WHERE d.`inbridgeid` IS NOT NULL AND d.`deviceid` = %s)"
            )
            params.append(device_id)
            notes.append(f"deviceid via bridge={device_id}")
        elif devid_column and address_column and device_table:
            where_parts.append(
                f"EXISTS (SELECT 1 FROM `{database}`.`{device_table}` d WHERE d.`deviceid` = %s AND d.`devid` = `{actual_table}`.`{devid_column}` AND d.`address` = `{actual_table}`.`{address_column}`)"
            )
            params.append(device_id)
            notes.append(f"deviceid via device-lookup={device_id}")
        else:
            notes.append("deviceid-filter niet toepasbaar")

    if slave_device_ids and not table_slave_column:
        placeholders = ", ".join(["%s"] * len(slave_device_ids))
        if actual_table.lower() == "failedcommunication" and devid_column and address_column and device_table and slave_table:
            where_parts.append(
                f"EXISTS (SELECT 1 FROM `{database}`.`{device_table}` d JOIN `{database}`.`{slave_table}` sd ON sd.`deviceid` = d.`deviceid` WHERE d.`devid` = `{actual_table}`.`{devid_column}` AND d.`address` = `{actual_table}`.`{address_column}` AND sd.`slavedeviceid` IN ({placeholders}))"
            )
            params.extend(slave_device_ids)
            notes.append(f"slavedevice via parent device in ({', '.join(slave_device_ids)})")
        elif slave_address_column and table_device_column and slave_table:
            where_parts.append(
                f"EXISTS (SELECT 1 FROM `{database}`.`{slave_table}` sd WHERE sd.`deviceid` = `{actual_table}`.`{table_device_column}` AND sd.`slaveaddress` = `{actual_table}`.`{slave_address_column}` AND sd.`slavedeviceid` IN ({placeholders}))"
            )
            params.extend(slave_device_ids)
            notes.append(f"slavedevice via slaveaddress in ({', '.join(slave_device_ids)})")
        elif slave_address_column and devid_column and address_column and device_table and slave_table:
            where_parts.append(
                f"EXISTS (SELECT 1 FROM `{database}`.`{device_table}` d JOIN `{database}`.`{slave_table}` sd ON sd.`deviceid` = d.`deviceid` WHERE d.`devid` = `{actual_table}`.`{devid_column}` AND d.`address` = `{actual_table}`.`{address_column}` AND sd.`slaveaddress` = `{actual_table}`.`{slave_address_column}` AND sd.`slavedeviceid` IN ({placeholders}))"
            )
            params.extend(slave_device_ids)
            notes.append(f"slavedevice via device-lookup in ({', '.join(slave_device_ids)})")
        elif inbridge_column and device_table and slave_table:
            where_parts.append(
                f"`{inbridge_column}` IN (SELECT DISTINCT d.`inbridgeid` FROM `{database}`.`{device_table}` d JOIN `{database}`.`{slave_table}` sd ON sd.`deviceid` = d.`deviceid` WHERE d.`inbridgeid` IS NOT NULL AND sd.`slavedeviceid` IN ({placeholders}))"
            )
            params.extend(slave_device_ids)
            notes.append(f"slavedevice via bridge in ({', '.join(slave_device_ids)})")
        else:
            notes.append("slavedeviceid-filter niet toepasbaar")

    return where_parts, params, notes


def find_existing_table(conn, database: str, preferred_name: str) -> Optional[str]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND LOWER(table_name) = %s
            LIMIT 1
            """,
            (database, preferred_name.lower()),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def find_timestamp_column(conn, database: str, table: str) -> tuple[Optional[str], list[str], str]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (database, table),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    if not rows:
        return None, [], "no-columns"

    columns = [name for name, _dtype in rows]
    lower_map = {name.lower(): name for name in columns}

    for candidate in TIMESTAMP_PRIORITY:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()], columns, "priority-match"

    for name, data_type in rows:
        lower_name = name.lower()
        if data_type.lower() in {"datetime", "timestamp", "date"} and any(
            token in lower_name for token in ("time", "date", "stamp", "created", "changed", "meas")
        ):
            return name, columns, "type-name-match"

    for name, data_type in rows:
        if data_type.lower() in {"datetime", "timestamp", "date"}:
            return name, columns, "first-datetime-column"

    return None, columns, "no-timestamp-column"


def sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return "'" + value.isoformat(sep=" ") + "'"
    if isinstance(value, (bytes, bytearray)):
        return "0x" + bytes(value).hex()
    text = str(value)
    text = text.replace("\\", "\\\\").replace("'", "\\'")
    text = text.replace("\r", "\\r").replace("\n", "\\n")
    return f"'{text}'"


def export_table(
    conn,
    host: str,
    database: str,
    table_label: str,
    table_candidates: list[str],
    output_dir: Path,
    since: dt.datetime,
    device_id: Optional[str] = None,
    slave_device_ids: Optional[list[str]] = None,
    dry_run: bool = False,
) -> dict:
    actual_table = None
    for candidate in table_candidates:
        actual_table = find_existing_table(conn, database, candidate)
        if actual_table:
            break

    if not actual_table:
        return {
            "host": host,
            "database": database,
            "table": table_label,
            "status": "skipped",
            "reason": "table-not-found",
            "rows": 0,
        }

    timestamp_column, all_columns, mode = find_timestamp_column(conn, database, actual_table)
    device_column = find_matching_column(all_columns, DEVICE_ID_PRIORITY)
    slave_column = find_matching_column(all_columns, SLAVE_DEVICE_ID_PRIORITY)

    where_parts: list[str] = []
    params: list[object] = []
    filter_bits: list[str] = []

    if timestamp_column and actual_table.lower() not in NO_TIME_FILTER_TABLES:
        where_parts.append(f"`{timestamp_column}` >= %s")
        params.append(since)
        filter_bits.append(f"laatste {since.isoformat(sep=' ', timespec='seconds')}")
    elif timestamp_column:
        filter_bits.append("tijdfilter overgeslagen voor deze logtabel")
    else:
        filter_bits.append("geen tijdkolom gevonden, volledige tabel geëxporteerd")

    filters_allowed = actual_table.lower() not in UNFILTERED_LOG_TABLES
    if not filters_allowed and (device_id or slave_device_ids):
        filter_bits.append("device/slavedevice-filters overgeslagen voor deze logtabel")

    if filters_allowed and device_id and device_column:
        where_parts.append(f"`{device_column}` = %s")
        params.append(device_id)
        filter_bits.append(f"{device_column}={device_id}")

    if filters_allowed and slave_device_ids and slave_column:
        placeholders = ", ".join(["%s"] * len(slave_device_ids))
        where_parts.append(f"`{slave_column}` IN ({placeholders})")
        params.extend(slave_device_ids)
        filter_bits.append(f"{slave_column} in ({', '.join(slave_device_ids)})")

    extra_where, extra_params, extra_notes = build_lookup_filters(
        conn,
        database,
        actual_table,
        all_columns,
        device_id=device_id if filters_allowed and device_id and not device_column else None,
        slave_device_ids=slave_device_ids if filters_allowed and slave_device_ids and not slave_column else None,
    )
    where_parts.extend(extra_where)
    params.extend(extra_params)
    filter_bits.extend(extra_notes)

    query = f"SELECT * FROM `{database}`.`{actual_table}`"
    if where_parts:
        query += " WHERE " + " AND ".join(where_parts)
    if timestamp_column:
        query += f" ORDER BY `{timestamp_column}` DESC"

    filter_label = ", ".join(filter_bits)

    if dry_run:
        return {
            "host": host,
            "database": database,
            "table": actual_table,
            "status": "dry-run",
            "reason": filter_label,
            "timestamp_column": timestamp_column,
            "device_column": device_column,
            "slave_column": slave_column,
            "mode": mode,
            "rows": None,
        }

    db_dir = output_dir / host / database
    db_dir.mkdir(parents=True, exist_ok=True)
    csv_path = db_dir / f"{actual_table}.csv"
    sql_path = db_dir / f"{actual_table}.sql"

    cur = conn.cursor()
    row_count = 0
    try:
        cur.execute(query, tuple(params))
        columns = [desc[0] for desc in cur.description] if cur.description else []
        with csv_path.open("w", newline="", encoding="utf-8") as csv_handle, sql_path.open("w", encoding="utf-8") as sql_handle:
            writer = csv.writer(csv_handle, delimiter=';')
            writer.writerow(columns)
            sql_handle.write(f"-- Backup generated: {dt.datetime.now().isoformat(sep=' ', timespec='seconds')}\n")
            sql_handle.write(f"-- Host: {host}\n-- Database: {database}\n-- Table: {actual_table}\n")
            sql_handle.write(f"-- Filter: {filter_label}\n\n")
            sql_handle.write("START TRANSACTION;\n")

            col_list = ", ".join(f"`{col}`" for col in columns)
            while True:
                rows = cur.fetchmany(FETCH_BATCH_SIZE)
                if not rows:
                    break
                for row in rows:
                    writer.writerow(row)
                    values_sql = ", ".join(sql_literal(value) for value in row)
                    sql_handle.write(f"INSERT INTO `{actual_table}` ({col_list}) VALUES ({values_sql});\n")
                    row_count += 1

            sql_handle.write("COMMIT;\n")
    finally:
        cur.close()

    if row_count == 0:
        try:
            csv_path.unlink(missing_ok=True)
            sql_path.unlink(missing_ok=True)
        except Exception:
            pass
        return {
            "host": host,
            "database": database,
            "table": actual_table,
            "status": "no-data",
            "reason": filter_label,
            "timestamp_column": timestamp_column,
            "device_column": device_column,
            "slave_column": slave_column,
            "mode": mode,
            "rows": 0,
        }

    return {
        "host": host,
        "database": database,
        "table": actual_table,
        "status": "exported",
        "reason": filter_label,
        "timestamp_column": timestamp_column,
        "device_column": device_column,
        "slave_column": slave_column,
        "mode": mode,
        "rows": row_count,
        "csv": str(csv_path),
        "sql": str(sql_path),
    }


def create_zip_from_folder(folder: Path) -> Path:
    zip_path = folder.with_suffix(".zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in folder.rglob("*"):
            if path.is_file():
                zf.write(path, path.relative_to(folder.parent))
    return zip_path


def reveal_output_path(path: Path) -> None:
    try:
        target = path.resolve()
        if os.name == "nt":
            if target.is_file():
                subprocess.Popen(["explorer", "/select,", str(target)])
            else:
                os.startfile(str(target))
    except Exception as exc:
        print(f"Kon de outputmap niet automatisch openen: {exc}")


def run_backup(
    days: int,
    requested_databases: list[str] | None,
    device_id: Optional[str],
    slave_device_ids: Optional[list[str]],
    include_system: bool,
    output_root: Optional[Path],
    dry_run: bool,
) -> int:
    since = dt.datetime.now() - dt.timedelta(days=days)
    run_stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    base_root = output_root or (Path.home() / "Documents" / "ICY-Logs" / "db-backups")
    output_dir = base_root / f"backup_last_{days}_days_{run_stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    targets = pick_databases(requested_databases, include_system=include_system)
    if not targets:
        print("Geen databases gevonden voor backup.")
        return 1

    summary: list[dict] = []
    print(f"Backup gestart voor {len(targets)} databases. Venster: laatste {days} dagen.")
    if requested_databases:
        print(f"Databases filter: {', '.join(requested_databases)}")
    if device_id:
        print(f"DeviceID filter: {device_id}")
    if slave_device_ids:
        print(f"SlavedeviceID filter: {', '.join(slave_device_ids)}")

    for host, database in targets:
        print(f"\n=== {host}/{database} ===")
        try:
            conn = create_connection(database=database, host=host)
        except Exception as exc:
            msg = {
                "host": host,
                "database": database,
                "table": None,
                "status": "error",
                "reason": str(exc),
                "rows": 0,
            }
            print(f"Fout bij verbinden met {host}/{database}: {exc}")
            summary.append(msg)
            continue

        try:
            for table_label, table_candidates in TARGET_TABLES:
                result = export_table(
                    conn,
                    host,
                    database,
                    table_label,
                    table_candidates,
                    output_dir,
                    since,
                    device_id=device_id,
                    slave_device_ids=slave_device_ids,
                    dry_run=dry_run,
                )
                summary.append(result)
                status = result.get("status")
                if status == "exported":
                    print(f"✔ {result['table']}: {result['rows']} rijen")
                elif status == "no-data":
                    print(f"○ {result['table']}: 0 rijen ({result.get('reason')})")
                elif status == "dry-run":
                    print(f"• {result['table']}: {result.get('timestamp_column') or 'geen tijdkolom'}")
                else:
                    print(f"- {table_label}: overgeslagen ({result.get('reason')})")
        finally:
            conn.close()

    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    summary_txt_path = output_dir / "summary.txt"
    with summary_txt_path.open("w", encoding="utf-8") as handle:
        handle.write(f"Backup samenvatting - laatste {days} dagen\n")
        handle.write(f"Gegenereerd op: {dt.datetime.now().isoformat(sep=' ', timespec='seconds')}\n\n")
        for item in summary:
            table_name = item.get("table") or "onbekend"
            status = item.get("status")
            host = item.get("host")
            database = item.get("database")
            rows = item.get("rows")
            reason = item.get("reason") or ""
            if status == "exported":
                line = f"[OK] {host}/{database}/{table_name}: {rows} rijen"
            elif status == "no-data":
                line = f"[LEEG] {host}/{database}/{table_name}: 0 rijen - {reason}"
            elif status == "dry-run":
                line = f"[DRY-RUN] {host}/{database}/{table_name}: {reason}"
            else:
                line = f"[SKIP] {host}/{database}/{table_name}: {reason}"
            handle.write(line + "\n")

    if dry_run:
        print(f"\nDry run klaar. Overzicht: {summary_path}")
        print(f"Samenvatting: {summary_txt_path}")
        reveal_output_path(summary_txt_path)
        return 0

    zip_path = create_zip_from_folder(output_dir)
    print("\nBackup klaar.")
    print(f"Map: {output_dir}")
    print(f"Zip: {zip_path}")
    print(f"Samenvatting: {summary_txt_path}")
    reveal_output_path(summary_txt_path)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Maak een backup van logtabellen uit meerdere databases voor de afgelopen periode."
    )
    parser.add_argument("--days", type=int, default=14, help="Aantal dagen terug om te exporteren")
    parser.add_argument(
        "--database",
        action="append",
        help="Database of host/database om te exporteren; kan meerdere keren worden opgegeven",
    )
    parser.add_argument("--deviceid", help="Optionele filter op deviceid")
    parser.add_argument(
        "--slavedeviceid",
        action="append",
        help="Optionele filter op slavedeviceid; kan meerdere keren worden opgegeven",
    )
    parser.add_argument("--include-system", action="store_true", help="Neem ook systeemdatabases mee")
    parser.add_argument("--output-dir", help="Optionele rootmap voor de backup output")
    parser.add_argument("--dry-run", action="store_true", help="Toon alleen wat geëxporteerd zou worden")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.days < 1:
        print("Aantal dagen moet minimaal 1 zijn.")
        return 1

    requested_databases: list[str] = []
    for item in args.database or []:
        requested_databases.extend(split_csv_values(item))

    slave_device_ids: list[str] = []
    for item in args.slavedeviceid or []:
        slave_device_ids.extend(split_csv_values(item))

    device_id = args.deviceid.strip() if args.deviceid else None

    if sys.stdin.isatty():
        if not requested_databases:
            db_input = input("Database naam (optioneel, komma-gescheiden; leeg = alle databases): ").strip()
            if db_input:
                requested_databases = split_csv_values(db_input)
            elif not confirm_all_databases_backup():
                return 0

        if not device_id:
            device_input = input("DeviceID (optioneel): ").strip()
            if device_input:
                device_id = device_input

        if not slave_device_ids:
            slave_input = input("SlavedeviceID(s) (optioneel, komma-gescheiden): ").strip()
            if slave_input:
                slave_device_ids = split_csv_values(slave_input)

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else None
    return run_backup(
        days=args.days,
        requested_databases=requested_databases or None,
        device_id=device_id,
        slave_device_ids=slave_device_ids or None,
        include_system=args.include_system,
        output_root=output_dir,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
