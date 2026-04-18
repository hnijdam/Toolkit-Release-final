import os
import re
from datetime import datetime
from io import BytesIO
from numbers import Number
from pathlib import Path

import mysql.connector
import pandas as pd
import streamlit as st
from dotenv import dotenv_values

APP_TITLE = "Pulsecounter Offset Tool"

LOG_TABLE = "pulsecounterlog"
SLAVE_TABLE = "slavedevice"
DEVICE_TABLE = "device"
LOCATION_TABLE = "location"
BUILDINGTYPE_TABLE = "buildingtype"
DEVICETYPE_TABLE = "devicetype"
OFFSET_TABLE = "pulsecounteroffset"

APP_FILE = Path(__file__).resolve()
APP_DIR = APP_FILE.parent
LOGO_PATH = APP_DIR / "logo_icy.svg"
SCRIPTS_ROOT = APP_FILE.parents[3] if len(APP_FILE.parents) >= 4 else APP_DIR
DEFAULT_RUNTIME_LOG_DIR = Path.home() / "Documents" / "ICY-Logs"
RUNTIME_LOG_DIR = DEFAULT_RUNTIME_LOG_DIR
RUNTIME_LOG_PATH = RUNTIME_LOG_DIR / "pulse_counter_offset_tool.log"

ENV_PATHS = [
    SCRIPTS_ROOT / "Toolkit" / ".env",
    SCRIPTS_ROOT / "DBscript" / ".env",
    SCRIPTS_ROOT / "python" / "DBscript" / ".env",
    APP_FILE.parents[1] / "DBscript" / ".env" if len(APP_FILE.parents) >= 2 else APP_DIR / ".env",
    APP_FILE.parents[2] / ".env" if len(APP_FILE.parents) >= 3 else APP_DIR / ".env",
    Path.cwd() / ".env",
    APP_DIR / ".env",
    APP_DIR / ".env.local",
]

LOADED_ENV_FILES = []
_seen_env_paths = set()
for env_path in ENV_PATHS:
    resolved_path = str(Path(env_path).resolve())
    if resolved_path in _seen_env_paths:
        continue
    _seen_env_paths.add(resolved_path)

    if Path(env_path).exists():
        env_values = dotenv_values(env_path)
        for key, value in env_values.items():
            if value is not None and str(value).strip() != "":
                os.environ[key] = str(value)
        LOADED_ENV_FILES.append(str(env_path))


DEVICETYPE_VARIABLES = {
    "PLE": {"meter_variable": "acu_kwh_meter", "meter_type_key": "electricity_import", "meter_type_label": "kWh meter", "meter_unit": "kWh"},
    "PLE8": {"meter_variable": "pl8_kwh_meter", "meter_type_key": "electricity_import", "meter_type_label": "kWh meter", "meter_unit": "kWh"},
    "PLEB": {"meter_variable": "acu_export_kwh_meter", "meter_type_key": "electricity_export", "meter_type_label": "Teruglevering kWh", "meter_unit": "kWh"},
    "PLG": {"meter_variable": "acu_gas_meter", "meter_type_key": "gas", "meter_type_label": "Gas meter m³", "meter_unit": "m³"},
    "PLG8": {"meter_variable": "pl8_gas_meter", "meter_type_key": "gas", "meter_type_label": "Gas meter m³", "meter_unit": "m³"},
    "PLW": {"meter_variable": "acu_water_meter", "meter_type_key": "water", "meter_type_label": "Water meter m³", "meter_unit": "m³"},
    "PLW8": {"meter_variable": "pl8_water_meter", "meter_type_key": "water", "meter_type_label": "Water meter m³", "meter_unit": "m³"},
    "PLHW": {"meter_variable": "acu_hot_water_meter", "meter_type_key": "hot_water", "meter_type_label": "Warmwater meter m³", "meter_unit": "m³"},
    "PLHGJ": {"meter_variable": "acu_heat_gj_meter", "meter_type_key": "heat_gj", "meter_type_label": "Warmtemeter GJ", "meter_unit": "GJ"},
    "PLHMWH": {"meter_variable": "acu_heat_mwh_meter", "meter_type_key": "heat_mwh", "meter_type_label": "Warmtemeter MWh", "meter_unit": "MWh"},
    "PLCGJ": {"meter_variable": "acu_cooling_gj_meter", "meter_type_key": "cooling_gj", "meter_type_label": "Koelmeter GJ", "meter_unit": "GJ"},
    "PLCMWH": {"meter_variable": "acu_cooling_mwh_meter", "meter_type_key": "cooling_mwh", "meter_type_label": "Koelmeter MWh", "meter_unit": "MWh"},
    "P1EL": {"meter_variable": "p1_electricity_low", "meter_type_key": "electricity_import_low", "meter_type_label": "P1 elektra laag", "meter_unit": "kWh"},
    "P1EH": {"meter_variable": "p1_electricity_high", "meter_type_key": "electricity_import_high", "meter_type_label": "P1 elektra hoog", "meter_unit": "kWh"},
    "P1BEL": {"meter_variable": "p1_export_low", "meter_type_key": "electricity_export_low", "meter_type_label": "P1 teruglever laag", "meter_unit": "kWh"},
    "P1BEH": {"meter_variable": "p1_export_high", "meter_type_key": "electricity_export_high", "meter_type_label": "P1 teruglever hoog", "meter_unit": "kWh"},
    "P1GAS": {"meter_variable": "p1_gas_meter", "meter_type_key": "gas", "meter_type_label": "P1 gasmeter", "meter_unit": "m³"},
    "CAMPSLAVE": {"meter_variable": "campere_meter", "meter_type_key": "camping", "meter_type_label": "Campère meter", "meter_unit": "kWh"},
    "CAMPCTRL": {"meter_variable": "campere_controller", "meter_type_key": "controller", "meter_type_label": "ICY4942 Campère controller", "meter_unit": ""},
    "CAMPEREMOD": {"meter_variable": "campere_module", "meter_type_key": "camping", "meter_type_label": "ICY4518 Campère module", "meter_unit": "kWh"},
    "CAMPEREWS": {"meter_variable": "campere_wall_socket", "meter_type_key": "camping", "meter_type_label": "ICY4518 Campère wall socket", "meter_unit": "kWh"},
    "PRMKWH": {"meter_variable": "prm_kwh_meter", "meter_type_key": "electricity_import", "meter_type_label": "PRM kWh meter", "meter_unit": "kWh"},
    "PRMWATER": {"meter_variable": "prm_water_meter", "meter_type_key": "water", "meter_type_label": "PRM watermeter", "meter_unit": "m³"},
    "PRMGAS": {"meter_variable": "prm_gas_meter", "meter_type_key": "gas", "meter_type_label": "PRM gasmeter", "meter_unit": "m³"},
    "PRMEXPORTKWH": {"meter_variable": "prm_export_kwh_meter", "meter_type_key": "electricity_export", "meter_type_label": "PRM teruglever kWh meter", "meter_unit": "kWh"},
    "PRMDHKWH": {"meter_variable": "prm_heat_kwh_meter", "meter_type_key": "heat_kwh", "meter_type_label": "PRM stadswarmte kWh", "meter_unit": "kWh"},
    "PRMDHGJ": {"meter_variable": "prm_heat_gj_meter", "meter_type_key": "heat_gj", "meter_type_label": "PRM stadswarmte GJ", "meter_unit": "GJ"},
    "PRMEXPORTM3": {"meter_variable": "prm_export_m3_meter", "meter_type_key": "export_m3", "meter_type_label": "PRM export m³", "meter_unit": "m³"},
    "PRMEXPORTGJ": {"meter_variable": "prm_export_gj_meter", "meter_type_key": "export_gj", "meter_type_label": "PRM export GJ", "meter_unit": "GJ"},
    "PRMPRODUCTIONKWH": {"meter_variable": "prm_production_kwh_meter", "meter_type_key": "production_kwh", "meter_type_label": "PRM productie kWh", "meter_unit": "kWh"},
    "PRMPRODUCTIONM3": {"meter_variable": "prm_production_m3_meter", "meter_type_key": "production_m3", "meter_type_label": "PRM productie m³", "meter_unit": "m³"},
    "PRMPRODUCTIONGJ": {"meter_variable": "prm_production_gj_meter", "meter_type_key": "production_gj", "meter_type_label": "PRM productie GJ", "meter_unit": "GJ"},
    "PRMHOTWATER": {"meter_variable": "prm_hot_water_meter", "meter_type_key": "hot_water", "meter_type_label": "PRM warmwatermeter", "meter_unit": "m³"},
    "PRMCAMPERE": {"meter_variable": "prm_campere_meter", "meter_type_key": "camping", "meter_type_label": "PRM campère meter", "meter_unit": "kWh"},
    "PRMWATERTAP": {"meter_variable": "prm_watertap_meter", "meter_type_key": "water", "meter_type_label": "PRM watertapmeter", "meter_unit": "m³"},
}

PERSISTED_STATE_DEFAULTS = {
    "db_host_override": "auto",
    "db_host_manual": "",
    "db_name": "",
    "db_user": "",
    "user_initials": "",
    "location_filter": "",
    "device_filter": "",
    "slave_filter": "",
    "mid_filter": "Alle meters",
    "selected_location": "Alle locaties",
    "search_text": "",
    "db_ready": False,
    "current_record_index": 0,
}

URL_SAFE_STATE_KEYS = {
    "mid_filter",
    "selected_location",
    "current_record_index",
}

ENV_BACKED_STATE_KEYS = {
    "db_host_manual": "DB_HOST",
    "db_name": "DB_NAME",
    "db_user": "DB_USER",
    "db_password": "DB_PASSWORD",
    "user_initials": "USER_INITIALS",
}

MID_PROTECTED_METER_MESSAGE = "Offsets voor MID gecertificeerde ICY 4850 Campère meters zijn geblokkeerd. Tonen mag wel, aanpassen niet."
MID_PROTECTED_DEVICETYPE_CODES = {
    "campslave",
    "campctrl",
}


def normalize_protection_text(value):
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().lower().replace("è", "e").replace("é", "e")


def is_offset_edit_blocked(row):
    if row is None:
        return False

    values = {
        "meter_type_label": normalize_protection_text(row.get("meter_type_label", "")),
        "devicetype_name": normalize_protection_text(row.get("devicetype_name", "")),
        "devicetype_code": normalize_protection_text(row.get("devicetype_code", row.get("devicename", ""))),
        "meter_variable": normalize_protection_text(row.get("meter_variable", "")),
        "meter_type_key": normalize_protection_text(row.get("meter_type_key", "")),
        "device_name": normalize_protection_text(row.get("device_name", "")),
        "icyname": normalize_protection_text(row.get("icyname", "")),
    }

    combined = " ".join(values.values())

    explicitly_not_blocked = any(token in combined for token in [
        "icy4518",
        "icy5247",
        "prm",
        "campere module",
        "campere wall socket",
    ])
    if explicitly_not_blocked:
        return False

    has_4850_marker = "icy4850" in combined or "4850" in combined
    has_protected_code = values["devicetype_code"] in MID_PROTECTED_DEVICETYPE_CODES

    return has_protected_code or has_4850_marker


def parse_persisted_state_value(key, value):
    if isinstance(value, list):
        value = value[0] if value else ""

    if key == "db_ready":
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    if key == "current_record_index":
        try:
            return max(0, int(float(str(value).strip() or "0")))
        except Exception:
            return 0

    return "" if value is None else str(value).strip()


def build_persisted_state(state):
    persisted = {}
    for key in URL_SAFE_STATE_KEYS:
        default_value = PERSISTED_STATE_DEFAULTS.get(key, "")
        value = parse_persisted_state_value(key, state.get(key, default_value))

        if key == "current_record_index":
            persisted[key] = str(value)
        elif value != "":
            persisted[key] = value

    return persisted


def restore_persisted_state():
    try:
        query_params = st.query_params
    except Exception:
        query_params = {}

    for key, default_value in PERSISTED_STATE_DEFAULTS.items():
        raw_value = query_params.get(key, default_value) if key in URL_SAFE_STATE_KEYS else default_value
        parsed_value = parse_persisted_state_value(key, raw_value)

        if parsed_value in {"", None} and key in ENV_BACKED_STATE_KEYS:
            env_value = cfg(ENV_BACKED_STATE_KEYS[key], default_value)
            parsed_value = parse_persisted_state_value(key, env_value)
            if key == "user_initials":
                parsed_value = str(parsed_value).upper()

        if key not in st.session_state:
            st.session_state[key] = parsed_value

    db_password_value = st.session_state.get("db_password", "") or cfg("DB_PASSWORD", "")
    st.session_state["db_password"] = db_password_value

    if "manual" not in st.session_state:
        st.session_state["manual"] = None
    if "batch_staging" not in st.session_state:
        st.session_state["batch_staging"] = []


def sync_persisted_state():
    try:
        query_params = st.query_params
    except Exception:
        return

    target = build_persisted_state(st.session_state)
    current = {}
    for key in URL_SAFE_STATE_KEYS:
        if key in query_params:
            current[key] = parse_persisted_state_value(key, query_params.get(key))

    normalized_target = {key: parse_persisted_state_value(key, value) for key, value in target.items()}

    if current != normalized_target:
        query_params.clear()
        for key, value in target.items():
            query_params[key] = value


# =========================
# DB
# =========================

def cfg(key, default=""):
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except Exception:
        return os.getenv(key, default)


def get_available_db_hosts():
    hosts = []
    for host in [
        st.session_state.get("db_host_manual", ""),
        cfg("DB_HOST", ""),
        cfg("DB_HOST2", ""),
    ]:
        host = str(host).strip()
        if host and host not in hosts:
            hosts.append(host)
    return hosts


def normalize_id_series(series):
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    return (
        series.fillna("")
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .replace(r"^(?i:nan|none|<na>|null)$", "", regex=True)
    )


def ensure_series(value, index, default=""):
    if isinstance(value, pd.Series):
        return value.reindex(index)
    if value is None:
        value = default
    return pd.Series([value] * len(index), index=index)


def normalize_meterdivider_series(value, index):
    divider = pd.to_numeric(ensure_series(value, index, 1), errors="coerce").fillna(1)
    divider = divider.where(divider.ne(0), 1).abs()
    return divider


def get_normalized_meterdivider(value, default=1.0):
    try:
        divider = abs(float(value))
    except (TypeError, ValueError):
        return float(default)
    if divider == 0:
        return float(default)
    return divider


def calculate_effective_reading(raw_value, offset_value_raw=0, meterdivider=1):
    divider = get_normalized_meterdivider(meterdivider)
    return (float(raw_value or 0) + float(offset_value_raw or 0)) / divider


def calculate_new_offset_raw(desired_meter_reading, raw_value, meterdivider=1, current_offset_raw=0):
    if desired_meter_reading is None or pd.isna(desired_meter_reading):
        return float(current_offset_raw or 0)
    divider = get_normalized_meterdivider(meterdivider)
    return (float(desired_meter_reading) * divider) - float(raw_value or 0)


def get_default_initials():
    return str(cfg("USER_INITIALS", "")).strip().upper()


def build_comment_value():
    initials = str(st.session_state.get("user_initials", "")).strip().upper()
    if not initials:
        raise ValueError("Initialen zijn verplicht.")
    date_part = datetime.now().strftime("%d-%m-%Y")
    return f"{date_part} {initials}"


def build_record_reference(record):
    if record is None:
        return "onbekend record"
    get_value = record.get if hasattr(record, "get") else lambda key, default="": default
    slave_id = str(get_value("slavedeviceid", "")).strip()
    device_id = str(get_value("deviceid", "")).strip()
    channel = str(get_value("channel", "")).strip()
    parts = []
    if slave_id:
        parts.append(f"slavedeviceid={slave_id}")
    if device_id:
        parts.append(f"deviceid={device_id}")
    if channel:
        parts.append(f"channel={channel}")
    return ", ".join(parts) if parts else "onbekend record"


def get_runtime_log_path():
    runtime_log_path = Path(RUNTIME_LOG_PATH)
    if os.getenv("PYTEST_CURRENT_TEST") and runtime_log_path == (DEFAULT_RUNTIME_LOG_DIR / "pulse_counter_offset_tool.log"):
        runtime_log_path = APP_DIR / ".pytest-logs" / "pulse_counter_offset_tool.log"
    return runtime_log_path


def start_batch_log(label="Batch opslaan"):
    try:
        runtime_log_path = get_runtime_log_path()
        runtime_log_path.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with runtime_log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"=== BATCH START {timestamp} | {label} ===\n")
    except Exception:
        pass


def write_runtime_log(message, level="INFO", record=None):
    try:
        runtime_log_path = get_runtime_log_path()
        runtime_log_path.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        record_ref = build_record_reference(record)
        line = f"[{timestamp}] [{level}] {message} | {record_ref}\n"
        with runtime_log_path.open("a", encoding="utf-8") as handle:
            handle.write(line)
    except Exception:
        pass


def read_runtime_log_tail(max_lines=200):
    try:
        runtime_log_path = get_runtime_log_path()
        if not runtime_log_path.exists():
            return "Nog geen logregels beschikbaar."
        lines = runtime_log_path.read_text(encoding="utf-8").splitlines()
        if not lines:
            return "Nog geen logregels beschikbaar."
        batch_start_indexes = [index for index, line in enumerate(lines) if line.startswith("=== BATCH START ")]
        if batch_start_indexes:
            lines = lines[batch_start_indexes[-1]:]
        return "\n".join(lines[-max_lines:]) if lines else "Nog geen logregels beschikbaar."
    except Exception:
        return "Log kon niet worden gelezen."


def to_plain_value(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def clean_display_text(value):
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "<na>", "null"}:
        return ""
    if text in {"0", "0.0", "0.00", "0.000"}:
        return ""
    return text


def normalize_display_text_series(value, index):
    series = ensure_series(value, index, "")
    return series.map(clean_display_text)


def normalize_searchable_text(value):
    text = clean_display_text(value).lower()
    text = re.sub(r"[-_/|]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_searchable_text_series(value, index):
    series = ensure_series(value, index, "")
    return series.map(normalize_searchable_text)


def get_meter_type_variables(devicetypeid="", devicename="", icyname="", metertype=""):
    def clean_text(value):
        return clean_display_text(value)

    devicetypeid = normalize_id_series([devicetypeid]).iloc[0]
    devicename = clean_text(devicename).upper()
    icyname = clean_text(icyname)
    metertype = clean_text(metertype)

    info = DEVICETYPE_VARIABLES.get(devicename, {}).copy()

    fallback_label = icyname or devicename or metertype or (f"Devicetype {devicetypeid}" if devicetypeid else "Onbekend metertype")

    if not info:
        info = {
            "meter_variable": f"devicetype_{devicetypeid}" if devicetypeid else "unknown_meter",
            "meter_type_key": devicename.lower() if devicename else "unknown",
            "meter_type_label": fallback_label,
            "meter_unit": "",
        }

    info["devicetype_code"] = devicename or devicetypeid
    display_label = info.get("meter_type_label") or fallback_label
    if icyname and display_label and icyname.strip().lower() != display_label.strip().lower():
        info["devicetype_name"] = f"{icyname} - {display_label}"
    else:
        info["devicetype_name"] = display_label or icyname or fallback_label
    return info


def get_text_series(df, column_name):
    if not isinstance(df, pd.DataFrame):
        return pd.Series(dtype="object")
    if column_name in df.columns:
        return ensure_series(df[column_name], df.index, "").fillna("").astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype="object")


def format_table_value(value):
    if value is None or pd.isna(value):
        return ""

    if isinstance(value, bool):
        return value

    if isinstance(value, Number):
        numeric = float(value)
        if abs(numeric - round(numeric)) < 1e-12:
            return str(int(round(numeric)))
        return f"{numeric:.6f}".rstrip("0").rstrip(".")

    return value


def get_batch_preview_display_df(df):
    if not isinstance(df, pd.DataFrame):
        return df

    visible_columns = [
        "deviceid",
        "slavedeviceid",
        "channel",
        "new_meter_reading",
        "new_meterdivider",
        "match_status",
        "location_label",
        "meter_type_label",
        "raw_reading",
        "current_offset",
        "effective_reading",
        "resulting_effective_reading",
        "new_offset",
    ]
    available_columns = [col for col in visible_columns if col in df.columns]
    return df[available_columns].copy() if available_columns else df.copy()


def get_batch_staging_editor_df(rows):
    desired_columns = ["deviceid", "slavedeviceid", "channel", "new_meter_reading", "new_meterdivider"]

    if isinstance(rows, pd.DataFrame):
        df = rows.copy()
    elif isinstance(rows, list):
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(columns=desired_columns)

    for col in desired_columns:
        if col not in df.columns:
            df[col] = "" if col != "new_meterdivider" else 1

    df = df[desired_columns].copy()
    for col in ["deviceid", "slavedeviceid", "channel"]:
        df[col] = normalize_id_series(df[col])

    df["new_meter_reading"] = df["new_meter_reading"].where(~pd.isna(df["new_meter_reading"]), "")
    df["new_meterdivider"] = pd.to_numeric(df["new_meterdivider"], errors="coerce").fillna(1)
    df["new_meterdivider"] = df["new_meterdivider"].where(df["new_meterdivider"].ne(0), 1).abs()
    return df


def normalize_id_value(value):
    series = normalize_id_series(pd.Series([value]))
    return str(series.iloc[0]) if not series.empty else ""


def build_batch_staging_key(record):
    if record is None:
        return ("", "", "")
    return (
        normalize_id_value(record.get("deviceid", "")),
        normalize_id_value(record.get("slavedeviceid", "")),
        normalize_id_value(record.get("channel", "")),
    )


def build_batch_staging_row(record, desired_meter_reading=None, new_meterdivider=None):
    if record is None:
        raise ValueError("Geen record geselecteerd om toe te voegen aan de batchwachtrij.")
    if is_offset_edit_blocked(record):
        raise ValueError(MID_PROTECTED_METER_MESSAGE)

    device_id, slave_id, channel = build_batch_staging_key(record)
    if not device_id and not slave_id:
        raise ValueError("Het geselecteerde record mist een DeviceID en SlavedeviceID.")

    divider_value = new_meterdivider
    if divider_value is None or divider_value == "" or pd.isna(divider_value):
        divider_value = record.get("new_meterdivider", record.get("meterdivider", record.get("current_meterdivider", 1)))

    desired_value = desired_meter_reading
    if desired_value is None or (isinstance(desired_value, str) and desired_value.strip() == "") or pd.isna(desired_value):
        desired_value = ""

    staged_row = {
        "deviceid": device_id,
        "slavedeviceid": slave_id,
        "channel": channel,
        "new_meter_reading": to_plain_value(desired_value) if desired_value != "" else "",
        "new_meterdivider": to_plain_value(get_normalized_meterdivider(divider_value)),
    }
    return staged_row


def upsert_batch_staging_rows(existing_rows, new_row):
    rows = []
    if isinstance(existing_rows, pd.DataFrame):
        rows = existing_rows.to_dict("records")
    elif isinstance(existing_rows, list):
        rows = [dict(row) for row in existing_rows]

    target_key = build_batch_staging_key(new_row)
    updated_rows = []
    replaced = False

    for row in rows:
        row_key = build_batch_staging_key(row)
        if row_key == target_key:
            if not replaced:
                updated_rows.append(dict(new_row))
                replaced = True
        else:
            updated_rows.append(dict(row))

    if not replaced:
        updated_rows.append(dict(new_row))

    return updated_rows, "updated" if replaced else "added"


def render_static_table(df, max_height=460):
    if df is None:
        return

    safe_df = df.copy()
    for col in safe_df.columns:
        safe_df[col] = safe_df[col].map(format_table_value)
    safe_df = safe_df.fillna("")

    table_html = safe_df.to_html(index=False, escape=False)
    st.markdown(
        f'<div class="icy-static-table" style="max-height: {int(max_height)}px;">{table_html}</div>',
        unsafe_allow_html=True,
    )


def db_config(database_name=None, host=None):
    hosts = get_available_db_hosts()
    manual_host = str(st.session_state.get("db_host_manual", "")).strip()
    fallback_host = host or manual_host or (hosts[0] if hosts else "")
    return {
        "host": fallback_host,
        "port": int(st.session_state.get("db_port", cfg("DB_PORT", "3306"))),
        "user": str(st.session_state.get("db_user", "")).strip() or cfg("DB_USER", "root"),
        "password": st.session_state.get("db_password", "") or cfg("DB_PASSWORD", ""),
        "database": database_name or str(st.session_state.get("db_name", "")).strip() or cfg("DB_NAME", ""),
    }


def conn(database_name=None):
    selected_host = st.session_state.get("db_host_override", "auto")
    manual_host = str(st.session_state.get("db_host_manual", "")).strip()
    hosts = get_available_db_hosts()

    if manual_host:
        hosts = [manual_host] + [host for host in hosts if host != manual_host]

    if selected_host and selected_host != "auto":
        hosts = [selected_host]

    if not hosts:
        raise ValueError("Database host ontbreekt. Vul een host in bij de database-instellingen of zet DB_HOST in de .env.")

    errors = []
    for host in hosts:
        c = db_config(database_name, host=host)
        if not c["database"]:
            raise ValueError("Database naam ontbreekt")
        try:
            connection = mysql.connector.connect(**c)
            st.session_state["active_db_host"] = host
            return connection
        except Exception as exc:
            errors.append(f"{host}: {exc}")

    raise ConnectionError("Geen verbinding mogelijk met de database hosts: " + " | ".join(errors))


# =========================
# LOAD
# =========================

@st.cache_data(ttl=60)
def load(table, database_name, host_choice="auto"):
    c = conn(database_name)
    cur = None
    try:
        cur = c.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        c.close()


def load_optional(table, database_name, host_choice="auto"):
    try:
        return load(table, database_name, host_choice)
    except Exception:
        return pd.DataFrame()


# =========================
# BUILD CATALOG (FIXED CORE)
# =========================

def build_catalog(log_df, slave_df, offset_df, device_df=None, location_df=None, buildingtype_df=None, devicetype_df=None):

    log_df = log_df.copy()
    slave_df = slave_df.copy()
    offset_df = offset_df.copy()
    device_df = device_df.copy() if isinstance(device_df, pd.DataFrame) else pd.DataFrame()
    location_df = location_df.copy() if isinstance(location_df, pd.DataFrame) else pd.DataFrame()
    buildingtype_df = buildingtype_df.copy() if isinstance(buildingtype_df, pd.DataFrame) else pd.DataFrame()
    devicetype_df = devicetype_df.copy() if isinstance(devicetype_df, pd.DataFrame) else pd.DataFrame()

    device_key = "deviceid"
    slave_key = "slavedeviceid"
    reading_col = "value"

    if reading_col not in log_df.columns:
        raise ValueError("Geen meterstand kolom")

    for df in [log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df]:
        for col in ["deviceid", "slavedeviceid", "locationid", "buildingtypeid", "devicetypeid"]:
            if col in df.columns:
                df[col] = normalize_id_series(df[col])
        if "channel" in df.columns:
            df["channel"] = normalize_id_series(df["channel"])

    if device_key not in log_df.columns:
        log_df[device_key] = ""
    if slave_key not in log_df.columns:
        log_df[slave_key] = ""
    if "channel" not in log_df.columns:
        log_df["channel"] = ""

    log_df[reading_col] = pd.to_numeric(log_df[reading_col], errors="coerce")
    log_df["record_key"] = log_df[slave_key].where(log_df[slave_key].ne(""), log_df[device_key])

    if "timestamp" in log_df.columns:
        log_df["last_reading_timestamp_sort"] = pd.to_datetime(log_df["timestamp"], errors="coerce")
    else:
        log_df["last_reading_timestamp_sort"] = pd.NaT

    if "pulsecounterlogid" in log_df.columns:
        log_df["latest_row_order"] = pd.to_numeric(log_df["pulsecounterlogid"], errors="coerce")
    else:
        log_df["latest_row_order"] = pd.Series(range(len(log_df)), index=log_df.index, dtype="int64")

    log_df = log_df.sort_values(
        by=["record_key", "channel", "last_reading_timestamp_sort", "latest_row_order"],
        ascending=[True, True, True, True],
        na_position="last",
        kind="stable",
    )

    latest = (
        log_df.dropna(subset=[reading_col])
        .drop_duplicates(subset=["record_key", "channel"], keep="last")
    )

    merged = latest.copy()
    merged["logged_deviceid"] = normalize_id_series(merged[device_key])

    if slave_key in slave_df.columns:
        slave_cols = [
            col for col in [
                slave_key,
                "deviceid",
                "locationid",
                "name",
                "slavedevicetypeid",
                "devicetypeid",
                "metertype",
                "meterdivider",
            ] if col in slave_df.columns
        ]
        if slave_cols:
            slave_lookup = slave_df[slave_cols].drop_duplicates(subset=[slave_key], keep="last").rename(
                columns={
                    "deviceid": "slave_parent_deviceid",
                    "locationid": "slave_locationid",
                    "name": "slave_name",
                    "slavedevicetypeid": "slave_slavedevicetypeid",
                    "devicetypeid": "slave_devicetypeid",
                    "metertype": "slave_metertype",
                    "meterdivider": "slave_meterdivider",
                }
            )
            merged = merged.merge(slave_lookup, on=slave_key, how="left")

    merged["deviceid"] = merged["logged_deviceid"]
    if "slave_parent_deviceid" in merged.columns:
        fallback_mask = merged["deviceid"].eq("")
        merged.loc[fallback_mask, "deviceid"] = merged.loc[fallback_mask, "slave_parent_deviceid"]
    merged["deviceid"] = normalize_id_series(merged["deviceid"])

    if device_key in device_df.columns:
        device_cols = [col for col in ["deviceid", "locationid", "name", "devicetypeid", "meterdivider"] if col in device_df.columns]
        if device_cols:
            device_lookup = device_df[device_cols].drop_duplicates(subset=[device_key], keep="last").rename(
                columns={
                    "locationid": "device_locationid",
                    "name": "device_name",
                    "devicetypeid": "device_devicetypeid",
                    "meterdivider": "device_meterdivider",
                }
            )
            merged = merged.merge(device_lookup, on="deviceid", how="left")

    if "slave_locationid" in merged.columns:
        merged["locationid"] = normalize_id_series(merged["slave_locationid"])
    if "device_locationid" in merged.columns:
        if "locationid" not in merged.columns:
            merged["locationid"] = normalize_id_series(merged["device_locationid"])
        else:
            merged["locationid"] = merged["locationid"].where(merged["locationid"].ne(""), normalize_id_series(merged["device_locationid"]))

    if "locationid" in merged.columns and "locationid" in location_df.columns:
        location_cols = [col for col in ["locationid", "locationname", "buildingtypeid"] if col in location_df.columns]
        if location_cols:
            merged = merged.merge(
                location_df[location_cols].drop_duplicates(subset=["locationid"], keep="last"),
                on="locationid",
                how="left",
                suffixes=("", "_location")
            )

    merged["devicetypeid"] = ""
    if "slave_slavedevicetypeid" in merged.columns:
        merged["devicetypeid"] = normalize_id_series(merged["slave_slavedevicetypeid"])
    if "slave_devicetypeid" in merged.columns:
        merged["devicetypeid"] = merged["devicetypeid"].where(
            merged["devicetypeid"].ne(""),
            normalize_id_series(merged["slave_devicetypeid"])
        )
    if "device_devicetypeid" in merged.columns:
        merged["devicetypeid"] = merged["devicetypeid"].where(
            merged["devicetypeid"].ne(""),
            normalize_id_series(merged["device_devicetypeid"])
        )

    if "buildingtypeid" in merged.columns and "buildingtypeid" in buildingtype_df.columns:
        building_cols = [col for col in ["buildingtypeid", "buildingname"] if col in buildingtype_df.columns]
        if building_cols:
            merged = merged.merge(
                buildingtype_df[building_cols].drop_duplicates(subset=["buildingtypeid"], keep="last"),
                on="buildingtypeid",
                how="left",
                suffixes=("", "_building")
            )

    if "devicetypeid" in merged.columns and "devicetypeid" in devicetype_df.columns:
        devicetype_cols = [col for col in ["devicetypeid", "devid", "devicename", "icyname"] if col in devicetype_df.columns]
        if devicetype_cols:
            merged = merged.merge(
                devicetype_df[devicetype_cols].drop_duplicates(subset=["devicetypeid"], keep="last"),
                on="devicetypeid",
                how="left",
                suffixes=("", "_devicetype")
            )

    if "device_devicetypeid" in merged.columns and "devicetypeid" in devicetype_df.columns:
        device_type_cols = [col for col in ["devicetypeid", "icyname", "devicename"] if col in devicetype_df.columns]
        if device_type_cols:
            device_type_lookup = devicetype_df[device_type_cols].drop_duplicates(subset=["devicetypeid"], keep="last").rename(
                columns={
                    "devicetypeid": "device_devicetypeid",
                    "icyname": "device_type_icyname",
                    "devicename": "device_type_name",
                }
            )
            merged = merged.merge(device_type_lookup, on="device_devicetypeid", how="left")

    if device_key in offset_df.columns or slave_key in offset_df.columns:
        offset_df["offset_value"] = pd.to_numeric(offset_df.get("offset"), errors="coerce")

        if slave_key in offset_df.columns and slave_key in merged.columns:
            slave_offsets = offset_df[[slave_key, "offset_value"]].copy()
            slave_offsets[slave_key] = normalize_id_series(slave_offsets[slave_key])
            slave_offsets = slave_offsets[slave_offsets[slave_key] != ""]
            slave_offsets = slave_offsets.drop_duplicates(subset=[slave_key], keep="last").rename(
                columns={"offset_value": "slave_offset_value"}
            )
            merged = merged.merge(slave_offsets, on=slave_key, how="left")

        if device_key in offset_df.columns and device_key in merged.columns:
            direct_offsets = offset_df[[device_key, slave_key, "offset_value"]].copy() if slave_key in offset_df.columns else offset_df[[device_key, "offset_value"]].copy()
            direct_offsets[device_key] = normalize_id_series(direct_offsets[device_key])
            if slave_key in direct_offsets.columns:
                direct_offsets[slave_key] = normalize_id_series(direct_offsets[slave_key])
                direct_offsets = direct_offsets[direct_offsets[slave_key] == ""]
                direct_offsets = direct_offsets.drop(columns=[slave_key])
            direct_offsets = direct_offsets[direct_offsets[device_key] != ""]
            direct_offsets = direct_offsets.drop_duplicates(subset=[device_key], keep="last").rename(
                columns={"offset_value": "device_offset_value"}
            )
            merged = merged.merge(direct_offsets, on=device_key, how="left")

        merged["offset_value"] = pd.to_numeric(merged.get("slave_offset_value"), errors="coerce")
        merged["offset_value"] = merged["offset_value"].combine_first(
            pd.to_numeric(merged.get("device_offset_value"), errors="coerce")
        )

    slave_meterdivider_series = pd.to_numeric(ensure_series(merged.get("slave_meterdivider", pd.NA), merged.index, pd.NA), errors="coerce")
    device_meterdivider_series = pd.to_numeric(ensure_series(merged.get("device_meterdivider", pd.NA), merged.index, pd.NA), errors="coerce")
    existing_meterdivider_series = pd.to_numeric(ensure_series(merged.get("meterdivider", pd.NA), merged.index, pd.NA), errors="coerce")
    merged["meterdivider"] = normalize_meterdivider_series(
        slave_meterdivider_series.combine_first(device_meterdivider_series).combine_first(existing_meterdivider_series).fillna(1),
        merged.index,
    )
    merged["raw_value"] = pd.to_numeric(ensure_series(merged.get(reading_col, 0), merged.index, 0), errors="coerce").fillna(0)
    merged["offset_value_raw"] = pd.to_numeric(ensure_series(merged.get("offset_value", 0), merged.index, 0), errors="coerce").fillna(0)
    merged["raw_reading"] = merged["raw_value"] / merged["meterdivider"]
    merged["current_offset"] = merged["offset_value_raw"] / merged["meterdivider"]
    merged["effective_reading"] = merged["raw_reading"] + merged["current_offset"]

    last_reading_timestamp = ensure_series(merged.get("timestamp", ""), merged.index, "").fillna("").astype(str).str.strip()
    merged["last_reading_timestamp"] = last_reading_timestamp.replace(r"^(?i:nat|nan|none|<na>|null)$", "", regex=True)
    merged["last_reading_timestamp_sort"] = pd.to_datetime(
        ensure_series(merged.get("last_reading_timestamp_sort", merged["last_reading_timestamp"]), merged.index, ""),
        errors="coerce",
    )

    merged[slave_key] = normalize_id_series(merged.get(slave_key, pd.Series([""] * len(merged), index=merged.index)))

    location_name = merged.get("locationname", pd.Series([""] * len(merged), index=merged.index))
    if not isinstance(location_name, pd.Series):
        location_name = pd.Series([location_name] * len(merged), index=merged.index)
    merged["locationname"] = normalize_display_text_series(location_name, merged.index)

    building_name = merged.get("buildingname", pd.Series([""] * len(merged), index=merged.index))
    if not isinstance(building_name, pd.Series):
        building_name = pd.Series([building_name] * len(merged), index=merged.index)
    merged["buildingname"] = normalize_display_text_series(building_name, merged.index)
    merged["location_label"] = (merged["buildingname"] + " - " + merged["locationname"]).str.strip(" -")
    merged.loc[merged["location_label"] == "", "location_label"] = merged["locationname"]

    device_name_series = normalize_display_text_series(merged.get("device_name", ""), merged.index)

    device_type_icy_series = normalize_display_text_series(merged.get("device_type_icyname", ""), merged.index)

    merged["device_name"] = device_name_series.where(device_name_series.ne(""), device_type_icy_series)

    fallback_name = normalize_display_text_series(
        merged.get("slave_name", merged.get("device_name", merged.get("name", ""))),
        merged.index,
    )

    merged.loc[merged["location_label"] == "", "location_label"] = fallback_name
    merged.loc[merged["location_label"] == "", "location_label"] = "Onbekende locatie"

    channel_series = merged.get("channel", pd.Series([""] * len(merged), index=merged.index))
    if not isinstance(channel_series, pd.Series):
        channel_series = pd.Series([channel_series] * len(merged), index=merged.index)
    merged["channel"] = channel_series.fillna("").astype(str).str.strip()

    merged["status"] = ""
    if "slave_parent_deviceid" in merged.columns:
        inferred_mask = merged["logged_deviceid"].eq("") & merged["slave_parent_deviceid"].notna()
        merged.loc[inferred_mask, "status"] = "Afgeleid via SlaveDeviceID"

        mismatch_mask = (
            merged["logged_deviceid"].ne("")
            & merged["slave_parent_deviceid"].notna()
            & (normalize_id_series(merged["logged_deviceid"]) != normalize_id_series(merged["slave_parent_deviceid"]))
        )
        merged.loc[mismatch_mask, "status"] = "Mismatch tussen log en slavedevice"

    if merged.empty:
        for col, default_value in {
            "meter_variable": "",
            "meter_type_key": "",
            "meter_type_label": "",
            "meter_unit": "",
            "devicetype_code": "",
            "devicetype_name": "",
        }.items():
            merged[col] = default_value
    else:
        meter_info_df = merged.apply(
            lambda row: pd.Series(
                get_meter_type_variables(
                    row.get("devicetypeid", ""),
                    row.get("devicename", ""),
                    row.get("icyname", ""),
                    row.get("slave_metertype", ""),
                )
            ),
            axis=1,
        )
        merged = pd.concat([merged, meter_info_df], axis=1)

    merged["display_name"] = merged["location_label"]
    merged.loc[merged["display_name"] == "", "display_name"] = fallback_name
    merged.loc[merged["display_name"] == "", "display_name"] = merged["deviceid"]

    merged["search_text"] = (
        normalize_searchable_text_series(merged.get("display_name", ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get("location_label", ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get("locationname", ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get("buildingname", ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get("deviceid", ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get(slave_key, ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get("channel", ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get("status", ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get("devicetype_code", ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get("devicetype_name", ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get("meter_type_label", ""), merged.index) + " " +
        normalize_searchable_text_series(merged.get("meter_variable", ""), merged.index)
    ).str.strip()

    active_link_mask = (
        get_text_series(merged, "deviceid").ne("") |
        get_text_series(merged, "slavedeviceid").isin(set(normalize_id_series(slave_df.get("slavedeviceid", pd.Series(dtype=str)))))
    )
    active_link_mask = active_link_mask & get_text_series(merged, "location_label").ne("Onbekende locatie")
    merged = merged[active_link_mask].copy()

    merged = merged.drop_duplicates(
        subset=["deviceid", "slavedeviceid", "channel"],
        keep="first"
    ).reset_index(drop=True)

    merged["offset_edit_blocked"] = merged.apply(is_offset_edit_blocked, axis=1)
    merged["offset_edit_status"] = merged["offset_edit_blocked"].apply(
        lambda blocked: "Geblokkeerd (MID Campère)" if blocked else "Toegestaan"
    )

    return merged


# =========================
# SAVE / DELETE OFFSET
# =========================

def find_existing_offset(cur, device_id=None, slave_id=None):
    if slave_id:
        device_id = None
        cur.execute(
            f"SELECT pulsecounteroffsetid FROM {OFFSET_TABLE} WHERE slavedeviceid = %s LIMIT 1",
            (slave_id,)
        )
    else:
        cur.execute(
            f"SELECT pulsecounteroffsetid FROM {OFFSET_TABLE} WHERE deviceid = %s AND (slavedeviceid IS NULL OR slavedeviceid = '') LIMIT 1",
            (device_id,)
        )
    return cur.fetchone(), device_id, slave_id


def update_meterdivider(cur, device_id=None, slave_id=None, new_meterdivider=None, current_meterdivider=None):
    if new_meterdivider is None or pd.isna(new_meterdivider):
        return

    divider = get_normalized_meterdivider(new_meterdivider)
    current_divider = get_normalized_meterdivider(current_meterdivider or 1)

    if abs(divider - current_divider) < 1e-12:
        return

    if slave_id:
        cur.execute(
            f"UPDATE {SLAVE_TABLE} SET meterdivider = %s WHERE slavedeviceid = %s",
            (divider, slave_id),
        )
        write_runtime_log(f"Meterdivider gewijzigd van {current_divider} naar {divider}.", level="INFO", record={"slavedeviceid": slave_id, "deviceid": device_id})
    elif device_id:
        cur.execute(
            f"UPDATE {DEVICE_TABLE} SET meterdivider = %s WHERE deviceid = %s",
            (divider, device_id),
        )
        write_runtime_log(f"Meterdivider gewijzigd van {current_divider} naar {divider}.", level="INFO", record={"deviceid": device_id})


def save_offset(df):
    c = conn(st.session_state.get("db_name"))
    cur = c.cursor()
    comment_value = build_comment_value()

    try:
        for _, r in df.iterrows():
            if is_offset_edit_blocked(r):
                write_runtime_log(MID_PROTECTED_METER_MESSAGE, level="WARN", record=r)
                raise ValueError(MID_PROTECTED_METER_MESSAGE)

            device_id = str(r.get("deviceid", "")).strip() or None
            slave_id = str(r.get("slavedeviceid", "")).strip() or None
            channel = str(r.get("channel", "")).strip() or None
            current_meterdivider = get_normalized_meterdivider(r.get("current_meterdivider", r.get("meterdivider", 1)))
            new_meterdivider = r.get("new_meterdivider", current_meterdivider)
            current_offset_raw = r.get("offset_value_raw", None)
            if current_offset_raw is None or pd.isna(current_offset_raw):
                current_offset_raw = float(r.get("current_offset", 0) or 0) * current_meterdivider
            new_offset = r.get("new_offset", current_offset_raw)
            if new_offset is None or pd.isna(new_offset):
                new_offset = current_offset_raw
            new_offset = float(new_offset)

            existing, device_id, slave_id = find_existing_offset(cur, device_id=device_id, slave_id=slave_id)
            update_meterdivider(
                cur,
                device_id=device_id,
                slave_id=slave_id,
                new_meterdivider=new_meterdivider,
                current_meterdivider=current_meterdivider,
            )

            if abs(float(new_offset) - float(current_offset_raw or 0)) < 1e-12:
                write_runtime_log("Geen wijziging opgeslagen omdat de berekende nieuwe offset gelijk is aan de huidige offset.", level="INFO", record=r)
                continue

            if existing:
                cur.execute(
                    f"""
                    UPDATE {OFFSET_TABLE}
                    SET deviceid = %s,
                        slavedeviceid = %s,
                        channel = %s,
                        `offset` = %s,
                        comment = %s
                    WHERE pulsecounteroffsetid = %s
                    """,
                    (device_id, slave_id, channel, new_offset, comment_value, existing[0])
                )
                write_runtime_log(
                    f"Offset bijgewerkt: huidig={r.get('effective_reading', '')}, doel={r.get('new_meter_reading', '')}, divider={r.get('new_meterdivider', current_meterdivider)}, raw_offset={new_offset}, resultaat={r.get('resulting_effective_reading', '')}.",
                    level="INFO",
                    record=r,
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO {OFFSET_TABLE} (deviceid, slavedeviceid, channel, `offset`, comment)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (device_id, slave_id, channel, new_offset, comment_value)
                )
                write_runtime_log(
                    f"Nieuwe offset opgeslagen: huidig={r.get('effective_reading', '')}, doel={r.get('new_meter_reading', '')}, divider={r.get('new_meterdivider', current_meterdivider)}, raw_offset={new_offset}, resultaat={r.get('resulting_effective_reading', '')}.",
                    level="INFO",
                    record=r,
                )

        c.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        c.close()


def delete_offset(df):
    c = conn(st.session_state.get("db_name"))
    cur = c.cursor()
    deleted_count = 0

    try:
        for _, r in df.iterrows():
            if is_offset_edit_blocked(r):
                raise ValueError(MID_PROTECTED_METER_MESSAGE)

            device_id = str(r.get("deviceid", "")).strip() or None
            slave_id = str(r.get("slavedeviceid", "")).strip() or None

            existing, _, _ = find_existing_offset(cur, device_id=device_id, slave_id=slave_id)
            if existing:
                cur.execute(
                    f"DELETE FROM {OFFSET_TABLE} WHERE pulsecounteroffsetid = %s",
                    (existing[0],)
                )
                deleted_count += 1

        c.commit()
        return deleted_count
    finally:
        try:
            cur.close()
        except Exception:
            pass
        c.close()


def prepare_batch_preview(df, catalog):
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "new_meterdivider" not in df.columns and "meterdivider" in df.columns:
        df["new_meterdivider"] = df["meterdivider"]

    if "new_meter_reading" not in df.columns:
        df["new_meter_reading"] = pd.NA
    if "new_meterdivider" not in df.columns:
        df["new_meterdivider"] = pd.NA

    for col in ["deviceid", "slavedeviceid", "channel"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = normalize_id_series(df[col])

    df["desired"] = pd.to_numeric(df["new_meter_reading"], errors="coerce")
    df["target_meterdivider"] = pd.to_numeric(df["new_meterdivider"], errors="coerce")

    catalog = catalog.copy()
    for col in ["deviceid", "slavedeviceid", "channel"]:
        if col not in catalog.columns:
            catalog[col] = ""
        catalog[col] = normalize_id_series(catalog[col])

    preview_rows = []

    for _, src in df.iterrows():
        candidates = catalog.copy()
        device_has_slave_rows = False

        if src["slavedeviceid"]:
            candidates = candidates[candidates["slavedeviceid"] == src["slavedeviceid"]]
            if src["deviceid"]:
                candidates = candidates[candidates["deviceid"] == src["deviceid"]]
        elif src["deviceid"]:
            device_rows = candidates[candidates["deviceid"] == src["deviceid"]]
            device_has_slave_rows = bool((device_rows["slavedeviceid"].astype(str).str.strip() != "").any()) if not device_rows.empty else False
            candidates = device_rows[device_rows["slavedeviceid"].astype(str).str.strip() == ""]

        if src["channel"]:
            candidates = candidates[candidates["channel"] == src["channel"]]

        row = {
            "deviceid": src.get("deviceid", ""),
            "slavedeviceid": src.get("slavedeviceid", ""),
            "channel": src.get("channel", ""),
            "new_meter_reading": src.get("new_meter_reading", ""),
            "new_meterdivider": src.get("new_meterdivider", ""),
            "match_count": int(len(candidates)),
            "match_status": "",
            "status_detail": "",
            "location_label": "",
            "raw_reading": None,
            "raw_value": None,
            "current_meterdivider": 1,
            "meterdivider": 1,
            "current_offset": None,
            "effective_reading": None,
            "resulting_effective_reading": None,
            "new_offset": None,
            "meter_type_label": "",
            "devicetype_name": "",
            "devicetype_code": "",
            "meter_variable": "",
        }

        desired_value = src.get("new_meter_reading", "")
        divider_value = src.get("new_meterdivider", "")
        desired_input = "" if pd.isna(desired_value) else str(desired_value).strip()
        divider_input = "" if pd.isna(divider_value) else str(divider_value).strip()
        desired_provided = desired_input != ""
        divider_provided = divider_input != ""

        if desired_provided and pd.isna(src["desired"]):
            row["match_status"] = "Ongeldige meterstand"
            row["status_detail"] = "Kolom new_meter_reading bevat geen geldige numerieke waarde."
        elif divider_provided and pd.isna(src["target_meterdivider"]):
            row["match_status"] = "Ongeldige meterdivider"
            row["status_detail"] = "Kolom new_meterdivider bevat geen geldige positieve waarde."
        elif not desired_provided and not divider_provided:
            row["match_status"] = "Geen wijzigingen opgegeven"
            row["status_detail"] = "Er is geen nieuwe meterstand of nieuwe meterdivider opgegeven."
        elif len(candidates) == 1:
            match = candidates.iloc[0]
            current_meterdivider = get_normalized_meterdivider(match.get("meterdivider", 1))
            target_meterdivider = get_normalized_meterdivider(src["target_meterdivider"], current_meterdivider) if divider_provided else current_meterdivider
            raw_value = float(match.get("raw_value", float(match.get("raw_reading", 0) or 0) * current_meterdivider) or 0)
            current_offset_raw = float(match.get("offset_value_raw", float(match.get("current_offset", 0) or 0) * current_meterdivider) or 0)
            new_offset_raw = calculate_new_offset_raw(
                src["desired"] if desired_provided else None,
                raw_value,
                target_meterdivider,
                current_offset_raw,
            )
            row.update({
                "deviceid": match.get("deviceid", src.get("deviceid", "")),
                "slavedeviceid": match.get("slavedeviceid", src.get("slavedeviceid", "")),
                "channel": match.get("channel", src.get("channel", "")),
                "location_label": match.get("location_label", ""),
                "raw_reading": to_plain_value(match.get("raw_reading", 0)),
                "raw_value": to_plain_value(raw_value),
                "current_meterdivider": to_plain_value(current_meterdivider),
                "meterdivider": to_plain_value(target_meterdivider),
                "new_meterdivider": to_plain_value(target_meterdivider),
                "current_offset": to_plain_value(match.get("current_offset", 0)),
                "effective_reading": to_plain_value(match.get("effective_reading", 0)),
                "resulting_effective_reading": to_plain_value(calculate_effective_reading(raw_value, new_offset_raw, target_meterdivider)),
                "offset_value_raw": to_plain_value(current_offset_raw),
                "new_offset": to_plain_value(new_offset_raw),
                "meter_type_label": match.get("meter_type_label", ""),
                "devicetype_name": match.get("devicetype_name", ""),
                "devicetype_code": match.get("devicetype_code", ""),
                "meter_variable": match.get("meter_variable", ""),
                "match_status": "Geblokkeerd - MID Campère meter" if is_offset_edit_blocked(match) else "Klaar om op te slaan",
                "status_detail": "MID-gecertificeerde Campère meter; tonen mag, aanpassen niet." if is_offset_edit_blocked(match) else "Deze wijziging voldoet aan de controles en kan worden opgeslagen.",
            })
        elif len(candidates) == 0:
            if src.get("deviceid", "") and not src.get("slavedeviceid", "") and device_has_slave_rows:
                row["match_status"] = "DeviceID heeft Slavedevices - gebruik SlavedeviceID"
                row["status_detail"] = "Dit DeviceID hoort bij een controller met meerdere slaves; gebruik daarom SlavedeviceID."
            else:
                row["match_status"] = "Niet gevonden"
                row["status_detail"] = "Geen match gevonden in de huidige database voor de opgegeven invoer."
        else:
            sample_locations = [str(v).strip() for v in candidates.get("location_label", pd.Series(dtype=str)).dropna().tolist() if str(v).strip()]
            row["location_label"] = " | ".join(sample_locations[:3])
            row["match_status"] = "Meerdere directe matches - controleer invoer"
            row["status_detail"] = "Er zijn meerdere mogelijke matches gevonden; maak de invoer specifieker met SlavedeviceID of channel."

        preview_rows.append(row)

    return pd.DataFrame(preview_rows)


# =========================
# MAIN
# =========================

def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="🧊", layout="wide")

    st.markdown(
        """
        <style>
        .icy-title {
            text-align: left;
            margin-top: 0.15rem;
            margin-bottom: 0;
        }
        .icy-subtitle {
            text-align: left;
            color: #6b7280;
            margin-top: 0.15rem;
            margin-bottom: 1rem;
        }
        .icy-static-table {
            overflow: auto;
            border: 1px solid rgba(128, 128, 128, 0.25);
            border-radius: 0.6rem;
            margin-bottom: 0.75rem;
        }
        .icy-static-table table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.92rem;
        }
        .icy-static-table thead th {
            position: sticky;
            top: 0;
            z-index: 1;
            background: #0f172a;
            color: #f8fafc;
            text-align: left;
            padding: 0.55rem 0.7rem;
            border-bottom: 1px solid rgba(128, 128, 128, 0.35);
        }
        .icy-static-table tbody td {
            padding: 0.45rem 0.7rem;
            border-bottom: 1px solid rgba(128, 128, 128, 0.12);
        }
        .icy-static-table tbody tr:nth-child(even) {
            background: rgba(148, 163, 184, 0.05);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if LOGO_PATH.exists():
        st.image(str(LOGO_PATH), width=150)

    st.markdown(f"<h1 class='icy-title'>{APP_TITLE}</h1>", unsafe_allow_html=True)
    st.markdown("<div class='icy-subtitle'>ICY Pulse Counter meterstanden beheren via offsets</div>", unsafe_allow_html=True)

    restore_persisted_state()

    with st.expander("Database selectie", expanded=True):
        available_hosts = get_available_db_hosts()
        host_options = ["auto"] + available_hosts

        with st.form("db_selection_form"):
            selected_host = st.selectbox(
                "Database host selectie",
                options=host_options,
                format_func=lambda x: "Automatisch: probeer alle gevonden hosts" if x == "auto" else x,
                index=host_options.index(st.session_state.get("db_host_override", "auto")) if st.session_state.get("db_host_override", "auto") in host_options else 0,
            )
            manual_host_input = st.text_input("Database host", value=str(st.session_state.get("db_host_manual", "")).strip() or cfg("DB_HOST", ""), placeholder="bijvoorbeeld icyccdb.icy.nl")
            db_name_input = st.text_input("Database naam", value=str(st.session_state.get("db_name", "")).strip() or cfg("DB_NAME", ""), placeholder="bijvoorbeeld nl_ackersate")
            db_user_input = st.text_input("Database gebruiker", value=str(st.session_state.get("db_user", "")).strip() or cfg("DB_USER", "root"))
            db_password_input = st.text_input("Database wachtwoord", value=st.session_state.get("db_password", "") or cfg("DB_PASSWORD", ""), type="password")
            initials_input = st.text_input("Initialen", value=str(st.session_state.get("user_initials", "")).strip() or get_default_initials(), max_chars=6)
            location_input = st.text_input("Location filter (optioneel)", value=st.session_state.get("location_filter", ""))
            device_id_input = st.text_input("DeviceID (optioneel)", value=st.session_state.get("device_filter", ""))
            slave_device_id_input = st.text_input("SlaveDeviceID (optioneel)", value=st.session_state.get("slave_filter", ""))
            mid_filter_input = st.selectbox(
                "MID filter",
                options=["Alle meters", "Alleen NON MID", "Alleen MID"],
                index=["Alle meters", "Alleen NON MID", "Alleen MID"].index(
                    st.session_state.get("mid_filter", "Alle meters")
                ) if st.session_state.get("mid_filter", "Alle meters") in ["Alle meters", "Alleen NON MID", "Alleen MID"] else 0,
            )
            submitted = st.form_submit_button("Database laden")

        if submitted:
            previous_db_name = str(st.session_state.get("db_name", "")).strip()
            new_db_name = db_name_input.strip()

            st.session_state["db_host_override"] = selected_host
            st.session_state["db_host_manual"] = manual_host_input.strip()
            st.session_state["db_name"] = new_db_name
            st.session_state["db_user"] = db_user_input.strip()
            st.session_state["db_password"] = db_password_input
            st.session_state["user_initials"] = initials_input.strip().upper()
            st.session_state["location_filter"] = location_input.strip()
            st.session_state["device_filter"] = device_id_input.strip()
            st.session_state["slave_filter"] = slave_device_id_input.strip()
            st.session_state["mid_filter"] = mid_filter_input
            st.session_state["db_ready"] = True
            st.session_state["manual"] = None
            st.session_state["current_record_index"] = 0

            if previous_db_name and previous_db_name != new_db_name:
                st.session_state["search_text"] = ""
                st.session_state["selected_location"] = "Alle locaties"

            sync_persisted_state()

    if not st.session_state.get("db_ready"):
        st.info("Vul eerst de database naam in. Eventueel kun je direct DeviceID en SlaveDeviceID meegeven.")
        st.stop()

    if not st.session_state.get("db_name"):
        st.warning("Database naam is verplicht.")
        st.stop()

    if not str(st.session_state.get("db_host_manual", "")).strip() and not get_available_db_hosts():
        st.warning("Database host is verplicht. Vul een host in of zet DB_HOST in de .env.")
        st.stop()

    if not str(st.session_state.get("user_initials", "")).strip():
        st.warning("Initialen zijn verplicht.")
        st.stop()

    try:
        db_name = st.session_state["db_name"]
        host_choice = st.session_state.get("db_host_override", "auto")
        log = load(LOG_TABLE, db_name, host_choice)
        slave = load_optional(SLAVE_TABLE, db_name, host_choice)
        device = load_optional(DEVICE_TABLE, db_name, host_choice)
        location = load_optional(LOCATION_TABLE, db_name, host_choice)
        buildingtype = load_optional(BUILDINGTYPE_TABLE, db_name, host_choice)
        devicetype = load_optional(DEVICETYPE_TABLE, db_name, host_choice)
        offset = load_optional(OFFSET_TABLE, db_name, host_choice)

        catalog = build_catalog(log, slave, offset, device, location, buildingtype, devicetype)

    except Exception as e:
        st.error(e)
        st.stop()

    active_host = st.session_state.get("active_db_host", "onbekend")
    info_col, refresh_col = st.columns([6, 1])
    info_col.caption(f"Actieve database: {st.session_state['db_name']} | Host: {active_host}")
    if refresh_col.button(
        "🔄 Data verversen",
        help="Laad de tabel opnieuw uit de database",
        type="secondary",
        width="stretch",
    ):
        st.cache_data.clear()
        st.session_state["manual"] = None
        st.rerun()

    search = st.text_input("Zoek op locatie, device of meter type", key="search_text")

    filtered = catalog.copy()

    device_filter = st.session_state.get("device_filter", "").strip().lower()
    if device_filter:
        filtered = filtered[
            filtered["deviceid"].astype(str).str.lower().str.contains(device_filter, na=False)
        ]

    slave_filter = st.session_state.get("slave_filter", "").strip().lower()
    if slave_filter and "slavedeviceid" in filtered.columns:
        filtered = filtered[
            filtered["slavedeviceid"].astype(str).str.lower().str.contains(slave_filter, na=False)
        ]

    location_filter = normalize_searchable_text(st.session_state.get("location_filter", ""))
    if location_filter and "location_label" in filtered.columns:
        location_search_series = (
            normalize_searchable_text_series(filtered.get("location_label", ""), filtered.index) + " " +
            normalize_searchable_text_series(filtered.get("locationname", ""), filtered.index) + " " +
            normalize_searchable_text_series(filtered.get("buildingname", ""), filtered.index)
        ).str.strip()
        filtered = filtered[
            location_search_series.str.contains(location_filter, na=False)
        ]

    if search:
        s = normalize_searchable_text(search)
        filtered = filtered[
            filtered["search_text"].fillna("").astype(str).str.contains(s, na=False)
        ]

    mid_filter = st.session_state.get("mid_filter", "Alle meters")
    if mid_filter == "Alleen NON MID" and "offset_edit_blocked" in filtered.columns:
        filtered = filtered[~filtered["offset_edit_blocked"].fillna(False)]
    elif mid_filter == "Alleen MID" and "offset_edit_blocked" in filtered.columns:
        filtered = filtered[filtered["offset_edit_blocked"].fillna(False)]

    if "location_label" in filtered.columns:
        location_options = ["Alle locaties"] + sorted([loc for loc in filtered["location_label"].dropna().astype(str).unique().tolist() if loc.strip()])
        if st.session_state.get("selected_location", "Alle locaties") not in location_options:
            st.session_state["selected_location"] = "Alle locaties"
        selected_location = st.selectbox("Geselecteerde locatie", options=location_options, key="selected_location")
        if selected_location != "Alle locaties":
            filtered = filtered[filtered["location_label"].astype(str) == selected_location]

    sort_options = {}
    if "location_label" in filtered.columns:
        sort_options["Locatie"] = "location_label"
    elif "locationname" in filtered.columns:
        sort_options["Locatie"] = "locationname"
    if "deviceid" in filtered.columns:
        sort_options["DeviceID"] = "deviceid"
    if "device_name" in filtered.columns:
        sort_options["Device naam"] = "device_name"
    if "slavedeviceid" in filtered.columns:
        sort_options["SlaveDeviceID"] = "slavedeviceid"
    if "devicetype_name" in filtered.columns:
        sort_options["Meter Type"] = "devicetype_name"
    if "meterdivider" in filtered.columns:
        sort_options["Meterdivider"] = "meterdivider"
    if "raw_reading" in filtered.columns:
        sort_options["Ruwe meterstand"] = "raw_reading"
    if "last_reading_timestamp_sort" in filtered.columns:
        sort_options["Laatste meterstand"] = "last_reading_timestamp_sort"
    elif "last_reading_timestamp" in filtered.columns:
        sort_options["Laatste meterstand"] = "last_reading_timestamp"
    if "current_offset" in filtered.columns:
        sort_options["Huidige offset"] = "current_offset"
    if "effective_reading" in filtered.columns:
        sort_options["Effectieve meterstand"] = "effective_reading"

    if sort_options:
        sort_col_ui, sort_dir_ui = st.columns([2, 1])
        sort_labels = list(sort_options.keys())
        default_sort = st.session_state.get("sort_column_ui", sort_labels[0])
        if default_sort not in sort_options:
            default_sort = sort_labels[0]
        sort_label = sort_col_ui.selectbox(
            "Sorteer tabel op",
            options=sort_labels,
            index=sort_labels.index(default_sort),
            key="sort_column_ui",
        )
        sort_direction = sort_dir_ui.selectbox(
            "Richting",
            options=["Oplopend", "Aflopend"],
            index=0 if st.session_state.get("sort_direction_ui", "Oplopend") == "Oplopend" else 1,
            key="sort_direction_ui",
        )

        primary_sort = sort_options[sort_label]
        fallback_sorts = [col for col in ["location_label", "deviceid", "slavedeviceid"] if col in filtered.columns and col != primary_sort]
        filtered = filtered.sort_values(
            by=[primary_sort] + fallback_sorts,
            ascending=[sort_direction == "Oplopend"] + [True] * len(fallback_sorts),
            na_position="last",
        ).reset_index(drop=True)
    else:
        filtered = filtered.reset_index(drop=True)

    sync_persisted_state()

    display_cols = []
    if "location_label" in filtered.columns:
        display_cols.append("location_label")
    elif "locationname" in filtered.columns:
        display_cols.append("locationname")
    if "deviceid" in filtered.columns:
        display_cols.append("deviceid")
    if "device_name" in filtered.columns:
        display_cols.append("device_name")
    if "slavedeviceid" in filtered.columns:
        display_cols.append("slavedeviceid")
    if "devicetype_name" in filtered.columns:
        display_cols.append("devicetype_name")
    if "meterdivider" in filtered.columns:
        display_cols.append("meterdivider")
    if "last_reading_timestamp" in filtered.columns:
        display_cols.append("last_reading_timestamp")
    if "offset_edit_status" in filtered.columns:
        display_cols.append("offset_edit_status")
    display_cols += ["raw_reading", "current_offset", "effective_reading"]
    display_cols = [col for col in display_cols if col in filtered.columns]

    display_df = filtered[display_cols].rename(columns={
        "location_label": "Location",
        "locationname": "Location",
        "deviceid": "DeviceID",
        "device_name": "Device naam",
        "slavedeviceid": "SlaveDeviceID",
        "devicetype_name": "Meter Type",
        "meterdivider": "Meterdivider",
        "last_reading_timestamp": "Laatste meterstand tijdstip",
        "offset_edit_status": "Aanpassen",
        "raw_reading": "Ruwe meterstand",
        "current_offset": "Huidige offset",
        "effective_reading": "Effectieve meterstand",
    })

    render_static_table(display_df, max_height=520)

    unknown_location_count = int((filtered.get("location_label", pd.Series(dtype=str)).astype(str) == "Onbekende locatie").sum()) if not filtered.empty else 0
    if unknown_location_count:
        st.caption(f"{unknown_location_count} record(s) hebben geen locatiekoppeling in de database en zijn gemarkeerd als 'Onbekende locatie'.")

    # =========================
    # MANUAL
    # =========================
    tab1, tab2 = st.tabs(["Handmatig", "Batch"])

    with tab1:

        if filtered.empty:
            st.info("Geen records gevonden voor de huidige selectie.")
        else:
            if st.session_state["current_record_index"] >= len(filtered):
                st.session_state["current_record_index"] = 0

            nav_prev, nav_next, nav_info = st.columns([1, 1, 3])
            if nav_prev.button("Vorige"):
                st.session_state["current_record_index"] = (st.session_state["current_record_index"] - 1) % len(filtered)
                sync_persisted_state()
            if nav_next.button("Volgende"):
                st.session_state["current_record_index"] = (st.session_state["current_record_index"] + 1) % len(filtered)
                sync_persisted_state()

            row = filtered.iloc[st.session_state["current_record_index"]]
            current_location = str(row.get("location_label", "")).strip() or "Onbekende locatie"
            cycle_location = st.session_state.get("selected_location", "Alle locaties")

            nav_info.caption(
                f"Record {st.session_state['current_record_index'] + 1} van {len(filtered)}"
                + (f" • {current_location}" if current_location else "")
            )
            st.info(f"Locatie in cyclus: {cycle_location if cycle_location != 'Alle locaties' else current_location}")
            st.caption(
                f"Huidig record → Locatie: {current_location} | DeviceID: {row.get('deviceid', '-') or '-'} | Device naam: {row.get('device_name', '-') or '-'} | SlaveDeviceID: {row.get('slavedeviceid', '-') or '-'} | Meter Type: {row.get('devicetype_name', '-') or '-'}"
            )

            is_locked = bool(row.get("offset_edit_blocked", False)) or is_offset_edit_blocked(row)
            if is_locked:
                st.session_state["manual"] = None
                st.warning(MID_PROTECTED_METER_MESSAGE)

            current_meterdivider = get_normalized_meterdivider(row.get("meterdivider", 1))
            raw_value = float(row.get("raw_value", row.get("raw_reading", 0)) or 0)
            current_offset_raw = float(row.get("offset_value_raw", float(row.get("current_offset", 0) or 0) * current_meterdivider) or 0)

            divider_toggle_col, divider_input_col = st.columns([1, 1])
            change_divider = divider_toggle_col.checkbox(
                "Meterdivider aanpassen",
                value=False,
                key=f"change_divider_{row.get('deviceid', '')}_{row.get('slavedeviceid', '')}_{row.get('channel', '')}",
                disabled=is_locked,
            )
            new_meterdivider = current_meterdivider
            if change_divider:
                new_meterdivider = divider_input_col.number_input(
                    "Nieuwe meterdivider",
                    min_value=1.0,
                    value=float(current_meterdivider),
                    step=1.0,
                    format="%g",
                    key=f"meterdivider_{row.get('deviceid', '')}_{row.get('slavedeviceid', '')}_{row.get('channel', '')}",
                    disabled=is_locked,
                )

            recalculated_effective = calculate_effective_reading(raw_value, current_offset_raw, new_meterdivider)
            st.caption(
                f"Meterdivider: {current_meterdivider:g}" + (f" → {new_meterdivider:g}" if change_divider else "")
                + f" | Huidige effectieve meterstand bij deze divider: {recalculated_effective:.6g}"
            )

            desired = st.number_input(
                "Nieuwe meterstand",
                value=float(recalculated_effective),
                key=f"desired_{row.get('deviceid', '')}_{row.get('slavedeviceid', '')}_{row.get('channel', '')}_{str(new_meterdivider).replace('.', '_')}",
                disabled=is_locked,
            )

            preview_col, push_col, save_col, delete_col = st.columns(4)
            record_payload = {
                "deviceid": row.get("deviceid", ""),
                "slavedeviceid": row.get("slavedeviceid", ""),
                "locationname": row.get("location_label", row.get("locationname", "")),
                "raw_reading": to_plain_value(row["raw_reading"]),
                "raw_value": to_plain_value(raw_value),
                "current_meterdivider": to_plain_value(current_meterdivider),
                "meterdivider": to_plain_value(new_meterdivider),
                "new_meterdivider": to_plain_value(new_meterdivider),
                "current_offset": to_plain_value(row["current_offset"]),
                "offset_value_raw": to_plain_value(current_offset_raw),
                "new_offset": to_plain_value(calculate_new_offset_raw(desired, raw_value, new_meterdivider, current_offset_raw)),
                "meter_type_label": row.get("meter_type_label", ""),
                "devicetype_name": row.get("devicetype_name", ""),
                "devicetype_code": row.get("devicetype_code", ""),
                "meter_variable": row.get("meter_variable", ""),
                "channel": row.get("channel", ""),
            }

            if preview_col.button("Preview", disabled=is_locked):
                st.session_state["manual"] = record_payload

            push_confirm = st.checkbox(
                "Bevestig toevoegen aan batchwachtrij (nog niet opslaan)",
                value=False,
                key=f"confirm_push_batch_{row.get('deviceid', '')}_{row.get('slavedeviceid', '')}_{row.get('channel', '')}",
                disabled=is_locked,
            )
            if push_col.button("Push selectie naar batch", disabled=is_locked or not push_confirm):
                try:
                    staged_row = build_batch_staging_row(record_payload, desired_meter_reading=None, new_meterdivider=new_meterdivider)
                    staged_rows, action = upsert_batch_staging_rows(st.session_state.get("batch_staging", []), staged_row)
                    st.session_state["batch_staging"] = staged_rows
                    write_runtime_log(
                        f"Selectie toegevoegd aan batchwachtrij ({action}). Nieuwe meterstand moet nog in Batch worden ingevuld; divider={staged_row.get('new_meterdivider', '')}.",
                        level="INFO",
                        record=staged_row,
                    )
                    st.success(f"Selectie staat klaar in Batch ({len(staged_rows)} regel(s) in wachtrij). Vul daar nog de gewenste meterstand in.")
                except Exception as e:
                    st.error(e)

            if st.session_state["manual"]:
                st.success("Preview klaar")
                st.json(st.session_state["manual"])

            if save_col.button("Opslaan en volgende", disabled=is_locked):
                if not st.session_state.get("manual"):
                    st.warning("Eerst preview maken")
                else:
                    save_offset(pd.DataFrame([st.session_state["manual"]]))
                    st.cache_data.clear()
                    st.session_state["manual"] = None
                    st.session_state["current_record_index"] = (st.session_state["current_record_index"] + 1) % len(filtered)
                    sync_persisted_state()
                    st.success("Opgeslagen, volgende record geladen")
                    st.rerun()

            has_current_offset = abs(float(row.get("current_offset", 0) or 0)) > 0
            confirm_delete = delete_col.checkbox(
                "Bevestig verwijderen",
                key=f"confirm_delete_{row.get('deviceid', '')}_{row.get('slavedeviceid', '')}_{row.get('channel', '')}",
                disabled=is_locked or not has_current_offset,
            )
            if delete_col.button("Huidige offset verwijderen", disabled=is_locked or not has_current_offset or not confirm_delete):
                deleted = delete_offset(pd.DataFrame([record_payload]))
                st.cache_data.clear()
                st.session_state["manual"] = None
                if deleted:
                    st.success("Huidige offset verwijderd")
                else:
                    st.info("Geen opgeslagen offset gevonden om te verwijderen")
                st.rerun()
    # =========================
    # BATCH
    # =========================
    with tab2:
        st.subheader("Batch import")
        st.write("Upload een Excel-bestand met minimaal een nieuwe meterstand. Als SlavedeviceID is ingevuld, wordt die altijd gebruikt. DeviceID wordt alleen gebruikt voor rechtstreekse meters zonder SlavedeviceID.")

        staged_rows = st.session_state.get("batch_staging", [])
        edited_staged_df = None
        if staged_rows:
            st.markdown("#### Batchwachtrij vanuit selectie")
            st.caption("Deze wachtrij is nog niet opgeslagen in de database. Pas hier de nieuwe meterstand en eventueel de divider aan; pas na de bevestiging onderaan wordt er echt opgeslagen.")
            staged_df = get_batch_staging_editor_df(staged_rows)
            edited_staged_df = st.data_editor(
                staged_df,
                hide_index=True,
                use_container_width=True,
                disabled=["deviceid", "slavedeviceid", "channel"],
                key="batch_staging_editor",
            )
            st.session_state["batch_staging"] = get_batch_staging_editor_df(edited_staged_df).to_dict("records")

            queue_col1, queue_col2 = st.columns([2, 1])
            use_staged_queue = queue_col1.checkbox(
                f"Gebruik batchwachtrij ({len(st.session_state.get('batch_staging', []))} regel(s))",
                value=True,
                key="use_staged_queue",
            )
            clear_queue_confirm = queue_col2.checkbox(
                "Bevestig legen",
                value=False,
                key="clear_batch_queue_confirm",
            )
            if queue_col2.button("Wachtrij legen", disabled=not clear_queue_confirm):
                st.session_state["batch_staging"] = []
                write_runtime_log("Batchwachtrij handmatig geleegd.", level="INFO")
                st.success("Batchwachtrij geleegd.")
                st.rerun()
        else:
            use_staged_queue = False

        template_df = pd.DataFrame([
            {"slavedeviceid": "50", "deviceid": "25", "new_meter_reading": 1500, "new_meterdivider": 100},
            {"slavedeviceid": "11174", "deviceid": "", "new_meter_reading": 3200, "new_meterdivider": ""},
            {"slavedeviceid": "", "deviceid": "9", "new_meter_reading": 8750, "new_meterdivider": 1000},
        ])
        template_buffer = BytesIO()
        with pd.ExcelWriter(template_buffer, engine="openpyxl") as writer:
            template_df.to_excel(writer, index=False, sheet_name="Template")

        st.download_button(
            "Download template Excel",
            data=template_buffer.getvalue(),
            file_name="pulse_counter_batch_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.caption("Aanbevolen kolommen: SlavedeviceID, DeviceID, new_meter_reading en optioneel new_meterdivider. Heeft een meter een SlavedeviceID, gebruik dan die waarde. DeviceID alleen is bedoeld voor meters die geen SlavedeviceID hebben.")

        file = st.file_uploader("Excel of CSV upload", type=["xlsx", "csv"])

        source_df = None
        if file:
            if file.name.lower().endswith(".csv"):
                source_df = pd.read_csv(file)
            else:
                source_df = pd.read_excel(file)

        if use_staged_queue and st.session_state.get("batch_staging"):
            source_df = get_batch_staging_editor_df(edited_staged_df if edited_staged_df is not None else st.session_state.get("batch_staging", []))

        if source_df is not None:
            try:
                preview = prepare_batch_preview(source_df, catalog)
                valid_rows = preview[preview["match_status"] == "Klaar om op te slaan"].copy()
                blocked_rows = preview[preview["match_status"].str.contains("Geblokkeerd", na=False)].copy()
                ambiguous_rows = preview[preview["match_status"].str.contains("Meerdere matches", na=False)].copy()
                missing_rows = preview[preview["match_status"] == "Niet gevonden"].copy()
                invalid_rows = preview[preview["match_status"] == "Ongeldige meterstand"].copy()

                st.info(
                    f"Klaar: {len(valid_rows)} | Geblokkeerd: {len(blocked_rows)} | Meerdere matches: {len(ambiguous_rows)} | Niet gevonden: {len(missing_rows)} | Ongeldig: {len(invalid_rows)}"
                )
                st.caption("De tool schrijft ook een lokale log met redenen van skips, blokkades en opslagacties in je Documenten/ICY-Logs map.")
                render_static_table(get_batch_preview_display_df(preview), max_height=420)
                with st.expander("Toon laatste batchlog"):
                    st.text_area("Batchlog", value=read_runtime_log_tail(), height=220, disabled=True)

                if not blocked_rows.empty:
                    st.warning("Offsets voor MID gecertificeerde ICY 4850 Campère meters zijn geblokkeerd en worden niet opgeslagen.")
                if not ambiguous_rows.empty:
                    st.warning("Sommige regels zijn nog niet specifiek genoeg. Controleer de invoer of gebruik het juiste SlavedeviceID.")
                if not missing_rows.empty:
                    st.warning("Sommige regels zijn niet gevonden in de gekozen database/host.")
                if not invalid_rows.empty:
                    st.warning("Sommige regels hebben geen geldige nieuwe meterstand.")

                batch_confirm = st.checkbox(
                    f"Ja, ik weet 100% zeker dat ik {len(valid_rows)} wijziging(en) wil opslaan.",
                    value=False,
                    disabled=valid_rows.empty,
                    key=f"batch_confirm_{len(valid_rows)}",
                )
                if not valid_rows.empty:
                    st.warning("Controleer de preview zorgvuldig. Batch opslaan kan in één keer veel offsets wijzigen.")

                if st.button("Batch opslaan", disabled=valid_rows.empty or not batch_confirm):
                    start_batch_log(f"{len(valid_rows)} geldige regel(s)")
                    for _, r in preview.iterrows():
                        status = str(r.get("match_status", "")).strip()
                        detail = str(r.get("status_detail", "")).strip()
                        if status and status != "Klaar om op te slaan":
                            write_runtime_log(f"Batchregel niet opgeslagen: {status}. {detail}", level="WARN", record=r)
                    save_offset(valid_rows)
                    write_runtime_log(f"Batch opgeslagen met {len(valid_rows)} geldige regel(s).", level="INFO")
                    st.success(f"Batch opgeslagen: {len(valid_rows)} regels")
            except Exception as e:
                write_runtime_log(f"Batch verwerking mislukt: {e}", level="ERROR")
                st.error(e)


if __name__ == "__main__":
    main()