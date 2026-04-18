from datetime import datetime
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "python" / "Pulse Counter Offset Tool" / "pulse_counter_offset_tool.py"
spec = importlib.util.spec_from_file_location("pulse_counter_offset_tool", MODULE_PATH)
pulse_tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pulse_tool)


class FixedDatetime(datetime):
    @classmethod
    def now(cls):
        return cls(2026, 4, 16, 12, 0, 0)


class FakeCursor:
    def __init__(self, fetchone_results=None):
        self.queries = []
        self.fetchone_results = list(fetchone_results or [])

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def fake_streamlit(initials="HN", session_state=None, secrets=None, query_params=None):
    base_state = {"user_initials": initials, "db_name": "test_db"}
    if session_state:
        base_state.update(session_state)
    return SimpleNamespace(
        session_state=base_state,
        secrets=secrets or {},
        query_params=query_params or {},
        cache_data=SimpleNamespace(clear=lambda: None),
    )


def test_build_comment_value_uses_date_and_initials(monkeypatch):
    monkeypatch.setattr(pulse_tool, "st", fake_streamlit("HN"))
    monkeypatch.setattr(pulse_tool, "datetime", FixedDatetime)

    assert pulse_tool.build_comment_value() == "16-04-2026 HN"


def test_build_comment_value_requires_initials(monkeypatch):
    monkeypatch.setattr(pulse_tool, "st", fake_streamlit(""))
    monkeypatch.setattr(pulse_tool, "datetime", FixedDatetime)

    try:
        pulse_tool.build_comment_value()
        assert False, "Expected ValueError when initials are missing"
    except ValueError as exc:
        assert "Initialen zijn verplicht" in str(exc)


def test_write_runtime_log_records_message_and_record_reference(tmp_path, monkeypatch):
    log_path = tmp_path / "pulse_counter_offset_tool.log"
    monkeypatch.setattr(pulse_tool, "RUNTIME_LOG_DIR", tmp_path)
    monkeypatch.setattr(pulse_tool, "RUNTIME_LOG_PATH", log_path)
    monkeypatch.setattr(pulse_tool, "datetime", FixedDatetime)

    pulse_tool.write_runtime_log(
        "Batchregel niet opgeslagen: Niet gevonden.",
        level="WARN",
        record={"slavedeviceid": "45", "deviceid": "25", "channel": "1"},
    )

    contents = log_path.read_text(encoding="utf-8")
    assert "[WARN] Batchregel niet opgeslagen: Niet gevonden." in contents
    assert "slavedeviceid=45" in contents
    assert "deviceid=25" in contents


def test_read_runtime_log_tail_returns_only_latest_batch_section(tmp_path, monkeypatch):
    log_path = tmp_path / "pulse_counter_offset_tool.log"
    monkeypatch.setattr(pulse_tool, "RUNTIME_LOG_DIR", tmp_path)
    monkeypatch.setattr(pulse_tool, "RUNTIME_LOG_PATH", log_path)

    log_path.write_text(
        "=== BATCH START 2026-04-16 12:00:00 ===\nold line\n=== BATCH START 2026-04-18 15:01:58 ===\nnew line 1\nnew line 2\n",
        encoding="utf-8",
    )

    tail = pulse_tool.read_runtime_log_tail()

    assert "new line 1" in tail
    assert "new line 2" in tail
    assert "old line" not in tail


def test_format_table_value_trims_only_unnecessary_decimals():
    assert pulse_tool.format_table_value(1000.0) == "1000"
    assert pulse_tool.format_table_value(294.0) == "294"
    assert pulse_tool.format_table_value(0.897) == "0.897"
    assert pulse_tool.format_table_value(24.256) == "24.256"


def test_normalize_searchable_text_supports_location_search():
    assert pulse_tool.normalize_searchable_text("Villa - 14") == "villa 14"
    assert pulse_tool.normalize_searchable_text("Aardappelpand -- 023") == "aardappelpand 023"


def test_restore_persisted_state_prefills_credentials_and_initials_from_env(monkeypatch):
    fake_st = fake_streamlit(
        initials="",
        session_state={},
        secrets={
            "DB_HOST": "icyccdb.icy.nl",
            "DB_USER": "tester",
            "DB_PASSWORD": "secret123",
            "USER_INITIALS": "hn",
        },
        query_params={},
    )
    fake_st.session_state = {}

    monkeypatch.setattr(pulse_tool, "st", fake_st)

    pulse_tool.restore_persisted_state()

    assert fake_st.session_state["db_host_manual"] == "icyccdb.icy.nl"
    assert fake_st.session_state["db_user"] == "tester"
    assert fake_st.session_state["db_password"] == "secret123"
    assert fake_st.session_state["user_initials"] == "HN"


def test_restore_persisted_state_does_not_reapply_cleared_search_from_query(monkeypatch):
    fake_st = fake_streamlit(
        session_state={"search_text": "", "selected_location": "Alle locaties"},
        query_params={"search_text": "Kampeerplek-001", "selected_location": "Kampeerplek-001"},
    )

    monkeypatch.setattr(pulse_tool, "st", fake_st)

    pulse_tool.restore_persisted_state()

    assert fake_st.session_state["search_text"] == ""
    assert fake_st.session_state["selected_location"] == "Alle locaties"


def test_build_catalog_prefers_slave_offset_above_device_offset():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 336, "timestamp": "2026-04-16", "deviceid": None, "slavedeviceid": "10002", "channel": None},
            {"pulsecounterlogid": 2, "value": 88, "timestamp": "2026-04-16", "deviceid": "9", "slavedeviceid": None, "channel": None},
        ]
    )
    slave_df = pd.DataFrame([
        {"slavedeviceid": "10002", "deviceid": "2", "locationid": "31", "name": "Slave 10002"}
    ])
    offset_df = pd.DataFrame(
        [
            {"deviceid": "2", "slavedeviceid": None, "offset": 5},
            {"deviceid": None, "slavedeviceid": "10002", "offset": 1234},
            {"deviceid": "9", "slavedeviceid": None, "offset": 7},
        ]
    )
    device_df = pd.DataFrame(
        [
            {"deviceid": "2", "locationid": "31", "name": "Main device 2"},
            {"deviceid": "9", "locationid": "16", "name": "Direct device 9"},
        ]
    )
    location_df = pd.DataFrame(
        [
            {"locationid": "31", "locationname": "004", "buildingtypeid": "1"},
            {"locationid": "16", "locationname": "016", "buildingtypeid": "2"},
        ]
    )
    buildingtype_df = pd.DataFrame(
        [
            {"buildingtypeid": "1", "buildingname": "Kampeerplek"},
            {"buildingtypeid": "2", "buildingname": "Camperplek"},
        ]
    )

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df)

    slave_row = catalog[catalog["slavedeviceid"] == "10002"].iloc[0]
    direct_row = catalog[(catalog["deviceid"] == "9") & (catalog["slavedeviceid"] == "")].iloc[0]

    assert slave_row["current_offset"] == 1234
    assert slave_row["effective_reading"] == 1570
    assert direct_row["current_offset"] == 7


def test_build_catalog_includes_latest_reading_timestamp():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 100, "timestamp": "2026-04-15 08:00:00", "deviceid": "8", "slavedeviceid": None, "channel": None},
            {"pulsecounterlogid": 2, "value": 125, "timestamp": "2026-04-16 09:30:45", "deviceid": "8", "slavedeviceid": None, "channel": None},
        ]
    )
    slave_df = pd.DataFrame()
    offset_df = pd.DataFrame()
    device_df = pd.DataFrame([{"deviceid": "8", "locationid": "15", "name": "Camper module", "devicetypeid": "60"}])
    location_df = pd.DataFrame([{"locationid": "15", "locationname": "15", "buildingtypeid": "2"}])
    buildingtype_df = pd.DataFrame([{"buildingtypeid": "2", "buildingname": "Camperplek"}])
    devicetype_df = pd.DataFrame(
        [
            {"devicetypeid": "60", "devid": "4518", "devicename": "CAMPMODULE", "icyname": "ICY4518 Campère module"},
        ]
    )

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)

    assert len(catalog) == 1
    assert catalog.iloc[0]["last_reading_timestamp"] == "2026-04-16 09:30:45"


def test_build_catalog_applies_meterdivider_to_display_values():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 2103, "timestamp": "2026-04-16", "deviceid": "8", "slavedeviceid": None, "channel": None},
        ]
    )
    slave_df = pd.DataFrame()
    offset_df = pd.DataFrame([{"deviceid": "8", "slavedeviceid": None, "offset": 897}])
    device_df = pd.DataFrame([
        {"deviceid": "8", "locationid": "15", "name": "PRM gasmeter", "devicetypeid": "60", "meterdivider": 1000}
    ])
    location_df = pd.DataFrame([{"locationid": "15", "locationname": "15", "buildingtypeid": "2"}])
    buildingtype_df = pd.DataFrame([{"buildingtypeid": "2", "buildingname": "Camperplek"}])
    devicetype_df = pd.DataFrame(
        [
            {"devicetypeid": "60", "devid": "4518", "devicename": "PRMGAS", "icyname": "PRM gasmeter"},
        ]
    )

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)
    row = catalog.iloc[0]

    assert row["meterdivider"] == 1000
    assert row["raw_reading"] == 2.103
    assert row["current_offset"] == 0.897
    assert row["effective_reading"] == 3.0


def test_build_catalog_prefers_slave_meterdivider_over_device_meterdivider():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 10730, "timestamp": "2026-04-18", "deviceid": "25", "slavedeviceid": "14", "channel": None},
        ]
    )
    slave_df = pd.DataFrame([
        {"slavedeviceid": "14", "deviceid": "25", "locationid": "29", "name": "Socket 14", "slavedevicetypeid": "45", "meterdivider": 1000}
    ])
    offset_df = pd.DataFrame(columns=["deviceid", "slavedeviceid", "offset"])
    device_df = pd.DataFrame([
        {"deviceid": "25", "locationid": "29", "name": "Controller 25", "devicetypeid": "56", "meterdivider": 1}
    ])
    location_df = pd.DataFrame([{"locationid": "29", "locationname": "029", "buildingtypeid": "1"}])
    buildingtype_df = pd.DataFrame([{"buildingtypeid": "1", "buildingname": "Kampeerplek"}])
    devicetype_df = pd.DataFrame([
        {"devicetypeid": "45", "devid": "4518", "devicename": "CAMPEREWS", "icyname": "ICY4518 Campère wall socket"},
        {"devicetypeid": "56", "devid": "4942", "devicename": "CAMPCTRL", "icyname": "ICY4942 Campère controller"},
    ])

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)
    row = catalog.iloc[0]

    assert row["slavedeviceid"] == "14"
    assert row["meterdivider"] == 1000
    assert row["raw_reading"] == 10.73


def test_build_catalog_uses_device_meterdivider_when_slave_column_exists_but_is_empty():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 10730, "timestamp": "2026-04-18", "deviceid": "14", "slavedeviceid": None, "channel": None},
        ]
    )
    slave_df = pd.DataFrame([
        {"slavedeviceid": "99", "deviceid": "88", "locationid": "29", "name": "Other slave", "slavedevicetypeid": "45", "meterdivider": 1000}
    ])
    offset_df = pd.DataFrame(columns=["deviceid", "slavedeviceid", "offset"])
    device_df = pd.DataFrame([
        {"deviceid": "14", "locationid": "29", "name": "Socket 14", "devicetypeid": "45", "meterdivider": 1000}
    ])
    location_df = pd.DataFrame([{"locationid": "29", "locationname": "029", "buildingtypeid": "1"}])
    buildingtype_df = pd.DataFrame([{"buildingtypeid": "1", "buildingname": "Kampeerplek"}])
    devicetype_df = pd.DataFrame([
        {"devicetypeid": "45", "devid": "4518", "devicename": "CAMPEREWS", "icyname": "ICY4518 Campère wall socket"},
    ])

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)
    row = catalog.iloc[0]

    assert row["deviceid"] == "14"
    assert row["slavedeviceid"] == ""
    assert row["meterdivider"] == 1000
    assert row["raw_reading"] == 10.73


def test_prepare_batch_preview_uses_meterdivider_for_new_offset():
    catalog = pd.DataFrame(
        [
            {
                "deviceid": "8",
                "slavedeviceid": "",
                "channel": "",
                "location_label": "Camperplek - 015",
                "raw_reading": 2.103,
                "raw_value": 2103,
                "current_offset": 0.897,
                "effective_reading": 3.0,
                "meterdivider": 1000,
                "meter_type_label": "PRM gasmeter",
                "devicetype_name": "PRM gasmeter",
                "devicetype_code": "PRMGAS",
                "meter_variable": "prm_gas_meter",
            }
        ]
    )
    batch_df = pd.DataFrame(
        [
            {"deviceid": "8", "new_meter_reading": 4.5},
        ]
    )

    preview = pulse_tool.prepare_batch_preview(batch_df, catalog)

    assert preview.iloc[0]["match_status"] == "Klaar om op te slaan"
    assert preview.iloc[0]["new_offset"] == 2397


def test_prepare_batch_preview_supports_optional_meterdivider_change():
    catalog = pd.DataFrame(
        [
            {
                "deviceid": "8",
                "slavedeviceid": "",
                "channel": "",
                "location_label": "Camperplek - 015",
                "raw_reading": 2.103,
                "raw_value": 2103,
                "current_offset": 0.897,
                "offset_value_raw": 897,
                "effective_reading": 3.0,
                "meterdivider": 1000,
                "meter_type_label": "PRM gasmeter",
                "devicetype_name": "PRM gasmeter",
                "devicetype_code": "PRMGAS",
                "meter_variable": "prm_gas_meter",
            }
        ]
    )
    batch_df = pd.DataFrame(
        [
            {"deviceid": "8", "new_meterdivider": 100},
        ]
    )

    preview = pulse_tool.prepare_batch_preview(batch_df, catalog)
    row = preview.iloc[0]

    assert row["match_status"] == "Klaar om op te slaan"
    assert row["current_meterdivider"] == 1000
    assert row["new_meterdivider"] == 100
    assert row["new_offset"] == 897
    assert row["resulting_effective_reading"] == 30.0


def test_get_batch_preview_display_df_hides_internal_columns():
    preview = pd.DataFrame(
        [
            {
                "deviceid": "8",
                "slavedeviceid": "",
                "channel": "",
                "new_meter_reading": 4.5,
                "new_meterdivider": 1000,
                "match_count": 1,
                "match_status": "Klaar om op te slaan",
                "status_detail": "Interne detailuitleg",
                "location_label": "Camperplek - 015",
                "raw_reading": 2.103,
                "raw_value": 2103,
                "current_meterdivider": 1000,
                "meterdivider": 1000,
                "current_offset": 0.897,
                "effective_reading": 3.0,
                "resulting_effective_reading": 4.5,
                "new_offset": 2397,
                "meter_type_label": "PRM gasmeter",
                "devicetype_name": "PRM gasmeter",
                "devicetype_code": "PRMGAS",
                "meter_variable": "prm_gas_meter",
                "offset_value_raw": 897,
            }
        ]
    )

    display = pulse_tool.get_batch_preview_display_df(preview)

    assert "raw_value" not in display.columns
    assert "current_meterdivider" not in display.columns
    assert "devicetype_code" not in display.columns
    assert "meter_variable" not in display.columns
    assert "offset_value_raw" not in display.columns
    assert "match_status" in display.columns
    assert "location_label" in display.columns


def test_build_batch_staging_row_keeps_only_batch_fields_and_values():
    row = {
        "deviceid": "8",
        "slavedeviceid": "",
        "channel": "",
        "meterdivider": 1000,
        "meter_type_label": "PRM gasmeter",
    }

    staged = pulse_tool.build_batch_staging_row(row, desired_meter_reading=10, new_meterdivider=1000)

    assert staged["deviceid"] == "8"
    assert staged["new_meter_reading"] == 10
    assert staged["new_meterdivider"] == 1000
    assert "meter_type_label" not in staged


def test_upsert_batch_staging_rows_replaces_duplicate_record():
    existing_rows = [
        {"deviceid": "8", "slavedeviceid": "", "channel": "", "new_meter_reading": 5, "new_meterdivider": 1000}
    ]
    new_row = {"deviceid": "8", "slavedeviceid": "", "channel": "", "new_meter_reading": 10, "new_meterdivider": 1000}

    staged_rows, action = pulse_tool.upsert_batch_staging_rows(existing_rows, new_row)

    assert action == "updated"
    assert len(staged_rows) == 1
    assert staged_rows[0]["new_meter_reading"] == 10


def test_build_batch_staging_row_rejects_mid_locked_meter():
    row = {
        "deviceid": "2",
        "slavedeviceid": "10002",
        "meter_type_label": "Campère meter",
        "devicetype_name": "ICY4850 Campère controller - Campère meter",
        "devicetype_code": "CAMPSLAVE",
        "meter_variable": "campere_meter",
    }

    try:
        pulse_tool.build_batch_staging_row(row, desired_meter_reading=10, new_meterdivider=1000)
        assert False, "Expected MID-locked meter to be blocked from batch staging"
    except ValueError as exc:
        assert "MID" in str(exc)


def test_build_catalog_handles_missing_offset_columns_without_crashing():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 2103, "timestamp": "2026-04-16", "deviceid": "8", "slavedeviceid": None, "channel": None},
        ]
    )
    slave_df = pd.DataFrame()
    offset_df = pd.DataFrame()
    device_df = pd.DataFrame([{"deviceid": "8", "locationid": "15", "name": "Camper module", "devicetypeid": "60"}])
    location_df = pd.DataFrame([{"locationid": "15", "locationname": "15", "buildingtypeid": "2"}])
    buildingtype_df = pd.DataFrame([{"buildingtypeid": "2", "buildingname": "Camperplek"}])
    devicetype_df = pd.DataFrame(
        [
            {"devicetypeid": "60", "devid": "4518", "devicename": "CAMPMODULE", "icyname": "ICY4518 Campère module"},
        ]
    )

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)

    assert len(catalog) == 1
    assert catalog.iloc[0]["current_offset"] == 0
    assert catalog.iloc[0]["effective_reading"] == 2103


def test_build_catalog_enriches_known_devicetype_variables():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 120, "timestamp": "2026-04-16", "deviceid": None, "slavedeviceid": "50", "channel": None},
            {"pulsecounterlogid": 2, "value": 80, "timestamp": "2026-04-16", "deviceid": None, "slavedeviceid": "51", "channel": None},
        ]
    )
    slave_df = pd.DataFrame(
        [
            {"slavedeviceid": "50", "deviceid": "25", "locationid": "31", "name": "PLE meter", "slavedevicetypeid": "101"},
            {"slavedeviceid": "51", "deviceid": "25", "locationid": "31", "name": "PLEB meter", "slavedevicetypeid": "109"},
        ]
    )
    offset_df = pd.DataFrame(columns=["deviceid", "slavedeviceid", "offset"])
    device_df = pd.DataFrame([{"deviceid": "25", "locationid": "31", "name": "Main device 25", "devicetypeid": "56"}])
    location_df = pd.DataFrame([{"locationid": "31", "locationname": "004", "buildingtypeid": "1"}])
    buildingtype_df = pd.DataFrame([{"buildingtypeid": "1", "buildingname": "Kampeerplek"}])
    devicetype_df = pd.DataFrame(
        [
            {"devicetypeid": "56", "devid": "4942", "devicename": "CAMPCTRL", "icyname": "ICY4942 Campère controller"},
            {"devicetypeid": "101", "devid": "5633", "devicename": "PLE", "icyname": "ICY3525PL4 Pulse Logging Module (4-channel)"},
            {"devicetypeid": "109", "devid": "5633", "devicename": "PLEB", "icyname": "ICY3525PL4 Pulse Logging Module (4-channel)"},
        ]
    )

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)

    ple_row = catalog[catalog["slavedeviceid"] == "50"].iloc[0]
    pleb_row = catalog[catalog["slavedeviceid"] == "51"].iloc[0]

    assert ple_row["devicetypeid"] == "101"
    assert ple_row["device_name"] == "Main device 25"
    assert ple_row["meter_type_key"] == "electricity_import"
    assert ple_row["meter_type_label"] == "kWh meter"
    assert ple_row["devicetype_name"] == "ICY3525PL4 Pulse Logging Module (4-channel) - kWh meter"
    assert pleb_row["devicetypeid"] == "109"
    assert pleb_row["meter_type_key"] == "electricity_export"
    assert pleb_row["meter_type_label"] == "Teruglevering kWh"
    assert pleb_row["devicetype_name"] == "ICY3525PL4 Pulse Logging Module (4-channel) - Teruglevering kWh"


def test_build_catalog_prefers_icyname_for_display_over_old_metertype_text():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 322, "timestamp": "2026-04-16", "deviceid": None, "slavedeviceid": "44", "channel": None},
        ]
    )
    slave_df = pd.DataFrame(
        [
            {"slavedeviceid": "44", "deviceid": "2", "locationid": "17", "name": None, "slavedevicetypeid": "701", "metertype": "Oude benaming"},
        ]
    )
    offset_df = pd.DataFrame(columns=["deviceid", "slavedeviceid", "offset"])
    device_df = pd.DataFrame([{"deviceid": "2", "locationid": "17", "name": "Main device 2", "devicetypeid": "56"}])
    location_df = pd.DataFrame([{"locationid": "17", "locationname": "017", "buildingtypeid": "2"}])
    buildingtype_df = pd.DataFrame([{"buildingtypeid": "2", "buildingname": "Camperplek"}])
    devicetype_df = pd.DataFrame(
        [
            {"devicetypeid": "56", "devid": "4942", "devicename": "CAMPCTRL", "icyname": "ICY4942 Campère controller"},
            {"devicetypeid": "701", "devid": "4942", "devicename": "CAMPSLAVE", "icyname": "ICY4942 Campère controller"},
        ]
    )

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)
    row = catalog.iloc[0]

    assert row["meter_type_label"] == "Campère meter"
    assert row["devicetype_name"] == "ICY4942 Campère controller - Campère meter"


def test_build_catalog_uses_device_devicetype_for_direct_devices_without_slave():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 2103, "timestamp": "2026-04-16", "deviceid": "8", "slavedeviceid": None, "channel": None},
        ]
    )
    slave_df = pd.DataFrame(columns=["slavedeviceid", "deviceid", "locationid", "name", "slavedevicetypeid"])
    offset_df = pd.DataFrame(columns=["deviceid", "slavedeviceid", "offset"])
    device_df = pd.DataFrame([{"deviceid": "8", "locationid": "15", "name": "Camper module", "devicetypeid": "60"}])
    location_df = pd.DataFrame([{"locationid": "15", "locationname": "15", "buildingtypeid": "2"}])
    buildingtype_df = pd.DataFrame([{"buildingtypeid": "2", "buildingname": "Camperplek"}])
    devicetype_df = pd.DataFrame(
        [
            {"devicetypeid": "60", "devid": "4518", "devicename": "CAMPMODULE", "icyname": "ICY4518 Campère module"},
        ]
    )

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)
    row = catalog.iloc[0]

    assert row["slavedeviceid"] == ""
    assert row["devicetypeid"] == "60"
    assert row["device_name"] == "Camper module"
    assert row["meter_type_label"] == "ICY4518 Campère module"
    assert row["devicetype_name"] == "ICY4518 Campère module"
    assert str(row["meter_type_label"]).lower() != "nan"


def test_build_catalog_never_shows_nan_for_unmapped_direct_devicetype():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 1048, "timestamp": "2026-04-16", "deviceid": "9", "slavedeviceid": None, "channel": None},
        ]
    )
    slave_df = pd.DataFrame(columns=["slavedeviceid", "deviceid", "locationid", "name", "slavedevicetypeid"])
    offset_df = pd.DataFrame(columns=["deviceid", "slavedeviceid", "offset"])
    device_df = pd.DataFrame([{"deviceid": "9", "locationid": "16", "name": None, "devicetypeid": "45"}])
    location_df = pd.DataFrame([{"locationid": "16", "locationname": "016", "buildingtypeid": "2"}])
    buildingtype_df = pd.DataFrame([{"buildingtypeid": "2", "buildingname": "Camperplek"}])
    devicetype_df = pd.DataFrame(
        [
            {"devicetypeid": "45", "devid": "4518", "devicename": "CAMPEREWS", "icyname": pd.NA},
        ]
    )

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)
    row = catalog.iloc[0]

    assert str(row["meter_type_label"]).lower() != "nan"
    assert row["meter_type_label"] == "ICY4518 Campère wall socket"


def test_build_catalog_does_not_show_zero_as_device_name():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 1048, "timestamp": "2026-04-16", "deviceid": "348", "slavedeviceid": "704", "channel": None},
        ]
    )
    slave_df = pd.DataFrame([
        {"slavedeviceid": "704", "deviceid": "348", "locationid": "14", "name": None, "slavedevicetypeid": "60"}
    ])
    offset_df = pd.DataFrame(columns=["deviceid", "slavedeviceid", "offset"])
    device_df = pd.DataFrame([{"deviceid": "348", "locationid": "14", "name": 0, "devicetypeid": "60"}])
    location_df = pd.DataFrame([{"locationid": "14", "locationname": "14", "buildingtypeid": "7"}])
    buildingtype_df = pd.DataFrame([{"buildingtypeid": "7", "buildingname": "Villa"}])
    devicetype_df = pd.DataFrame(
        [
            {"devicetypeid": "60", "devid": "5247", "devicename": "PRMKWH", "icyname": "ICY5247 prm kWh"},
        ]
    )

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)
    row = catalog.iloc[0]

    assert row["device_name"] != "0"
    assert row["device_name"] == "ICY5247 prm kWh"


def test_build_catalog_hides_orphan_historical_logs_without_active_links():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 100, "timestamp": "2026-04-16", "deviceid": None, "slavedeviceid": "11174", "channel": None},
            {"pulsecounterlogid": 2, "value": 200, "timestamp": "2026-04-16", "deviceid": "9", "slavedeviceid": None, "channel": None},
        ]
    )
    slave_df = pd.DataFrame([
        {"slavedeviceid": "50", "deviceid": "25", "locationid": "31", "slavedevicetypeid": "701"}
    ])
    offset_df = pd.DataFrame(columns=["deviceid", "slavedeviceid", "offset"])
    device_df = pd.DataFrame([
        {"deviceid": "9", "locationid": "16", "name": "Direct device 9", "devicetypeid": "45"}
    ])
    location_df = pd.DataFrame([
        {"locationid": "16", "locationname": "016", "buildingtypeid": "2"}
    ])
    buildingtype_df = pd.DataFrame([
        {"buildingtypeid": "2", "buildingname": "Camperplek"}
    ])
    devicetype_df = pd.DataFrame([
        {"devicetypeid": "45", "devid": "4518", "devicename": "CAMPEREWS", "icyname": "ICY4518 Campère wall socket"}
    ])

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)

    assert len(catalog) == 1
    assert catalog.iloc[0]["deviceid"] == "9"
    assert catalog.iloc[0]["slavedeviceid"] == ""


def test_build_catalog_search_text_contains_normalized_location_name():
    log_df = pd.DataFrame(
        [
            {"pulsecounterlogid": 1, "value": 100, "timestamp": "2026-04-16", "deviceid": "9", "slavedeviceid": None, "channel": None},
        ]
    )
    slave_df = pd.DataFrame(columns=["slavedeviceid", "deviceid", "locationid", "name", "slavedevicetypeid"])
    offset_df = pd.DataFrame(columns=["deviceid", "slavedeviceid", "offset"])
    device_df = pd.DataFrame([
        {"deviceid": "9", "locationid": "16", "name": "Villa meter", "devicetypeid": "45"}
    ])
    location_df = pd.DataFrame([
        {"locationid": "16", "locationname": "14", "buildingtypeid": "2"}
    ])
    buildingtype_df = pd.DataFrame([
        {"buildingtypeid": "2", "buildingname": "Villa"}
    ])
    devicetype_df = pd.DataFrame([
        {"devicetypeid": "45", "devid": "4518", "devicename": "CAMPEREWS", "icyname": "ICY4518 Campère wall socket"}
    ])

    catalog = pulse_tool.build_catalog(log_df, slave_df, offset_df, device_df, location_df, buildingtype_df, devicetype_df)

    assert "villa 14" in str(catalog.iloc[0]["search_text"])


def test_build_persisted_state_keeps_only_safe_url_values():
    state = {
        "db_host_override": "auto",
        "db_host_manual": "icyccdb.icy.nl",
        "db_name": "nl_icydemopark",
        "db_user": "report_user",
        "db_password": "secret",
        "user_initials": "HN",
        "location_filter": "016",
        "device_filter": "9",
        "slave_filter": "50",
        "selected_location": "Camperplek - 016",
        "search_text": "wall socket",
        "mid_filter": "Alleen NON MID",
        "db_ready": True,
        "current_record_index": 3,
    }

    persisted = pulse_tool.build_persisted_state(state)

    assert persisted["selected_location"] == "Camperplek - 016"
    assert persisted["mid_filter"] == "Alleen NON MID"
    assert persisted["current_record_index"] == "3"
    assert "db_host_manual" not in persisted
    assert "db_name" not in persisted
    assert "db_user" not in persisted
    assert "user_initials" not in persisted
    assert "db_password" not in persisted
    assert "search_text" not in persisted
    assert "db_ready" not in persisted


def test_sync_persisted_state_removes_sensitive_query_params(monkeypatch):
    fake_st = fake_streamlit(
        session_state={
            "selected_location": "Alle locaties",
            "mid_filter": "Alle meters",
            "current_record_index": 0,
        },
        query_params={
            "db_host_manual": "icyccdb.icy.nl",
            "db_name": "nl_icydemopark",
            "db_user": "HVNijdam",
            "user_initials": "HVN",
            "selected_location": "Alle locaties",
        },
    )
    monkeypatch.setattr(pulse_tool, "st", fake_st)

    pulse_tool.sync_persisted_state()

    assert "db_host_manual" not in fake_st.query_params
    assert "db_name" not in fake_st.query_params
    assert "db_user" not in fake_st.query_params
    assert "user_initials" not in fake_st.query_params


def test_search_text_can_be_cleared_when_switching_database():
    state = {
        "db_name": "old_db",
        "search_text": "water",
        "selected_location": "Villa - 14",
    }

    previous_db_name = str(state.get("db_name", "")).strip()
    new_db_name = "new_db"

    if previous_db_name and previous_db_name != new_db_name:
        state["search_text"] = ""
        state["selected_location"] = "Alle locaties"

    assert state["search_text"] == ""
    assert state["selected_location"] == "Alle locaties"


def test_prepare_batch_preview_separates_direct_devices_from_slave_devices():
    catalog = pd.DataFrame(
        [
            {"deviceid": "25", "slavedeviceid": "50", "channel": "", "location_label": "Kampeerplek - 004", "raw_reading": 459, "current_offset": 0, "effective_reading": 459},
            {"deviceid": "9", "slavedeviceid": "", "channel": "", "location_label": "Camperplek - 016", "raw_reading": 501, "current_offset": 0, "effective_reading": 501},
        ]
    )
    batch_df = pd.DataFrame(
        [
            {"deviceid": "25", "new_meter_reading": 1500},
            {"slavedeviceid": "50", "deviceid": "25", "new_meter_reading": 1500},
            {"deviceid": "9", "new_meter_reading": 900},
        ]
    )

    preview = pulse_tool.prepare_batch_preview(batch_df, catalog)

    assert preview.iloc[0]["match_status"] == "DeviceID heeft Slavedevices - gebruik SlavedeviceID"
    assert preview.iloc[1]["match_status"] == "Klaar om op te slaan"
    assert preview.iloc[2]["match_status"] == "Klaar om op te slaan"


def test_save_offset_updates_existing_slave_record_and_sets_comment(monkeypatch):
    monkeypatch.setattr(pulse_tool, "st", fake_streamlit("HN"))
    monkeypatch.setattr(pulse_tool, "datetime", FixedDatetime)

    cursor = FakeCursor(fetchone_results=[(15,)])
    connection = FakeConnection(cursor)
    monkeypatch.setattr(pulse_tool, "conn", lambda database_name=None: connection)

    df = pd.DataFrame(
        [
            {
                "deviceid": "2",
                "slavedeviceid": "10002",
                "channel": "",
                "new_offset": 1234,
            }
        ]
    )

    pulse_tool.save_offset(df)

    assert connection.committed is True
    assert connection.closed is True
    assert "WHERE slavedeviceid = %s" in cursor.queries[0][0]
    assert "UPDATE" in cursor.queries[1][0]
    update_params = cursor.queries[1][1]
    assert update_params[0] is None
    assert update_params[1] == "10002"
    assert update_params[4] == "16-04-2026 HN"
    assert update_params[5] == 15


def test_save_offset_can_update_meterdivider_without_creating_zero_offset(monkeypatch):
    monkeypatch.setattr(pulse_tool, "st", fake_streamlit("HN"))
    monkeypatch.setattr(pulse_tool, "datetime", FixedDatetime)

    cursor = FakeCursor(fetchone_results=[None])
    connection = FakeConnection(cursor)
    monkeypatch.setattr(pulse_tool, "conn", lambda database_name=None: connection)

    df = pd.DataFrame(
        [
            {
                "deviceid": "9",
                "slavedeviceid": "",
                "channel": "",
                "current_meterdivider": 1000,
                "new_meterdivider": 100,
                "current_offset": 0,
                "offset_value_raw": 0,
                "new_offset": 0,
            }
        ]
    )

    pulse_tool.save_offset(df)

    assert connection.committed is True
    assert connection.closed is True
    assert len(cursor.queries) == 2
    assert "WHERE deviceid = %s" in cursor.queries[0][0]
    assert f"UPDATE {pulse_tool.DEVICE_TABLE} SET meterdivider = %s" in cursor.queries[1][0]
    assert cursor.queries[1][1] == (100.0, "9")


def test_is_offset_edit_blocked_for_campere_meter():
    row = pd.Series(
        {
            "meter_type_label": "Campère meter",
            "devicetype_name": "ICY4942 Campère controller - Campère meter",
            "devicetype_code": "CAMPSLAVE",
            "meter_variable": "campere_meter",
        }
    )

    assert pulse_tool.is_offset_edit_blocked(row) is True


def test_non_mid_filter_logic_can_exclude_mid_devices():
    df = pd.DataFrame([
        {"deviceid": "1", "offset_edit_blocked": True},
        {"deviceid": "2", "offset_edit_blocked": False},
    ])

    filtered = df[~df["offset_edit_blocked"].fillna(False)]

    assert filtered["deviceid"].tolist() == ["2"]


def test_prepare_batch_preview_marks_campere_rows_as_blocked():
    catalog = pd.DataFrame(
        [
            {
                "deviceid": "25",
                "slavedeviceid": "50",
                "channel": "",
                "location_label": "Kampeerplek - 004",
                "raw_reading": 459,
                "current_offset": 0,
                "effective_reading": 459,
                "meter_type_label": "Campère meter",
                "devicetype_name": "ICY4850 Campère controller - Campère meter",
                "devicetype_code": "CAMPSLAVE",
                "meter_variable": "campere_meter",
            }
        ]
    )
    batch_df = pd.DataFrame(
        [
            {"slavedeviceid": "50", "deviceid": "25", "new_meter_reading": 1500},
        ]
    )

    preview = pulse_tool.prepare_batch_preview(batch_df, catalog)

    assert preview.iloc[0]["match_status"] == "Geblokkeerd - MID Campère meter"
    assert "MID" in preview.iloc[0]["status_detail"]


def test_save_offset_rejects_mid_certified_campere_meter(monkeypatch):
    monkeypatch.setattr(pulse_tool, "st", fake_streamlit("HN"))
    monkeypatch.setattr(pulse_tool, "datetime", FixedDatetime)

    cursor = FakeCursor(fetchone_results=[(15,)])
    connection = FakeConnection(cursor)
    monkeypatch.setattr(pulse_tool, "conn", lambda database_name=None: connection)

    df = pd.DataFrame(
        [
            {
                "deviceid": "2",
                "slavedeviceid": "10002",
                "channel": "",
                "new_offset": 1234,
                "meter_type_label": "Campère meter",
                "devicetype_name": "ICY4850 Campère controller - Campère meter",
                "devicetype_code": "CAMPSLAVE",
                "meter_variable": "campere_meter",
            }
        ]
    )

    try:
        pulse_tool.save_offset(df)
        assert False, "Expected Campère meter save to be blocked"
    except ValueError as exc:
        assert "MID" in str(exc)

    assert connection.committed is False
    assert cursor.queries == []


def test_is_offset_edit_not_blocked_for_icy4518_module():
    row = pd.Series(
        {
            "meter_type_label": "ICY4518 Campère module",
            "devicetype_name": "ICY4518 Campère module",
            "devicetype_code": "CAMPEREMOD",
            "meter_variable": "campere_module",
        }
    )

    assert pulse_tool.is_offset_edit_blocked(row) is False


def test_is_offset_edit_not_blocked_for_icy5247_pr_meter():
    row = pd.Series(
        {
            "meter_type_label": "PRM campère meter",
            "devicetype_name": "ICY5247 PR meter",
            "devicetype_code": "PRMCAMPERE",
            "meter_variable": "prm_campere_meter",
        }
    )

    assert pulse_tool.is_offset_edit_blocked(row) is False


def test_delete_offset_removes_existing_slave_record(monkeypatch):
    monkeypatch.setattr(pulse_tool, "st", fake_streamlit("HN"))
    monkeypatch.setattr(pulse_tool, "datetime", FixedDatetime)

    cursor = FakeCursor(fetchone_results=[(15,)])
    connection = FakeConnection(cursor)
    monkeypatch.setattr(pulse_tool, "conn", lambda database_name=None: connection)

    df = pd.DataFrame(
        [
            {
                "deviceid": "2",
                "slavedeviceid": "10002",
                "channel": "",
            }
        ]
    )

    deleted = pulse_tool.delete_offset(df)

    assert deleted == 1
    assert connection.committed is True
    assert connection.closed is True
    assert "WHERE slavedeviceid = %s" in cursor.queries[0][0]
    assert "DELETE FROM" in cursor.queries[1][0]


def test_delete_offset_rejects_mid_certified_campere_meter(monkeypatch):
    monkeypatch.setattr(pulse_tool, "st", fake_streamlit("HN"))
    monkeypatch.setattr(pulse_tool, "datetime", FixedDatetime)

    cursor = FakeCursor(fetchone_results=[(15,)])
    connection = FakeConnection(cursor)
    monkeypatch.setattr(pulse_tool, "conn", lambda database_name=None: connection)

    df = pd.DataFrame(
        [
            {
                "deviceid": "2",
                "slavedeviceid": "10002",
                "channel": "",
                "meter_type_label": "Campère meter",
                "devicetype_name": "ICY4850 Campère controller - Campère meter",
                "devicetype_code": "CAMPSLAVE",
                "meter_variable": "campere_meter",
            }
        ]
    )

    try:
        pulse_tool.delete_offset(df)
        assert False, "Expected Campère meter delete to be blocked"
    except ValueError as exc:
        assert "MID" in str(exc)

    assert connection.committed is False
