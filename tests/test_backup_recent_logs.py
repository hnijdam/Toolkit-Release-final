from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace


MODULE_PATH = Path(__file__).resolve().parents[1] / "python" / "DBscript" / "backup_recent_logs.py"
spec = spec_from_file_location("backup_recent_logs", MODULE_PATH)
backup_recent_logs = module_from_spec(spec)
spec.loader.exec_module(backup_recent_logs)


class FakeParser:
    def __init__(self, args):
        self._args = args

    def parse_args(self):
        return self._args


def make_args():
    return SimpleNamespace(
        days=14,
        database=None,
        deviceid=None,
        slavedeviceid=None,
        include_system=False,
        output_dir=None,
        dry_run=False,
    )


def test_main_cancels_all_database_backup_when_user_quits(monkeypatch, capsys):
    monkeypatch.setattr(backup_recent_logs, "build_parser", lambda: FakeParser(make_args()))
    monkeypatch.setattr(backup_recent_logs.sys, "stdin", SimpleNamespace(isatty=lambda: True))

    answers = iter(["", "q"])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(answers))

    run_calls = []
    monkeypatch.setattr(backup_recent_logs, "run_backup", lambda **kwargs: run_calls.append(kwargs) or 1)

    result = backup_recent_logs.main()
    output = capsys.readouterr().out.lower()

    assert result == 0
    assert run_calls == []
    assert "geannuleerd" in output


def test_main_runs_all_database_backup_only_after_yes(monkeypatch):
    monkeypatch.setattr(backup_recent_logs, "build_parser", lambda: FakeParser(make_args()))
    monkeypatch.setattr(backup_recent_logs.sys, "stdin", SimpleNamespace(isatty=lambda: True))

    answers = iter(["", "yes", "", ""])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(answers))

    run_calls = []
    monkeypatch.setattr(backup_recent_logs, "run_backup", lambda **kwargs: run_calls.append(kwargs) or 7)

    result = backup_recent_logs.main()

    assert result == 7
    assert len(run_calls) == 1
    assert run_calls[0]["requested_databases"] is None
