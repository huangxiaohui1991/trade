"""Smoke tests for the real CLI entrypoint."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


def _cli_env(tmp_path: Path) -> dict:
    env = os.environ.copy()
    env["ASTOCK_DATABASE_URL"] = f"sqlite:///{tmp_path / 'runtime.db'}"
    return env


def test_doctor_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "doctor", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["db"]["schema_version"] == 2
    assert payload["config"]["version"].startswith("v")
    assert "installed" in payload["mcp"]
    assert payload["timezone"] == "Asia/Shanghai"


def test_doctor_json_fails_without_database_url():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env = os.environ.copy()
    env.pop("ASTOCK_DATABASE_URL", None)

    result = subprocess.run(
        [str(cli), "doctor", "--json"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "failed"
    assert "ASTOCK_DATABASE_URL is required" in payload["error"]


def test_continuation_validate_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "continuation-validate", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Top N" in result.stdout
    assert "--start" in result.stdout
    assert "--end" in result.stdout


def test_continuation_backtest_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "continuation-backtest", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--hold-days" in result.stdout
    assert "--top-n" in result.stdout


def test_continuation_study_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "continuation-study", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--top-ns" in result.stdout
    assert "--hold-days" in result.stdout


def test_health_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "health", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] in {"ok", "warning", "failed"}
    assert "db" in payload
    assert "runs" in payload
    assert "data_sources" in payload
    assert "status" in payload["data_sources"]
    assert "checks" in payload["data_sources"]


def test_data_sources_status_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "data-sources", "status", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--max-age-hours" in result.stdout
    assert "--json" in result.stdout


def test_mcp_help_uses_stable_entrypoint_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "mcp", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "bin/trade mcp" in result.stdout
    assert "python -m astock_trading" not in result.stdout


def test_run_pipeline_help_includes_data_source_health_override():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "run-pipeline", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--ignore-data-source-health" in result.stdout


def test_db_maintenance_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    for args, expected in [
        (["db", "backup", "--help"], "--output"),
        (["db", "tables", "--help"], "MySQL"),
        (["db", "check", "--help"], "CHECK TABLE"),
        (["db", "optimize", "--help"], "OPTIMIZE TABLE"),
    ]:
        result = subprocess.run(
            [str(cli), *args],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        assert expected in result.stdout


def test_db_status_initializes_schema_version_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "db", "status", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 2


def test_removed_sqlite_maintenance_commands_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    for command in ["vacuum", "integrity", "audit-projections", "rebuild-projections"]:
        result = subprocess.run(
            [str(cli), "db", command, "--help"],
            cwd=root,
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0


def test_runs_cleanup_stale_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "runs", "cleanup-stale", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--older-than-hours" in result.stdout
    assert "--yes" in result.stdout


def test_agent_context_json_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "agent-context", "--json"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert "bin/trade" in payload["safe_entrypoints"]
    assert "src/astock_trading/**/*.py" in payload["forbidden_entrypoints"]


def test_machine_readable_runtime_commands_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env = _cli_env(tmp_path)

    for args in [
        ["events", "query", "--json"],
        ["runs", "list", "--json"],
        ["manual-trades", "list", "--json"],
    ]:
        result = subprocess.run(
            [str(cli), *args],
            cwd=root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        assert json.loads(result.stdout) == []


def test_sqlite_to_mysql_migration_dry_run_json_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "db", "migrate-sqlite-to-mysql", "--dry-run", "--json"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert "event_log" in payload["source_counts"]
    assert payload["target"] == "not_written"
