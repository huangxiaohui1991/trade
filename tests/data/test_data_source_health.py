import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


class DataSourceHealthTests(unittest.TestCase):
    def test_health_snapshot_is_ok_when_recent_runs_and_caches_are_usable(self):
        import scripts.cli.trade as trade

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_dir = root / "cache"
            runs_dir = root / "runs"
            now = datetime(2026, 4, 10, 10, 0, 0)
            cached_at = "2026-04-10T09:58:00"
            for namespace, data in {
                "financial": {
                    "code": "300389",
                    "roe": 15.0,
                    "revenue_growth": 20.0,
                    "operating_cash_flow": 1.0,
                    "cash_flow_positive": True,
                },
                "flow": {"code": "300389", "main_net_inflow": 1.0},
                "market_timer": {"symbol": "sh000001", "signal": "GREEN"},
                "screening_candidates": [{"code": "300389"}],
                "trading_calendar": ["2026-04-10"],
            }.items():
                _write_json(
                    cache_dir / namespace / "sample.json",
                    {"cached_at": cached_at, "data": data, "meta": {"source": "unit_test"}},
                )
            _write_json(
                runs_dir / "2026-04-10" / "scoring_scoring_20260410_100000_1.json",
                {
                    "pipeline": "scoring",
                    "run_id": "scoring_1",
                    "status": "warning",
                    "started_at": "2026-04-10T10:00:00",
                    "finished_at": "2026-04-10T10:01:00",
                    "result": {"status": "ok"},
                },
            )

            snapshot = trade._data_source_health_snapshot(
                cache_dir=cache_dir,
                runs_dir=runs_dir,
                now=now,
                pool_snapshot={"entries": [{"code": "300389", "data_quality": "ok"}]},
            )

        self.assertEqual(snapshot["status"], "ok")
        self.assertEqual(snapshot["warning"], [])
        self.assertEqual(snapshot["recent_runs"]["success_count"], 1)
        self.assertEqual(snapshot["cache_summary"]["file_count"], 5)
        self.assertEqual(snapshot["score_data_quality"]["quality_counts"]["ok"], 1)

    def test_health_snapshot_warns_on_run_failure_stale_cache_and_degraded_scores(self):
        import scripts.cli.trade as trade

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_dir = root / "cache"
            runs_dir = root / "runs"
            now = datetime(2026, 4, 10, 10, 0, 0)
            _write_json(
                cache_dir / "financial" / "300389.json",
                {
                    "cached_at": "2026-03-01T09:00:00",
                    "data": {"code": "300389", "roe": None, "revenue_growth": None},
                    "meta": {"source": "unit_test"},
                },
            )
            _write_json(
                runs_dir / "2026-04-10" / "scoring_scoring_20260410_100000_1.json",
                {
                    "pipeline": "scoring",
                    "run_id": "scoring_1",
                    "status": "error",
                    "started_at": "2026-04-10T10:00:00",
                    "finished_at": "2026-04-10T10:01:00",
                    "result": {"status": "error"},
                },
            )

            snapshot = trade._data_source_health_snapshot(
                cache_dir=cache_dir,
                runs_dir=runs_dir,
                now=now,
                pool_snapshot={
                    "entries": [
                        {"code": "300389", "data_quality": "degraded", "data_missing_fields": ["营收"]}
                    ]
                },
            )

        self.assertEqual(snapshot["status"], "warning")
        self.assertIn("recent_pipeline_runs", snapshot["warning"])
        self.assertIn("cache_freshness", snapshot["warning"])
        self.assertIn("score_data_quality", snapshot["warning"])
        self.assertEqual(snapshot["recent_runs"]["failure_count"], 1)
        self.assertEqual(snapshot["cache_namespaces"]["financial"]["missing_field_count"], 1)
        self.assertEqual(snapshot["score_data_quality"]["quality_counts"]["degraded"], 1)


if __name__ == "__main__":
    unittest.main()
