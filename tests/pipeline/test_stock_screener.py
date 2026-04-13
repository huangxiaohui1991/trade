"""Tests for scripts.pipeline.stock_screener."""

import unittest
from unittest import mock
from datetime import datetime

from scripts.pipeline import stock_screener


class ResolveTrackedCandidatesTests(unittest.TestCase):
    """Tests for _resolve_tracked_candidates and _tracked_candidates helpers."""

    def setUp(self):
        self.stocks_cfg = {
            "core_pool": [{"code": "000001", "name": "平安银行"}, {"code": "000002", "name": "万科A"}],
            "watch_pool": [{"code": "600000", "name": "浦发银行"}, {"code": "600036", "name": "招商银行"}],
            "blacklist": {"permanent": ["999999"], "temporary": []},
        }
        self.blacklist = {"999999"}
        self.empty_snapshot = None

    def test_resolve_tracked_candidates_returns_core_pool_when_pool_is_core(self):
        """core 池模式时，只返回核心池股票并排除黑名单。"""
        core_candidates = [
            {"code": "000001", "name": "平安银行"},
            {"code": "000002", "name": "万科A"},
        ]
        # _tracked_candidates is a module-level function; patch at the module namespace.
        # default_query="" skips MX → falls back to _fallback_tracked_candidates.
        with mock.patch(
            "scripts.pipeline.stock_screener._tracked_candidates",
            return_value=(core_candidates, "核心"),
        ):
            candidates, pool_name, source = stock_screener._resolve_tracked_candidates(
                pool="core",
                stocks_cfg=self.stocks_cfg,
                blacklist=self.blacklist,
                default_query="",
                select_type="A股",
                current_snapshot=None,
            )
        self.assertEqual(pool_name, "核心")
        # default_query="" skips MX, so fallback returns all non-blacklist candidates.
        codes = {c["code"] for c in candidates}
        self.assertIn("000001", codes)
        self.assertIn("000002", codes)
        self.assertNotIn("999999", codes)  # blacklist excluded

    def test_resolve_tracked_candidates_returns_watch_pool_when_pool_is_watch(self):
        """watch 池模式时，只返回观察池股票。"""
        watch_candidates = [
            {"code": "600000", "name": "浦发银行"},
            {"code": "600036", "name": "招商银行"},
        ]
        # default_query="" skips MX → falls back to _fallback_tracked_candidates.
        # Mock both the primary and fallback to isolate the pool selection logic.
        # _tracked_candidates returns (list, str); _fallback_tracked_candidates returns list.
        with (
            mock.patch(
                "scripts.pipeline.stock_screener._tracked_candidates",
                return_value=(watch_candidates, "观察"),
            ),
            mock.patch(
                "scripts.pipeline.stock_screener._fallback_tracked_candidates",
                return_value=watch_candidates,
            ),
        ):
            candidates, pool_name, source = stock_screener._resolve_tracked_candidates(
                pool="watch",
                stocks_cfg=self.stocks_cfg,
                blacklist=self.blacklist,
                default_query="",
                select_type="A股",
                current_snapshot=None,
            )
        self.assertEqual(pool_name, "观察")
        codes = {c["code"] for c in candidates}
        self.assertIn("600000", codes)
        self.assertIn("600036", codes)
        self.assertNotIn("000001", codes)  # core pool excluded

    def test_resolve_tracked_candidates_returns_all_when_pool_is_all(self):
        """pool='all' 时合并核心池和观察池。"""
        candidates, pool_name, source = stock_screener._resolve_tracked_candidates(
            pool="all",
            stocks_cfg=self.stocks_cfg,
            blacklist=self.blacklist,
            default_query="",
            select_type="A股",
            current_snapshot=None,
        )
        self.assertEqual(pool_name, "综合")
        codes = {c["code"] for c in candidates}
        self.assertEqual(len(candidates), 4)
        self.assertIn("000001", codes)
        self.assertIn("000002", codes)
        self.assertIn("600000", codes)
        self.assertIn("600036", codes)

    def test_resolve_tracked_candidates_uses_snapshot_entries_when_available(self):
        """有快照时从快照 entries 中读取池子信息，忽略 stocks_cfg。"""
        snapshot = {
            "entries": [
                {"code": "888888", "name": "快照核心", "bucket": "core"},
                {"code": "777777", "name": "快照观察", "bucket": "watch"},
            ]
        }
        candidates, pool_name, source = stock_screener._resolve_tracked_candidates(
            pool="all",
            stocks_cfg=self.stocks_cfg,
            blacklist=self.blacklist,
            default_query="",
            select_type="A股",
            current_snapshot=snapshot,
        )
        codes = {c["code"] for c in candidates}
        self.assertIn("888888", codes)
        self.assertIn("777777", codes)
        # stocks_cfg 中的原始池子不应出现
        self.assertNotIn("000001", codes)
        self.assertNotIn("600000", codes)

    def test_resolve_tracked_candidates_filters_blacklist_from_snapshot(self):
        """快照结果中也要过滤黑名单代码。"""
        snapshot = {
            "entries": [
                {"code": "888888", "name": "正常", "bucket": "core"},
                {"code": "999999", "name": "黑名单", "bucket": "core"},
            ]
        }
        candidates, pool_name, source = stock_screener._resolve_tracked_candidates(
            pool="all",
            stocks_cfg=self.stocks_cfg,
            blacklist=self.blacklist,
            default_query="",
            select_type="A股",
            current_snapshot=snapshot,
        )
        codes = {c["code"] for c in candidates}
        self.assertIn("888888", codes)
        self.assertNotIn("999999", codes)

    def test_resolve_tracked_candidates_mx_filter_intersects_with_base_pool(self):
        """有 mx 查询时，只保留同时在基础池和 mx 结果中的股票。"""
        # _call_mx_screener is a module-level function; patch at the module namespace.
        with mock.patch(
            "scripts.pipeline.stock_screener._call_mx_screener",
            return_value=[
                {"code": "000001", "name": "平安银行"},
                {"code": "000002", "name": "万科A"},
                {"code": "999999", "name": "外部股票"},
            ],
        ):
            candidates, pool_name, source = stock_screener._resolve_tracked_candidates(
                pool="core",
                stocks_cfg=self.stocks_cfg,
                blacklist=self.blacklist,
                default_query="银行",
                select_type="A股",
                current_snapshot=None,
            )
        codes = {c["code"] for c in candidates}
        # 基础池中有的才保留，外部股票被过滤
        self.assertIn("000001", codes)
        self.assertIn("000002", codes)
        self.assertNotIn("999999", codes)
        self.assertEqual(source, "妙想智能选股")

    def test_resolve_tracked_candidates_fallback_when_mx_returns_empty(self):
        """mx 调用失败时 fallback 回 akshare 原生接口。"""
        with mock.patch(
            "scripts.pipeline.stock_screener._call_mx_screener",
            return_value=[],
        ):
            candidates, pool_name, source = stock_screener._resolve_tracked_candidates(
                pool="core",
                stocks_cfg=self.stocks_cfg,
                blacklist=self.blacklist,
                default_query="",
                select_type="A股",
                current_snapshot=None,
            )
        codes = {c["code"] for c in candidates}
        self.assertIn("000001", codes)
        self.assertIn("000002", codes)
        self.assertEqual(source, "akshare 原生接口")

    def test_resolve_tracked_candidates_deduplicates_by_code(self):
        """快照和配置中出现重复代码时去重。"""
        snapshot = {
            "entries": [
                {"code": "000001", "name": "快照平安", "bucket": "core"},
                {"code": "000001", "name": "快照平安2", "bucket": "watch"},
            ]
        }
        candidates, pool_name, source = stock_screener._resolve_tracked_candidates(
            pool="all",
            stocks_cfg=self.stocks_cfg,
            blacklist=self.blacklist,
            default_query="",
            select_type="A股",
            current_snapshot=snapshot,
        )
        # 000001 只出现一次
        codes = [c["code"] for c in candidates]
        self.assertEqual(codes.count("000001"), 1)


class EvaluatePoolActionsTests(unittest.TestCase):
    """Tests for evaluate_pool_actions via the screener's integration path."""

    def _fake_score_result(self, code: str, name: str, total_score: float, veto: bool = False) -> dict:
        return {
            "code": code,
            "name": name,
            "total_score": total_score,
            "technical_score": total_score * 0.3,
            "fundamental_score": total_score * 0.3,
            "flow_score": total_score * 0.2,
            "sentiment_score": total_score * 0.2,
            "veto_triggered": veto,
        }

    def _make_stocks_cfg(self) -> dict:
        return {
            "core_pool": [{"code": "000001", "name": "平安银行"}],
            "watch_pool": [{"code": "600000", "name": "浦发银行"}],
        }

    def _make_strategy_cfg(self) -> dict:
        return {
            "pool_management": {
                "watch_min_score": 5.0,
                "promote_min_score": 7.0,
                "promote_streak_days": 2,
                "demote_max_score": 5.0,
                "demote_streak_days": 2,
                "remove_max_score": 4.0,
                "remove_streak_days": 2,
                "add_to_watch_streak_days": 1,
                "veto_immediate_demote": True,
            }
        }

    def _make_empty_snapshot(self) -> dict:
        return {"entries": [], "summary": {"core_count": 0, "watch_count": 0}}

    def _make_state_with_streak(self, codes: dict[str, dict]) -> dict:
        """Build code_state fixture. Seeds prev state with today's date so the
        streak logic increments from the existing value instead of resetting."""
        today = datetime.now().strftime("%Y-%m-%d")
        for code_data in codes.values():
            code_data["last_date"] = today
        return codes

    def test_evaluate_pool_actions_promotes_watch_to_core(self):
        """观察池股票连续高分时建议晋级核心池。"""
        scored = [
            self._fake_score_result("600000", "浦发银行", 8.0),
            self._fake_score_result("600036", "招商银行", 7.5),
        ]
        snapshot = {
            "entries": [
                {"code": "600000", "name": "浦发银行", "bucket": "watch", "total_score": 8.0},
                {"code": "600036", "name": "招商银行", "bucket": "watch", "total_score": 7.0},
            ],
            "summary": {"core_count": 0, "watch_count": 2},
        }
        # Seed with last_date=today so streak is READ (not incremented).
        # To trigger promotion (high_streak >= 2), seed high_streak: 2 directly.
        state_with_streak = self._make_state_with_streak({
            "600000": {
                "name": "浦发银行",
                "last_date": datetime.now().strftime("%Y-%m-%d"),
                "last_score": 8.0,
                "last_veto": False,
                "last_veto_signals": [],
                "membership": "watch",
                "data_quality": "ok",
                "data_missing_fields": [],
                "high_streak": 2,   # already at 2 → promote
                "low_streak": 0,
                "watch_streak": 1,
                "veto_streak": 0,
            },
            "600036": {
                "name": "招商银行",
                "last_date": datetime.now().strftime("%Y-%m-%d"),
                "last_score": 7.0,
                "last_veto": False,
                "last_veto_signals": [],
                "membership": "watch",
                "data_quality": "ok",
                "data_missing_fields": [],
                "high_streak": 0,   # below 2 → no promote
                "low_streak": 0,
                "watch_streak": 1,
                "veto_streak": 0,
            },
        })
        with (
            mock.patch("scripts.utils.pool_manager.load_pool_snapshot", return_value=snapshot),
            mock.patch("scripts.utils.pool_manager.get_strategy", return_value=self._make_strategy_cfg()),
            mock.patch("scripts.utils.pool_manager._load_previous_code_state", return_value=state_with_streak),
            mock.patch("scripts.utils.pool_manager.save_pool_snapshot", return_value="mock_db_path"),
        ):
            suggestions, meta = stock_screener.evaluate_pool_actions(
                scored,
                self._make_stocks_cfg(),
                self._make_strategy_cfg(),
                current_snapshot=snapshot,
                source="test",
            )
        self.assertIn("promote_to_core", suggestions)
        self.assertGreater(len(suggestions["promote_to_core"]), 0)
        promoted_codes = {item["code"] for item in suggestions["promote_to_core"]}
        self.assertIn("600000", promoted_codes)

    def test_evaluate_pool_actions_demotes_core_below_threshold(self):
        """核心池股票连续低分时建议降级。"""
        scored = [
            self._fake_score_result("000001", "平安银行", 4.0),
        ]
        snapshot = {
            "entries": [
                {"code": "000001", "name": "平安银行", "bucket": "core", "total_score": 4.0},
            ],
            "summary": {"core_count": 1, "watch_count": 0},
        }
        # Seed with last_date=today so streak is READ (not incremented).
        # To trigger demotion (low_streak >= 2), seed low_streak: 2 directly.
        state_with_streak = self._make_state_with_streak({
            "000001": {
                "name": "平安银行",
                "last_date": datetime.now().strftime("%Y-%m-%d"),
                "last_score": 4.0,
                "last_veto": False,
                "last_veto_signals": [],
                "membership": "core",
                "data_quality": "ok",
                "data_missing_fields": [],
                "high_streak": 0,
                "low_streak": 2,   # already at 2 → demote
                "watch_streak": 0,
                "veto_streak": 0,
            },
        })
        with (
            mock.patch("scripts.utils.pool_manager.load_pool_snapshot", return_value=snapshot),
            mock.patch("scripts.utils.pool_manager.get_strategy", return_value=self._make_strategy_cfg()),
            mock.patch("scripts.utils.pool_manager._load_previous_code_state", return_value=state_with_streak),
            mock.patch("scripts.utils.pool_manager.save_pool_snapshot", return_value="mock_db_path"),
        ):
            suggestions, meta = stock_screener.evaluate_pool_actions(
                scored,
                self._make_stocks_cfg(),
                self._make_strategy_cfg(),
                current_snapshot=snapshot,
                source="test",
            )
        self.assertIn("demote_from_core", suggestions)
        demoted_codes = {item["code"] for item in suggestions["demote_from_core"]}
        self.assertIn("000001", demoted_codes)

    def test_evaluate_pool_actions_adds_new_to_watch(self):
        """新股票（不在任何池）达到观察池门槛时建议加入。"""
        scored = [
            self._fake_score_result("300001", "新股票A", 5.5),
        ]
        snapshot = {"entries": [], "summary": {"core_count": 0, "watch_count": 0}}
        with (
            mock.patch("scripts.utils.pool_manager.load_pool_snapshot", return_value=snapshot),
            mock.patch("scripts.utils.pool_manager.get_strategy", return_value=self._make_strategy_cfg()),
            mock.patch("scripts.utils.pool_manager._load_previous_code_state", return_value={}),
            mock.patch("scripts.utils.pool_manager.save_pool_snapshot", return_value="mock_db_path"),
        ):
            suggestions, meta = stock_screener.evaluate_pool_actions(
                scored,
                self._make_stocks_cfg(),
                self._make_strategy_cfg(),
                current_snapshot=snapshot,
                source="test",
            )
        self.assertIn("add_to_watch", suggestions)
        add_codes = {item["code"] for item in suggestions["add_to_watch"]}
        self.assertIn("300001", add_codes)

    def test_evaluate_pool_actions_removes_watch_below_threshold(self):
        """观察池股票连续低于移除阈值时建议移出。"""
        scored = [
            self._fake_score_result("600000", "浦发银行", 3.5),
        ]
        snapshot = {
            "entries": [
                {"code": "600000", "name": "浦发银行", "bucket": "watch", "total_score": 3.5},
            ],
            "summary": {"core_count": 0, "watch_count": 1},
        }
        with (
            mock.patch("scripts.utils.pool_manager.load_pool_snapshot", return_value=snapshot),
            mock.patch("scripts.utils.pool_manager.get_strategy", return_value=self._make_strategy_cfg()),
            mock.patch("scripts.utils.pool_manager._load_previous_code_state", return_value={}),
            mock.patch("scripts.utils.pool_manager.save_pool_snapshot", return_value="mock_db_path"),
        ):
            suggestions, meta = stock_screener.evaluate_pool_actions(
                scored,
                self._make_stocks_cfg(),
                self._make_strategy_cfg(),
                current_snapshot=snapshot,
                source="test",
            )
        self.assertIn("remove_or_avoid", suggestions)
        removed_codes = {item["code"] for item in suggestions["remove_or_avoid"]}
        self.assertIn("600000", removed_codes)

    def test_evaluate_pool_actions_veto_triggers_immediate_demote(self):
        """触发 veto 的核心池股票立即降级。"""
        scored = [
            self._fake_score_result("000001", "平安银行", 7.0, veto=True),
        ]
        snapshot = {
            "entries": [
                {"code": "000001", "name": "平安银行", "bucket": "core"},
            ],
            "summary": {"core_count": 1, "watch_count": 0},
        }
        with (
            mock.patch("scripts.utils.pool_manager.load_pool_snapshot", return_value=snapshot),
            mock.patch("scripts.utils.pool_manager.get_strategy", return_value=self._make_strategy_cfg()),
            mock.patch("scripts.utils.pool_manager._load_previous_code_state", return_value={}),
            mock.patch("scripts.utils.pool_manager.save_pool_snapshot", return_value="mock_db_path"),
        ):
            suggestions, meta = stock_screener.evaluate_pool_actions(
                scored,
                self._make_stocks_cfg(),
                self._make_strategy_cfg(),
                current_snapshot=snapshot,
                source="test",
            )
        self.assertIn("demote_from_core", suggestions)
        demoted_codes = {item["code"] for item in suggestions["demote_from_core"]}
        self.assertIn("000001", demoted_codes)

    def test_evaluate_pool_actions_keeps_high_score_watch(self):
        """观察池高分股票保留在观察池。"""
        scored = [
            self._fake_score_result("600036", "招商银行", 6.5),
        ]
        snapshot = {
            "entries": [
                {"code": "600036", "name": "招商银行", "bucket": "watch", "total_score": 6.5},
            ],
            "summary": {"core_count": 0, "watch_count": 1},
        }
        with (
            mock.patch("scripts.utils.pool_manager.load_pool_snapshot", return_value=snapshot),
            mock.patch("scripts.utils.pool_manager.get_strategy", return_value=self._make_strategy_cfg()),
            mock.patch("scripts.utils.pool_manager._load_previous_code_state", return_value={}),
            mock.patch("scripts.utils.pool_manager.save_pool_snapshot", return_value="mock_db_path"),
        ):
            suggestions, meta = stock_screener.evaluate_pool_actions(
                scored,
                self._make_stocks_cfg(),
                self._make_strategy_cfg(),
                current_snapshot=snapshot,
                source="test",
            )
        # High-score watch stocks should be kept, not demoted
        self.assertIn("keep_watch", suggestions)
        kept_codes = {item["code"] for item in suggestions["keep_watch"]}
        self.assertIn("600036", kept_codes)


class RunScreenerTests(unittest.TestCase):
    """Tests for the top-level run() function and its orchestration logic."""

    def _fake_scored(self, codes=None):
        codes = codes or ["000001", "600000"]
        return [
            {
                "code": c,
                "name": f"股票{c}",
                "total_score": 7.5,
                "technical_score": 2.5,
                "fundamental_score": 2.0,
                "flow_score": 1.5,
                "sentiment_score": 1.5,
                "veto_triggered": False,
            }
            for c in codes
        ]

    def _fake_today_decision(self):
        return {
            "action": "NO_TRADE",
            "portfolio_risk": {"state": "ok", "reason_codes": [], "reasons": []},
        }

    def _fake_pool_snapshot(self):
        return {
            "entries": [],
            "summary": {"core_count": 0, "watch_count": 0},
        }

    def _fake_pool_meta(self):
        return {
            "snapshot_entries": [
                {"code": "000001", "name": "股票000001", "bucket": "core", "total_score": 7.5},
                {"code": "600000", "name": "股票600000", "bucket": "watch", "total_score": 7.5},
            ],
            "snapshot_summary": {"core_count": 1, "watch_count": 1},
            "rules": {
                "promote_streak_days": 2,
                "promote_min_score": 7.0,
                "watch_min_score": 5.0,
                "demote_streak_days": 2,
                "demote_max_score": 5.0,
                "remove_streak_days": 2,
                "remove_max_score": 4.0,
                "add_to_watch_streak_days": 1,
            },
        }

    def test_run_returns_scored_results_on_success(self):
        """run() 成功执行时返回评分结果列表。"""
        scored = self._fake_scored()

        with (
            mock.patch.object(stock_screener, "get_stocks", return_value={
                "core_pool": [{"code": "000001", "name": "股票000001"}],
                "watch_pool": [],
            }),
            mock.patch.object(stock_screener, "get_strategy", return_value={
                "screening": {"mx_query": "", "mx_select_type": "A股"},
                "pool_management": {},
            }),
            mock.patch.object(stock_screener, "load_pool_snapshot", return_value=self._fake_pool_snapshot()),
            mock.patch.object(stock_screener, "_mx_health_snapshot", return_value={"status": "ok"}),
            mock.patch.object(stock_screener, "batch_score", return_value=scored),
            mock.patch.object(stock_screener, "build_today_decision", return_value=self._fake_today_decision()),
            mock.patch.object(stock_screener, "evaluate_pool_actions", return_value=({}, self._fake_pool_meta())),
            mock.patch.object(stock_screener, "save_pool_snapshot", return_value="/tmp/pool_snap.json"),
            mock.patch.object(stock_screener, "load_market_snapshot", return_value={"signal": "GREEN"}),
            mock.patch.object(stock_screener, "save_market_snapshot_history"),
            mock.patch.object(stock_screener, "save_decision_snapshot_history"),
            mock.patch.object(stock_screener, "save_candidate_snapshot_history"),
            mock.patch.object(stock_screener, "_write_screening_result", return_value="/tmp/screening.md"),
            mock.patch.object(stock_screener, "_write_pool_suggestions", return_value="/tmp/suggestions.md"),
            mock.patch.object(stock_screener, "_sync_to_zixuan"),
            mock.patch.object(stock_screener, "update_pipeline_state"),
            mock.patch.object(stock_screener, "ObsidianVault") as MockVault,
        ):
            MockVault.return_value.write_today_decision = mock.MagicMock()
            MockVault.return_value.vault_path = "/tmp/vault"
            MockVault.return_value.screening_results_dir = "screening_results"

            result = stock_screener.run(pool="core", universe="tracked")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["code"], "000001")
        self.assertEqual(result[0]["total_score"], 7.5)

    def test_run_calls_batch_score_with_resolved_candidates(self):
        """run() 将解析后的候选股票传给 batch_score。"""
        batch_mock = mock.MagicMock(return_value=[])
        stocks_cfg = {
            "core_pool": [{"code": "000001", "name": "股票A"}],
            "watch_pool": [],
        }
        strategy_cfg = {
            "screening": {"mx_query": "", "mx_select_type": "A股"},
            "pool_management": {},
        }

        with (
            mock.patch.object(stock_screener, "get_stocks", return_value=stocks_cfg),
            mock.patch.object(stock_screener, "get_strategy", return_value=strategy_cfg),
            mock.patch.object(stock_screener, "load_pool_snapshot", return_value=self._fake_pool_snapshot()),
            mock.patch.object(stock_screener, "_mx_health_snapshot", return_value={"status": "ok"}),
            mock.patch.object(stock_screener, "batch_score", batch_mock),
            mock.patch.object(stock_screener, "build_today_decision", return_value=self._fake_today_decision()),
            mock.patch.object(stock_screener, "evaluate_pool_actions", return_value=({}, self._fake_pool_meta())),
            mock.patch.object(stock_screener, "save_pool_snapshot", return_value="/tmp/pool_snap.json"),
            mock.patch.object(stock_screener, "load_market_snapshot", return_value={"signal": "GREEN"}),
            mock.patch.object(stock_screener, "save_market_snapshot_history"),
            mock.patch.object(stock_screener, "save_decision_snapshot_history"),
            mock.patch.object(stock_screener, "save_candidate_snapshot_history"),
            mock.patch.object(stock_screener, "_write_screening_result", return_value="/tmp/screening.md"),
            mock.patch.object(stock_screener, "_write_pool_suggestions", return_value="/tmp/suggestions.md"),
            mock.patch.object(stock_screener, "_sync_to_zixuan"),
            mock.patch.object(stock_screener, "update_pipeline_state"),
            mock.patch.object(stock_screener, "ObsidianVault") as MockVault,
        ):
            MockVault.return_value.write_today_decision = mock.MagicMock()
            MockVault.return_value.vault_path = "/tmp/vault"
            MockVault.return_value.screening_results_dir = "screening_results"

            stock_screener.run(pool="core", universe="tracked")

        batch_mock.assert_called_once()
        call_arg = batch_mock.call_args[0][0]
        self.assertEqual(len(call_arg), 1)
        self.assertEqual(call_arg[0]["code"], "000001")

    def test_run_returns_empty_when_no_candidates(self):
        """无候选股票时 run() 返回空列表并更新 pipeline 状态为 skipped。"""
        state_updates = []

        def capture_state(*args):
            state_updates.append(args)

        with (
            mock.patch.object(stock_screener, "get_stocks", return_value={
                "core_pool": [],
                "watch_pool": [],
            }),
            mock.patch.object(stock_screener, "get_strategy", return_value={
                "screening": {"mx_query": "", "mx_select_type": "A股"},
                "pool_management": {},
            }),
            mock.patch.object(stock_screener, "load_pool_snapshot", return_value=self._fake_pool_snapshot()),
            mock.patch.object(stock_screener, "_mx_health_snapshot", return_value={"status": "ok"}),
            mock.patch.object(stock_screener, "_call_mx_screener", return_value=[]),
            mock.patch.object(stock_screener, "update_pipeline_state", side_effect=capture_state),
        ):
            result = stock_screener.run(pool="core", universe="tracked")

        self.assertEqual(result, [])
        self.assertEqual(state_updates[0][1], "skipped")

    def test_run_handles_market_universe_with_mx_fallback(self):
        """universe='market' 时走全市场路径并使用 mx 筛选结果。"""
        mx_results = [
            {"code": "000001", "name": "平安银行"},
            {"code": "600000", "name": "浦发银行"},
        ]
        scored = self._fake_scored(["000001", "600000"])

        with (
            mock.patch.object(stock_screener, "get_stocks", return_value={
                "core_pool": [], "watch_pool": [],
            }),
            mock.patch.object(stock_screener, "get_strategy", return_value={
                "screening": {
                    "mx_query": "银行股",
                    "mx_select_type": "A股",
                    "candidate_cache_ttl_hours": 24,
                },
                "pool_management": {},
            }),
            mock.patch.object(stock_screener, "load_pool_snapshot", return_value=self._fake_pool_snapshot()),
            mock.patch.object(stock_screener, "_mx_health_snapshot", return_value={"status": "ok"}),
            mock.patch.object(stock_screener, "_call_mx_screener", return_value=mx_results),
            mock.patch.object(stock_screener, "batch_score", return_value=scored),
            mock.patch.object(stock_screener, "build_today_decision", return_value=self._fake_today_decision()),
            mock.patch.object(stock_screener, "evaluate_pool_actions", return_value=({}, self._fake_pool_meta())),
            mock.patch.object(stock_screener, "save_pool_snapshot", return_value="/tmp/pool_snap.json"),
            mock.patch.object(stock_screener, "load_market_snapshot", return_value={"signal": "GREEN"}),
            mock.patch.object(stock_screener, "save_market_snapshot_history"),
            mock.patch.object(stock_screener, "save_decision_snapshot_history"),
            mock.patch.object(stock_screener, "save_candidate_snapshot_history"),
            mock.patch.object(stock_screener, "_write_screening_result", return_value="/tmp/screening.md"),
            mock.patch.object(stock_screener, "_write_pool_suggestions", return_value="/tmp/suggestions.md"),
            mock.patch.object(stock_screener, "_write_market_scan_watchlist", return_value="/tmp/watchlist.md"),
            mock.patch.object(stock_screener, "_sync_to_zixuan"),
            mock.patch.object(stock_screener, "update_pipeline_state"),
            mock.patch.object(stock_screener, "ObsidianVault") as MockVault,
        ):
            MockVault.return_value.write_today_decision = mock.MagicMock()
            MockVault.return_value.vault_path = "/tmp/vault"
            MockVault.return_value.screening_results_dir = "screening_results"

            result = stock_screener.run(pool="watch", universe="market")

        self.assertEqual(len(result), 2)
        scored_codes = {r["code"] for r in result}
        self.assertIn("000001", scored_codes)
        self.assertIn("600000", scored_codes)

    def test_run_reports_mx_health_in_pipeline_state(self):
        """run() 成功时在 pipeline state 中记录 mx health 状态。"""
        scored = self._fake_scored()
        mx_health = {"status": "ok", "available_count": 3, "unavailable_count": 0}
        state_updates = {}

        def capture_state(*args):
            state_updates["key"] = args

        with (
            mock.patch.object(stock_screener, "get_stocks", return_value={
                "core_pool": [{"code": "000001", "name": "A"}], "watch_pool": [],
            }),
            mock.patch.object(stock_screener, "get_strategy", return_value={
                "screening": {"mx_query": "", "mx_select_type": "A股"},
                "pool_management": {},
            }),
            mock.patch.object(stock_screener, "load_pool_snapshot", return_value=self._fake_pool_snapshot()),
            mock.patch.object(stock_screener, "_mx_health_snapshot", return_value=mx_health),
            mock.patch.object(stock_screener, "batch_score", return_value=scored),
            mock.patch.object(stock_screener, "build_today_decision", return_value=self._fake_today_decision()),
            mock.patch.object(stock_screener, "evaluate_pool_actions", return_value=({}, self._fake_pool_meta())),
            mock.patch.object(stock_screener, "save_pool_snapshot", return_value="/tmp/pool_snap.json"),
            mock.patch.object(stock_screener, "load_market_snapshot", return_value={"signal": "GREEN"}),
            mock.patch.object(stock_screener, "save_market_snapshot_history"),
            mock.patch.object(stock_screener, "save_decision_snapshot_history"),
            mock.patch.object(stock_screener, "save_candidate_snapshot_history"),
            mock.patch.object(stock_screener, "_write_screening_result", return_value="/tmp/screening.md"),
            mock.patch.object(stock_screener, "_write_pool_suggestions", return_value="/tmp/suggestions.md"),
            mock.patch.object(stock_screener, "_sync_to_zixuan"),
            mock.patch.object(stock_screener, "update_pipeline_state", side_effect=capture_state),
            mock.patch.object(stock_screener, "ObsidianVault") as MockVault,
        ):
            MockVault.return_value.write_today_decision = mock.MagicMock()
            MockVault.return_value.vault_path = "/tmp/vault"
            MockVault.return_value.screening_results_dir = "screening_results"

            stock_screener.run(pool="core", universe="tracked")

        _, _, meta, _ = state_updates["key"]
        self.assertEqual(meta["mx_health"]["status"], "ok")
        self.assertEqual(meta["mx_health"]["available_count"], 3)


if __name__ == "__main__":
    unittest.main()
