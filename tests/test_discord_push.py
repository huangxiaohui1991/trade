import unittest


class DiscordPushTests(unittest.TestCase):
    def test_morning_embed_includes_condition_order_preview(self):
        from scripts.utils.discord_push import _build_morning_embeds

        embeds = _build_morning_embeds({
            "date": "2026-04-11",
            "weekday": "周六",
            "market_signal": "GREEN",
            "market": {},
            "positions": [
                {
                    "name": "艾比森",
                    "shares": 100,
                    "price": 10.5,
                    "currency": "¥",
                    "note": "",
                },
            ],
            "condition_orders": [
                {
                    "name": "艾比森",
                    "type": "止损",
                    "price": 10.08,
                    "currency": "¥",
                    "quantity": "100股",
                    "note": "动态止损",
                },
                {
                    "name": "艾比森",
                    "type": "止盈(第一批)",
                    "price": 12.08,
                    "currency": "¥",
                    "quantity": "1/3仓",
                    "note": "+15%卖1/3",
                },
            ],
            "core_pool": [],
            "weekly_bought": 0,
            "weekly_limit": 2,
        })

        fields = embeds[0]["fields"]
        preview_field = next(field for field in fields if field["name"] == "⏰ 条件单预览（2 笔）")
        self.assertIn("艾比森 止损", preview_field["value"])
        self.assertIn("艾比森 止盈(第一批)", preview_field["value"])

    def test_evening_core_pool_includes_data_quality_note(self):
        from scripts.utils.discord_push import _build_evening_embeds

        embeds = _build_evening_embeds({
            "date": "2026-04-10",
            "weekday": "周五",
            "market": {},
            "positions": [],
            "total_value": 0,
            "alerts": [],
            "core_pool": [
                {
                    "name": "艾比森",
                    "score": 6.8,
                    "note": "观察",
                    "data_quality": "degraded",
                    "data_missing_fields": ["营收", "现金流"],
                },
            ],
            "tomorrow_plan": [],
        })

        fields = embeds[0]["fields"]
        core_field = next(field for field in fields if field["name"] == "艾比森")
        self.assertIn("⚠️ 数据降级", core_field["value"])
        self.assertIn("缺失:营收,现金流", core_field["value"])

    def test_evening_embed_includes_condition_order_suggestions(self):
        from scripts.utils.discord_push import _build_evening_embeds

        embeds = _build_evening_embeds({
            "date": "2026-04-10",
            "weekday": "周五",
            "market": {},
            "positions": [],
            "total_value": 0,
            "alerts": [],
            "condition_orders": [
                {
                    "name": "艾比森",
                    "type": "止损",
                    "price": 10.08,
                    "currency": "¥",
                    "quantity": "100股",
                    "note": "动态止损",
                },
                {
                    "name": "艾比森",
                    "type": "绝对止损",
                    "price": 9.77,
                    "currency": "¥",
                    "quantity": "100股",
                    "note": "-7%无条件",
                },
                {
                    "name": "艾比森",
                    "type": "止盈(第一批)",
                    "price": 12.08,
                    "currency": "¥",
                    "quantity": "1/3仓",
                    "note": "+15%卖1/3",
                },
            ],
            "core_pool": [],
            "tomorrow_plan": [],
        })

        fields = embeds[0]["fields"]
        order_field = next(field for field in fields if field["name"] == "⏰ 明日挂单建议（3 笔）")
        self.assertIn("艾比森 止损", order_field["value"])
        self.assertIn("艾比森 绝对止损", order_field["value"])
        self.assertIn("艾比森 止盈(第一批)", order_field["value"])


if __name__ == "__main__":
    unittest.main()
