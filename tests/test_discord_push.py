import unittest


class DiscordPushTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
