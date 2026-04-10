import unittest
from unittest import mock


class MorningNewsTests(unittest.TestCase):
    def test_get_morning_news_uses_mx_capability_dispatch(self):
        import scripts.pipeline.morning as morning

        response = {
            "data": {
                "data": {
                    "llmSearchResponse": {
                        "data": [
                            {
                                "title": "贵州茅台披露年度经营数据",
                                "date": "2026-04-09 10:30:00",
                                "informationType": "REPORT",
                                "rating": "强烈推荐",
                            },
                            {
                                "title": "贵州茅台发布公告",
                                "date": "2026-04-09",
                                "informationType": "ANNOUNCEMENT",
                            },
                        ]
                    }
                }
            }
        }

        core_items = [{"name": "贵州茅台", "code": "600519"}]
        positions = [{"name": "贵州茅台", "code": "600519"}]

        with mock.patch.object(morning, "dispatch_mx_command", return_value=response) as dispatch_mock:
            news_items = morning._get_morning_news(core_items, positions)

        self.assertEqual(len(news_items), 2)
        self.assertEqual(news_items[0]["stock"], "贵州茅台")
        self.assertEqual(news_items[0]["code"], "600519")
        self.assertEqual(news_items[0]["type"], "研报")
        self.assertEqual(news_items[0]["rating"], "强烈推荐")
        self.assertEqual(news_items[1]["type"], "公告")
        dispatch_mock.assert_called_once_with("news", query="贵州茅台 最新公告 新闻")

    def test_get_morning_news_degrades_when_mx_unavailable(self):
        import scripts.pipeline.morning as morning

        core_items = [{"name": "贵州茅台", "code": "600519"}]
        positions = []

        with mock.patch.object(morning, "dispatch_mx_command", side_effect=morning.MXCommandError("search unavailable")):
            news_items = morning._get_morning_news(core_items, positions)

        self.assertEqual(news_items, [])


if __name__ == "__main__":
    unittest.main()
