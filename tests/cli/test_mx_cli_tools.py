import unittest
from unittest import mock


class _RecorderBase:
    instances = []

    def __init__(self):
        self.calls = []
        type(self).instances.append(self)


class FakeData(_RecorderBase):
    def query(self, query):
        self.calls.append(("query", query))
        return {"client": "data", "query": query}


class FakeSearch(_RecorderBase):
    def search(self, query):
        self.calls.append(("search", query))
        return {"client": "search", "query": query}


class FakeXuangu(_RecorderBase):
    def search(self, query):
        self.calls.append(("search", query))
        return {"client": "xuangu", "query": query}


class FakeZixuan(_RecorderBase):
    def query(self):
        self.calls.append(("query",))
        return {"client": "zixuan", "action": "query"}

    def manage(self, query):
        self.calls.append(("manage", query))
        return {"client": "zixuan", "action": "manage", "query": query}


class FakeMoni(_RecorderBase):
    def positions(self):
        self.calls.append(("positions",))
        return {"client": "moni", "action": "positions"}

    def balance(self):
        self.calls.append(("balance",))
        return {"client": "moni", "action": "balance"}

    def orders(self):
        self.calls.append(("orders",))
        return {"client": "moni", "action": "orders"}

    def trade(self, side, stock_code, quantity, price, use_market_price):
        self.calls.append(("trade", side, stock_code, quantity, price, use_market_price))
        return {
            "client": "moni",
            "action": "trade",
            "side": side,
            "stock_code": stock_code,
            "quantity": quantity,
            "price": price,
            "use_market_price": use_market_price,
        }

    def cancel(self, order_id=None, cancel_all=False):
        self.calls.append(("cancel", order_id, cancel_all))
        return {"client": "moni", "action": "cancel", "order_id": order_id, "cancel_all": cancel_all}


def _fake_loader_factory(overrides):
    def loader(module_name, class_name):
        return overrides.get((module_name, class_name), (None, "missing"))

    return loader


class MXCliToolsTests(unittest.TestCase):
    def setUp(self):
        for cls in (FakeData, FakeSearch, FakeXuangu, FakeZixuan, FakeMoni):
            cls.instances = []

    def test_registry_metadata_and_groups_are_structured(self):
        import scripts.mx.cli_tools as cli_tools

        overrides = {
            ("scripts.mx.mx_data", "MXData"): (FakeData, None),
            ("scripts.mx.mx_search", "MXSearch"): (FakeSearch, None),
            ("scripts.mx.mx_xuangu", "MXXuangu"): (FakeXuangu, None),
            ("scripts.mx.mx_zixuan", "MXZixuan"): (FakeZixuan, None),
            ("scripts.mx.mx_moni", "MXMoni"): (FakeMoni, None),
        }
        with mock.patch.object(cli_tools, "_load_command_class", side_effect=_fake_loader_factory(overrides)):
            registry = cli_tools.build_mx_command_registry()

        metadata = registry.metadata()
        ids = {item["id"] for item in metadata}
        self.assertIn("mx.data.query", ids)
        self.assertIn("mx.search.news", ids)
        self.assertIn("mx.xuangu.search", ids)
        self.assertIn("mx.zixuan.query", ids)
        self.assertIn("mx.moni.cancel_all", ids)

        self.assertEqual(registry.resolve("data").id, "mx.data.query")
        self.assertEqual(registry.resolve("news").id, "mx.search.news")
        self.assertEqual(registry.resolve("xuangu").id, "mx.xuangu.search")
        self.assertEqual(registry.resolve("zixuan").id, "mx.zixuan.query")
        self.assertEqual(registry.resolve("moni.cancel").id, "mx.moni.cancel")

        buy_spec = registry.resolve("mx.moni.buy")
        self.assertEqual([arg.name for arg in buy_spec.args], ["stock_code", "quantity", "price", "use_market_price"])
        self.assertEqual(buy_spec.args[3].default, False)

        groups = registry.by_group()
        self.assertEqual(set(groups.keys()), {"data", "search", "xuangu", "zixuan", "moni"})
        self.assertGreaterEqual(len(registry.dispatch_table()), 12)

    def test_dispatch_routes_to_underlying_mx_clients(self):
        import scripts.mx.cli_tools as cli_tools

        overrides = {
            ("scripts.mx.mx_data", "MXData"): (FakeData, None),
            ("scripts.mx.mx_search", "MXSearch"): (FakeSearch, None),
            ("scripts.mx.mx_xuangu", "MXXuangu"): (FakeXuangu, None),
            ("scripts.mx.mx_zixuan", "MXZixuan"): (FakeZixuan, None),
            ("scripts.mx.mx_moni", "MXMoni"): (FakeMoni, None),
        }
        with mock.patch.object(cli_tools, "_load_command_class", side_effect=_fake_loader_factory(overrides)):
            registry = cli_tools.build_mx_command_registry()

        data_result = registry.dispatch("mx.data.query", query="东方财富最新价")
        search_result = registry.dispatch("news", query="贵州茅台最新研报")
        xuangu_result = registry.dispatch("xuangu", query="净利润增长率大于30%的股票")
        zixuan_query_result = registry.dispatch("zixuan")
        zixuan_manage_result = registry.dispatch("mx.zixuan.manage", query="把比亚迪加入自选")
        moni_buy_result = registry.dispatch(
            "mx.moni.buy",
            stock_code="600519",
            quantity=100,
            price=1700.0,
            use_market_price=False,
        )
        moni_cancel_all_result = registry.dispatch("mx.moni.cancel_all")

        self.assertEqual(data_result["client"], "data")
        self.assertEqual(search_result["client"], "search")
        self.assertEqual(xuangu_result["client"], "xuangu")
        self.assertEqual(zixuan_query_result["action"], "query")
        self.assertEqual(zixuan_manage_result["action"], "manage")
        self.assertEqual(moni_buy_result["side"], "buy")
        self.assertEqual(moni_cancel_all_result["cancel_all"], True)

        self.assertEqual(FakeData.instances[-1].calls, [("query", "东方财富最新价")])
        self.assertEqual(FakeSearch.instances[-1].calls, [("search", "贵州茅台最新研报")])
        self.assertEqual(FakeXuangu.instances[-1].calls, [("search", "净利润增长率大于30%的股票")])
        self.assertEqual(FakeZixuan.instances[-2].calls, [("query",)])
        self.assertEqual(FakeZixuan.instances[-1].calls, [("manage", "把比亚迪加入自选")])
        self.assertEqual(
            FakeMoni.instances[-2].calls,
            [("trade", "buy", "600519", 100, 1700.0, False)],
        )
        self.assertEqual(FakeMoni.instances[-1].calls, [("cancel", None, True)])

    def test_optional_commands_can_be_hidden_or_retained(self):
        import scripts.mx.cli_tools as cli_tools

        overrides = {
            ("scripts.mx.mx_data", "MXData"): (FakeData, None),
            ("scripts.mx.mx_search", "MXSearch"): (None, "search module missing"),
            ("scripts.mx.mx_xuangu", "MXXuangu"): (None, "xuangu module missing"),
            ("scripts.mx.mx_zixuan", "MXZixuan"): (FakeZixuan, None),
            ("scripts.mx.mx_moni", "MXMoni"): (FakeMoni, None),
        }

        with mock.patch.object(cli_tools, "_load_command_class", side_effect=_fake_loader_factory(overrides)):
            registry = cli_tools.build_mx_command_registry()
            available_ids = {spec.id for spec in registry.specs()}
            self.assertIn("mx.data.query", available_ids)
            self.assertNotIn("mx.search.news", available_ids)
            self.assertNotIn("mx.xuangu.search", available_ids)

            full_registry = cli_tools.build_mx_command_registry(include_unavailable=True)
            search_spec = full_registry.resolve("mx.search.news")
            self.assertFalse(search_spec.available)
            self.assertIn("missing", search_spec.availability_note)
            with self.assertRaises(cli_tools.MXCommandUnavailable):
                full_registry.dispatch("mx.search.news", query="新能源")

        with mock.patch.object(cli_tools, "_load_command_class", side_effect=_fake_loader_factory(overrides)):
            metadata = cli_tools.list_mx_command_metadata(include_unavailable=True)
        self.assertTrue(any(item["id"] == "mx.search.news" and not item["available"] for item in metadata))

    def test_unknown_command_raises(self):
        import scripts.mx.cli_tools as cli_tools

        overrides = {
            ("scripts.mx.mx_data", "MXData"): (FakeData, None),
            ("scripts.mx.mx_search", "MXSearch"): (FakeSearch, None),
            ("scripts.mx.mx_xuangu", "MXXuangu"): (FakeXuangu, None),
            ("scripts.mx.mx_zixuan", "MXZixuan"): (FakeZixuan, None),
            ("scripts.mx.mx_moni", "MXMoni"): (FakeMoni, None),
        }
        with mock.patch.object(cli_tools, "_load_command_class", side_effect=_fake_loader_factory(overrides)):
            registry = cli_tools.build_mx_command_registry()

        with self.assertRaises(cli_tools.MXCommandNotFound):
            registry.resolve("mx.unknown.command")


if __name__ == "__main__":
    unittest.main()
