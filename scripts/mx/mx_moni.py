#!/usr/bin/env python3
"""
mx_moni — 妙想模拟组合管理

支持模拟交易操作：
  - 持仓查询、资金查询、委托查询
  - 买入/卖出（限价/市价）
  - 撤单/一键撤单

用法：
  python -m scripts.mx.mx_moni "我的持仓"
  python -m scripts.mx.mx_moni "买入 600519 价格 1700 数量 100 股"
  python -m scripts.mx.mx_moni "市价卖出 600519 100 股"
  python -m scripts.mx.mx_moni "一键撤单"
"""

import os
import sys
import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.mx.client import MXBaseClient, _PROJECT_ROOT as ROOT
from scripts.utils.logger import get_logger

_logger = get_logger("mx.moni")
OUTPUT_DIR = ROOT / "data" / "mx_output"


class MXMoni(MXBaseClient):
    """妙想模拟组合管理客户端"""

    def positions(self) -> Dict[str, Any]:
        """查询持仓"""
        return self._post("/api/claw/mockTrading/positions", {"moneyUnit": 1})

    def balance(self) -> Dict[str, Any]:
        """查询资金"""
        return self._post("/api/claw/mockTrading/balance", {"moneyUnit": 1})

    def orders(self) -> Dict[str, Any]:
        """查询委托"""
        return self._post("/api/claw/mockTrading/orders", {"fltOrderDrt": 0, "fltOrderStatus": 0})

    def trade(self, trade_type: str, stock_code: str, quantity: int,
              price: Optional[float] = None, use_market_price: bool = False) -> Dict[str, Any]:
        """买入/卖出"""
        body = {
            "type": trade_type,
            "stockCode": stock_code,
            "quantity": quantity,
            "useMarketPrice": use_market_price,
        }
        if not use_market_price and price is not None:
            body["price"] = price
        return self._post("/api/claw/mockTrading/trade", body)

    def cancel(self, order_id: Optional[str] = None, cancel_all: bool = False) -> Dict[str, Any]:
        """撤单"""
        if cancel_all:
            return self._post("/api/claw/mockTrading/cancel", {"type": "all"})
        body = {"type": "order", "orderId": order_id}
        return self._post("/api/claw/mockTrading/cancel", body)


def parse_buy_sell(query: str) -> Tuple[Optional[str], Optional[float], Optional[int], bool]:
    code_match = re.search(r'(\d{6})', query)
    if not code_match:
        return None, None, None, False
    stock_code = code_match.group(1)

    quantity_match = re.search(r'(\d+)\s*(股|手)', query)
    quantity = None
    if quantity_match:
        qty = int(quantity_match.group(1))
        if quantity_match.group(2) == '手':
            qty = qty * 100
        quantity = qty

    is_market = any(word in query for word in ['市价', '市价买入', '市价卖出', '现价买入', '现价卖出'])

    price_match = re.search(r'(\d+\.?\d*)\s*元', query) if not is_market else None
    price = None
    if price_match and not is_market:
        price = float(price_match.group(1))
    elif not is_market and quantity:
        price_candidates = re.findall(r'\d+\.?\d*', query)
        for candidate in price_candidates:
            if len(candidate) != 6:
                price = float(candidate)
                break

    return stock_code, price, quantity, is_market


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if len(sys.argv) < 2:
        print("用法: python -m scripts.mx.mx_moni \"操作指令\"")
        print("  查询持仓: python -m scripts.mx.mx_moni \"我的持仓\"")
        print("  查询资金: python -m scripts.mx.mx_moni \"我的资金\"")
        print("  买入: python -m scripts.mx.mx_moni \"买入 600519 价格 1700 数量 100 股\"")
        print("  卖出: python -m scripts.mx.mx_moni \"卖出 600519 价格 1750 数量 100 股\"")
        print("  撤单: python -m scripts.mx.mx_moni \"一键撤单\"")
        sys.exit(1)

    query = ' '.join(sys.argv[1:])
    mx = MXMoni()

    try:
        if any(word in query for word in ['持仓', '我的持仓', '持仓情况']):
            result = mx.positions()
        elif any(word in query for word in ['资金', '我的资金', '账户余额', '资金情况']):
            result = mx.balance()
        elif any(word in query for word in ['委托', '我的委托', '订单', '委托记录']):
            result = mx.orders()
        elif any(word in query for word in ['买入', '买进', '建仓']):
            stock_code, price, quantity, is_market = parse_buy_sell(query)
            if not stock_code or not quantity:
                print("错误: 无法解析买入指令，请包含股票代码(6位)和数量(100的整数倍)")
                sys.exit(1)
            if not is_market and price is None:
                print("错误: 限价买入需要提供价格，或使用市价买入")
                sys.exit(1)
            if quantity % 100 != 0:
                print("错误: 委托数量必须为100的整数倍")
                sys.exit(1)
            result = mx.trade("buy", stock_code, quantity, price, is_market)
        elif any(word in query for word in ['卖出', '抛售', '减仓']):
            stock_code, price, quantity, is_market = parse_buy_sell(query)
            if not stock_code or not quantity:
                print("错误: 无法解析卖出指令")
                sys.exit(1)
            if not is_market and price is None:
                print("错误: 限价卖出需要提供价格，或使用市价卖出")
                sys.exit(1)
            if quantity % 100 != 0:
                print("错误: 委托数量必须为100的整数倍")
                sys.exit(1)
            result = mx.trade("sell", stock_code, quantity, price, is_market)
        elif any(word in query for word in ['撤单', '撤销']):
            if any(word in query for word in ['全部', '所有', '一键撤单']):
                result = mx.cancel(cancel_all=True)
            else:
                order_id_match = re.search(r'(\d{16,20})', query)
                if not order_id_match:
                    print("错误: 请提供委托编号，或使用一键撤单")
                    sys.exit(1)
                result = mx.cancel(order_id=order_id_match.group(1))
        else:
            print("无法识别意图，支持: 持仓/资金/委托/买入/卖出/撤单")
            sys.exit(1)

        # 输出结果
        print(json.dumps(result, ensure_ascii=False, indent=2))

        # 保存原始 JSON
        safe_query = query.replace(' ', '_')[:60]
        json_path = OUTPUT_DIR / f"mx_moni_{safe_query}_raw.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\n📄 结果已保存: {json_path}")

    except Exception as e:
        print(f"错误: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
