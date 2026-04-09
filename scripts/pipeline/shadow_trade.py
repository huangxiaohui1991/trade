#!/usr/bin/env python3
"""
pipeline/shadow_trade.py — 影子交易引擎

用妙想模拟盘自动执行交易系统的买卖信号，验证策略有效性。

功能：
  - buy_new_picks: 核心池新入池股票 → 模拟盘市价买入
  - check_stop_signals: 检查模拟盘持仓的止损/止盈信号 → 自动卖出
  - get_performance: 拉模拟盘数据，统计胜率/盈亏比
  - sync_report: 生成模拟盘周报写入 Obsidian

用法：
  python -m scripts.pipeline.shadow_trade buy     # 对核心池执行买入
  python -m scripts.pipeline.shadow_trade check   # 检查止损止盈
  python -m scripts.pipeline.shadow_trade status   # 查看模拟盘状态
  python -m scripts.pipeline.shadow_trade report   # 生成报告
"""

import os
import sys
import math
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from scripts.mx.mx_moni import MXMoni
from scripts.engine.scorer import score as score_stock
from scripts.utils.config_loader import get_stocks, get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("pipeline.shadow_trade")

# 每只股票的模拟买入金额（元）
POSITION_SIZE = 20000


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def _get_moni() -> MXMoni:
    return MXMoni()


def _log_trade(action: str, code: str, name: str, shares: int,
               price: float, reason: str = "") -> None:
    """
    记录模拟盘交易到 Obsidian 交易日志。
    追加到 03-复盘/模拟盘/交易记录.md
    """
    vault_path = os.environ.get("AStockVault", _PROJECT_ROOT)
    log_dir = Path(vault_path) / "03-复盘" / "模拟盘"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "交易记录.md"

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    amount = round(shares * price, 2)

    # 如果文件不存在，写表头
    if not log_path.exists():
        header = (
            "# 模拟盘交易记录\n\n"
            "| 时间 | 操作 | 股票 | 代码 | 数量 | 价格 | 金额 | 原因 |\n"
            "|------|------|------|------|------|------|------|------|\n"
        )
        log_path.write_text(header, encoding="utf-8")

    # 追加一行
    line = f"| {now} | {action} | {name} | {code} | {shares} | ¥{price:.2f} | ¥{amount:,.0f} | {reason} |\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)

    _logger.info(f"[shadow] 交易记录: {action} {name}({code}) {shares}股 @ ¥{price:.2f}")


def _get_positions(mx: MXMoni) -> list:
    """获取模拟盘当前持仓列表"""
    result = mx.positions()
    data = result.get("data", {})
    return data.get("posList", [])


def _get_balance(mx: MXMoni) -> dict:
    """获取模拟盘资金信息"""
    result = mx.balance()
    data = result.get("data", {})
    return {
        "total_assets": data.get("totalAssets", 0),
        "available": data.get("availBalance", 0),
        "position_value": data.get("totalPosValue", 0),
        "total_profit": data.get("totalProfit", 0),
        "init_money": data.get("initMoney", 200000),
    }


def _get_orders(mx: MXMoni) -> list:
    """获取模拟盘委托/成交记录"""
    result = mx.orders()
    data = result.get("data", {})
    return data.get("orderList", data.get("list", []))


def _calc_shares(price: float, amount: float = POSITION_SIZE) -> int:
    """根据目标金额和价格计算买入股数（100的整数倍）"""
    if price <= 0:
        return 0
    shares = int(amount / price)
    shares = (shares // 100) * 100
    return max(shares, 100)


# ---------------------------------------------------------------------------
# 买入：核心池新股票
# ---------------------------------------------------------------------------

def buy_new_picks(dry_run: bool = False) -> list:
    """
    对核心池中的股票执行模拟盘买入。
    已持有的跳过，只买新入池的。

    Returns:
        list of {"code": str, "name": str, "shares": int, "status": str}
    """
    mx = _get_moni()
    stocks_cfg = get_stocks()
    core_pool = stocks_cfg.get("core_pool", [])

    if not core_pool:
        _logger.info("[shadow] 核心池为空，无需买入")
        return []

    # 获取当前持仓代码
    positions = _get_positions(mx)
    held_codes = set()
    for pos in positions:
        code = str(pos.get("stockCode", pos.get("secuCode", ""))).strip()
        if code:
            held_codes.add(code)

    # 获取可用资金
    balance = _get_balance(mx)
    available = balance["available"]
    _logger.info(f"[shadow] 可用资金: ¥{available:,.0f}  持仓: {len(held_codes)} 只")
    strategy = get_strategy()
    buy_threshold = strategy.get("scoring", {}).get("thresholds", {}).get("buy", 7)

    results = []
    for item in core_pool:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()

        if not code or not name:
            continue

        try:
            score_result = score_stock(code, name)
        except Exception as e:
            _logger.warning(f"[shadow] {name}({code}) 评分失败，跳过: {e}")
            results.append({"code": code, "name": name, "shares": 0, "status": "评分失败"})
            continue

        total_score = float(score_result.get("total_score", 0) or 0)
        veto_signals = score_result.get("veto_signals", [])
        if veto_signals:
            status = f"veto:{','.join(veto_signals)}"
            _logger.info(f"[shadow] {name}({code}) 触发一票否决，跳过: {status}")
            results.append({"code": code, "name": name, "shares": 0, "status": status})
            continue
        if total_score < buy_threshold:
            _logger.info(f"[shadow] {name}({code}) 分数{total_score:.1f}<{buy_threshold}，跳过")
            results.append({"code": code, "name": name, "shares": 0, "status": f"分数不足:{total_score:.1f}"})
            continue

        if code in held_codes:
            _logger.info(f"[shadow] {name}({code}) 已持有，跳过")
            results.append({"code": code, "name": name, "shares": 0, "status": "已持有"})
            continue

        if available < POSITION_SIZE * 0.5:
            _logger.warning(f"[shadow] 可用资金不足 ¥{available:,.0f}，停止买入")
            results.append({"code": code, "name": name, "shares": 0, "status": "资金不足"})
            continue

        # 用 MX 查最新价
        try:
            from scripts.mx.mx_data import MXData
            mx_data = MXData()
            price_result = mx_data.query(f"{code}最新价")
            dto_list = price_result.get("data", {}).get("data", {}).get(
                "searchDataResultDTO", {}).get("dataTableDTOList", [])
            price = 0
            if dto_list:
                table = dto_list[0].get("table", {})
                keys = [k for k in table.keys() if k != "headName"]
                if keys:
                    vals = table[keys[0]]
                    if vals:
                        price = float(str(vals[0]).replace("元", "").replace(",", ""))
        except Exception:
            price = 0

        if price <= 0:
            _logger.warning(f"[shadow] {name}({code}) 无法获取价格，跳过")
            results.append({"code": code, "name": name, "shares": 0, "status": "无价格"})
            continue

        shares = _calc_shares(price)
        actual_cost = shares * price

        if dry_run:
            _logger.info(f"[shadow][DRY] 买入 {name}({code}) {shares}股 @ ¥{price:.2f} ≈ ¥{actual_cost:,.0f}")
            results.append({"code": code, "name": name, "shares": shares, "status": "dry_run", "price": price})
            continue

        # 执行市价买入
        _logger.info(f"[shadow] 买入 {name}({code}) {shares}股 @ ¥{price:.2f}")
        trade_result = mx.trade("buy", code, shares, use_market_price=True)
        trade_code = str(trade_result.get("code", ""))
        trade_msg = trade_result.get("message", "")

        if trade_code == "200":
            available -= actual_cost
            held_codes.add(code)
            _logger.info(f"[shadow] ✅ {name} 买入成功 {shares}股")
            _log_trade("买入", code, name, shares, price, f"核心池评分{total_score:.1f}")
            results.append({"code": code, "name": name, "shares": shares, "status": "成功", "price": price})
        else:
            _logger.warning(f"[shadow] ❌ {name} 买入失败: {trade_code} {trade_msg}")
            results.append({"code": code, "name": name, "shares": 0, "status": f"失败:{trade_msg}"})

    return results


# ---------------------------------------------------------------------------
# 止损止盈检查
# ---------------------------------------------------------------------------

def check_stop_signals(dry_run: bool = False) -> list:
    """
    检查模拟盘持仓是否触发止损/止盈信号，盘中执行卖出。

    调用时机：
      - morning.py 盘前（8:25）→ 只计算价格，不下单（盘前无法交易）
      - noon.py 午休（11:55）→ 盘中检查，触发则市价卖出
      - evening.py 收盘（15:35）→ 最后一次检查，触发则市价卖出

    Returns:
        list of {"code": str, "name": str, "action": str, "reason": str}
    """
    from datetime import time as dt_time

    mx = _get_moni()
    strategy = get_strategy()
    risk_cfg = strategy.get("risk", {})
    stop_loss_pct = risk_cfg.get("stop_loss", 0.04)
    absolute_stop_pct = risk_cfg.get("absolute_stop", 0.07)
    t1_pct = risk_cfg.get("take_profit", {}).get("t1_pct", 0.15)
    t1_drawdown = risk_cfg.get("take_profit", {}).get("t1_drawdown", 0.05)
    t2_drawdown = risk_cfg.get("take_profit", {}).get("t2_drawdown", 0.08)

    positions = _get_positions(mx)
    if not positions:
        _logger.info("[shadow] 模拟盘空仓，无需检查")
        return []

    # 判断是否在交易时间（可以下单）
    now = datetime.now()
    current_time = now.time()
    can_trade = (
        now.weekday() < 5 and (
            dt_time(9, 30) <= current_time <= dt_time(11, 30) or
            dt_time(13, 0) <= current_time <= dt_time(15, 0)
        )
    )

    results = []
    for pos in positions:
        code = str(pos.get("stockCode", pos.get("secuCode", ""))).strip()
        name = str(pos.get("stockName", pos.get("secuName", ""))).strip()
        shares = int(pos.get("totalQty", pos.get("currentQty", 0)))
        cost = float(pos.get("costPrice", pos.get("avgCost", 0)))
        price = float(pos.get("lastPrice", pos.get("currentPrice", 0)))
        avail_shares = int(pos.get("enableQty", pos.get("availQty", shares)))

        if shares <= 0 or cost <= 0:
            continue

        pnl_pct = (price / cost - 1)

        # 计算止损止盈价格
        stop_loss_price = round(cost * (1 - stop_loss_pct), 2)
        absolute_stop_price = round(cost * (1 - absolute_stop_pct), 2)
        t1_price = round(cost * (1 + t1_pct), 2)

        action = None
        reason = None
        sell_price = None

        # 绝对止损
        if pnl_pct <= -absolute_stop_pct:
            action = "清仓"
            reason = f"绝对止损 现价¥{price:.2f} < ¥{absolute_stop_price:.2f} ({pnl_pct*100:+.1f}%)"
            sell_price = absolute_stop_price
        # 动态止损
        elif pnl_pct <= -stop_loss_pct:
            action = "清仓"
            reason = f"动态止损 现价¥{price:.2f} < ¥{stop_loss_price:.2f} ({pnl_pct*100:+.1f}%)"
            sell_price = stop_loss_price
        # 第一批止盈
        elif pnl_pct >= t1_pct:
            sell_shares = (avail_shares // 4 // 100) * 100
            if sell_shares >= 100:
                action = f"卖出{sell_shares}股"
                reason = f"止盈第一批 现价¥{price:.2f} > ¥{t1_price:.2f} ({pnl_pct*100:+.1f}%)"
                sell_price = t1_price

        if not action:
            _logger.info(
                f"[shadow] {name}({code}) 现价¥{price:.2f} 成本¥{cost:.2f} "
                f"盈亏{pnl_pct*100:+.1f}% | 止损¥{stop_loss_price} 止盈¥{t1_price} → 持有"
            )
            results.append({
                "code": code, "name": name, "action": "持有",
                "reason": f"盈亏{pnl_pct*100:+.1f}% 止损¥{stop_loss_price} 止盈¥{t1_price}",
                "stop_loss": stop_loss_price, "take_profit": t1_price,
            })
            continue

        _logger.info(f"[shadow] {name}({code}) → {action} ({reason})")

        if dry_run or not can_trade:
            tag = "dry_run" if dry_run else "非交易时间"
            _logger.info(f"[shadow] [{tag}] 不下单，记录信号待下次盘中执行")
            results.append({
                "code": code, "name": name, "action": action, "reason": reason,
                "status": tag, "stop_loss": stop_loss_price, "take_profit": t1_price,
            })
            continue

        # 盘中执行：限价卖出（止损用止损价，止盈用止盈价）
        if action == "清仓":
            sell_qty = (avail_shares // 100) * 100
        else:
            sell_qty = int(action.replace("卖出", "").replace("股", ""))

        if sell_qty < 100:
            results.append({"code": code, "name": name, "action": action, "reason": reason, "status": "不足100股"})
            continue

        # 止损用市价确保成交，止盈用限价锁定利润
        if "止损" in reason:
            trade_result = mx.trade("sell", code, sell_qty, use_market_price=True)
        else:
            trade_result = mx.trade("sell", code, sell_qty, price=sell_price, use_market_price=False)

        trade_code = str(trade_result.get("code", ""))
        if trade_code == "200":
            _logger.info(f"[shadow] ✅ {name} {action} 成功")
            _log_trade("卖出", code, name, sell_qty, price, reason)
            results.append({"code": code, "name": name, "action": action, "reason": reason, "status": "成功"})
        else:
            msg = trade_result.get("message", "")
            _logger.warning(f"[shadow] ❌ {name} {action} 失败: {msg}")
            results.append({"code": code, "name": name, "action": action, "reason": reason, "status": f"失败:{msg}"})

    return results


# ---------------------------------------------------------------------------
# 状态查询
# ---------------------------------------------------------------------------

def get_status() -> dict:
    """获取模拟盘完整状态"""
    mx = _get_moni()
    balance = _get_balance(mx)
    positions = _get_positions(mx)

    pos_list = []
    for pos in positions:
        code = str(pos.get("stockCode", pos.get("secuCode", ""))).strip()
        name = str(pos.get("stockName", pos.get("secuName", ""))).strip()
        shares = int(pos.get("totalQty", pos.get("currentQty", 0)))
        cost = float(pos.get("costPrice", pos.get("avgCost", 0)))
        price = float(pos.get("lastPrice", pos.get("currentPrice", 0)))
        market_value = float(pos.get("marketValue", shares * price))
        pnl = float(pos.get("profit", pos.get("floatProfit", (price - cost) * shares)))
        pnl_pct = (price / cost - 1) * 100 if cost > 0 else 0

        pos_list.append({
            "code": code, "name": name, "shares": shares,
            "cost": cost, "price": price, "market_value": market_value,
            "pnl": pnl, "pnl_pct": pnl_pct,
        })

    return {
        "balance": balance,
        "positions": pos_list,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


# ---------------------------------------------------------------------------
# 报告生成
# ---------------------------------------------------------------------------

def generate_report() -> str:
    """生成模拟盘报告写入 Obsidian"""
    status = get_status()
    bal = status["balance"]
    positions = status["positions"]

    init = bal.get("init_money", 200000)
    total = bal.get("total_assets", 0)
    total_return = ((total / init) - 1) * 100 if init > 0 else 0

    lines = [
        f"# 模拟盘报告 — {status['timestamp']}",
        "",
        "## 账户概览",
        "",
        f"| 项目 | 数值 |",
        f"|------|------|",
        f"| 初始资金 | ¥{init:,.0f} |",
        f"| 总资产 | ¥{total:,.0f} |",
        f"| 可用资金 | ¥{bal.get('available', 0):,.0f} |",
        f"| 持仓市值 | ¥{bal.get('position_value', 0):,.0f} |",
        f"| 总收益 | ¥{bal.get('total_profit', 0):,.0f} ({total_return:+.2f}%) |",
        "",
    ]

    if positions:
        lines.append("## 当前持仓")
        lines.append("")
        lines.append("| 股票 | 代码 | 持仓 | 成本 | 现价 | 市值 | 盈亏 | 盈亏% |")
        lines.append("|------|------|------|------|------|------|------|-------|")
        for p in positions:
            lines.append(
                f"| {p['name']} | {p['code']} | {p['shares']}股 | "
                f"¥{p['cost']:.2f} | ¥{p['price']:.2f} | "
                f"¥{p['market_value']:,.0f} | ¥{p['pnl']:,.0f} | "
                f"{p['pnl_pct']:+.1f}% |"
            )
        lines.append("")
    else:
        lines.append("## 当前持仓：空仓")
        lines.append("")

    lines.append(f"> 本报告由影子交易引擎自动生成，用于验证交易系统逻辑")

    content = "\n".join(lines)

    # 写入 Obsidian
    vault_path = os.environ.get("AStockVault", _PROJECT_ROOT)
    report_dir = Path(vault_path) / "03-复盘" / "模拟盘"
    report_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    report_path = report_dir / f"模拟盘_{date_str}.md"
    report_path.write_text(content, encoding="utf-8")

    _logger.info(f"[shadow] 报告已写入: {report_path}")
    return str(report_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="影子交易引擎（模拟盘验证）")
    parser.add_argument("action", choices=["buy", "check", "status", "report"],
                        help="buy=买入核心池 check=检查止损止盈 status=查看状态 report=生成报告")
    parser.add_argument("--dry-run", action="store_true", help="模拟运行，不实际下单")
    args = parser.parse_args()

    if args.action == "buy":
        results = buy_new_picks(dry_run=args.dry_run)
        print(f"\n影子交易买入: {len(results)} 只")
        for r in results:
            print(f"  {r['name']}({r['code']}): {r['status']}"
                  + (f" {r['shares']}股" if r.get('shares') else ""))

    elif args.action == "check":
        results = check_stop_signals(dry_run=args.dry_run)
        print(f"\n止损止盈检查: {len(results)} 只")
        for r in results:
            print(f"  {r['name']}({r['code']}): {r['action']} — {r['reason']}")

    elif args.action == "status":
        status = get_status()
        bal = status["balance"]
        print(f"\n模拟盘状态 ({status['timestamp']})")
        print(f"  总资产: ¥{bal['total_assets']:,.0f}  可用: ¥{bal['available']:,.0f}")
        print(f"  持仓市值: ¥{bal['position_value']:,.0f}  总收益: ¥{bal['total_profit']:,.0f}")
        if status["positions"]:
            print(f"\n  持仓 ({len(status['positions'])} 只):")
            for p in status["positions"]:
                print(f"    {p['name']}({p['code']}) {p['shares']}股 "
                      f"成本¥{p['cost']:.2f} 现价¥{p['price']:.2f} "
                      f"盈亏{p['pnl_pct']:+.1f}%")
        else:
            print("  持仓: 空仓")

    elif args.action == "report":
        path = generate_report()
        print(f"\n报告已生成: {path}")


if __name__ == "__main__":
    main()
