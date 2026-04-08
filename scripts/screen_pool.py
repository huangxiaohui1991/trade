#!/usr/bin/env python3
"""
screen_pool.py — 核心池/观察池 四维量化评分
每日自动调用，更新 Obsidian 核心池

用法:
  python screen_pool.py                  # 跑核心池+观察池全部标的
  python screen_pool.py --pool core      # 只跑核心池
  python screen_pool.py --pool observe   # 只跑观察池
  python screen_pool.py --dry-run        # 不写文件，只打印结果
"""

import os
import sys
import json
import csv
import warnings
import argparse
from datetime import datetime
from pathlib import Path

# 路径
SCRIPT_DIR = Path(os.path.abspath(__file__)).parent
DATA_DIR = SCRIPT_DIR.parent / "data"
OUTPUT_DIR = DATA_DIR / "04-选股"
CORE_POOL_PATH = OUTPUT_DIR / "核心池.md"
OBSERVE_POOL_PATH = OUTPUT_DIR / "观察池.md"
SKILL_MX_SCREENER = Path.home() / ".openclaw/workspace/skills/mx-stocks-screener/scripts/get_data.py"
SKILL_MX_SEARCH = Path.home() / ".openclaw/workspace/skills/mx-finance-search/scripts/get_data.py"
OUTPUT_RESULTS_DIR = OUTPUT_DIR / "筛选结果"

EM_API_KEY = os.environ.get("EM_API_KEY", "")

warnings.filterwarnings("ignore")


# ─────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────

def run_py(script_path: str, args: list, timeout=90, env_vars=None) -> str:
    import subprocess
    env = {**os.environ, "EM_API_KEY": EM_API_KEY}
    if env_vars:
        env.update(env_vars)
    result = subprocess.run(
        [sys.executable, script_path] + args,
        capture_output=True, text=True, timeout=timeout, env=env
    )
    return result.stdout + result.stderr


def load_pool(md_path: Path, pool_label: str) -> list:
    """解析 markdown 文件，提取股票列表"""
    if not md_path.exists():
        return []
    
    stocks = []
    in_table = False
    with open(md_path, encoding="utf-8") as f:
        content = f.read()
    
    for line in content.split("\n"):
        line = line.strip()
        if "## 当前" in line and pool_label[:2] in line:
            in_table = True
            continue  # 跳过标题，行留在缓冲区，下一行自然进入解析
        if in_table and line.startswith("##") and not line.strip().startswith("|"):
            break  # 遇到下一个 ## 章节（不是表格行）就停止
        if in_table and line.startswith("|"):
            if "股票" in line and "代码" in line:
                continue
            if "--" in line or "---" in line:
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 4:
                name = parts[2].strip()
                code = parts[3].strip()
                if name and name not in ["股票", ""] and code.isdigit():
                    stocks.append({"name": name, "code": code})
    return stocks


def akshare_data_batch(codes: list) -> dict:
    """用 akshare 批量获取技术面+主力资金+基本面数据"""
    result = {}
    sys.path.insert(0, str(SCRIPT_DIR))
    from akshare_data import get_technical, get_realtime, get_fund_flow, get_fundamental
    for code in codes:
        try:
            tech = get_technical(code, 60)
            rt = get_realtime(code)
            mf = get_fund_flow(code, 5) or {}
            fund = get_fundamental(code) or {}
            result[code] = {"tech": tech, "realtime": rt, "main_flow": mf, "fundamental": fund}
        except Exception as e:
            result[code] = {"error": str(e)}
    return result


def akshare_main_flow(code: str) -> dict:
    """用 akshare 获取主力资金"""
    try:
        sys.path.insert(0, str(SCRIPT_DIR))
        from akshare_data import get_fund_flow
        return get_fund_flow(code, 5) or {}
    except Exception:
        return {}


def mx_search_catalyst(name: str, code: str) -> dict:
    """用 mx-finance-search 搜索催化剂，返回是否通过"""
    try:
        output = run_py(str(SKILL_MX_SEARCH), [f"{name} {code} 政策 催化 利好"], timeout=60)
        
        # 简单判断：结果中有无催化剂关键词
        catalyst_kw = ["政策", "催化", "AI", "算力", "国产替代", "数据要素", "机器人",
                       "半导体", "订单", "业绩", "扩产", "景气", "需求", "景气", "特斯拉"]
        count = sum(1 for kw in catalyst_kw if kw in output)
        
        return {
            "has_catalyst": count >= 2,
            "match_count": count,
            "output_snippet": output[:500]
        }
    except Exception:
        return {"has_catalyst": False, "match_count": 0}


def apply_fine_filters(name: str, code: str, ak_all: dict) -> dict:
    """
    三道精筛门槛（任一项不通过 → 直接淘汰，不进入评分）
    
    1. 量价确认：今日涨幅 > 3% 且 成交量 > 5日均量 1.2倍
    2. 箱体位置：当前价在近20日高低点区间的 40%~90% 区间
    3. 现金流过滤：经营现金流为负 → 直接淘汰
    
    返回：{"pass": bool, "reasons": [str]}
    """
    d = ak_all.get(code, {})
    tech = d.get("tech", {})
    rt = d.get("realtime", {})
    fund = d.get("fundamental", {})
    reasons = []
    passed = True
    
    # ── 精筛1：量价确认 ───────────────
    # 核心池股票：只要涨幅不为负（已处于上涨趋势），即通过量价确认
    # 初筛候选：才需要今日涨幅>3%确认突破强度
    try:
        change_pct = float(rt.get("change_pct", 0)) if rt.get("change_pct") is not None else float(tech.get("change_pct", 0))
        vol_analysis = tech.get("volume_analysis", {})
        vol_today = float(vol_analysis.get("today", 0))
        vol_ma5 = float(vol_analysis.get("MA5", 0))
        
        if change_pct < 0:
            reasons.append(f"今日下跌{change_pct:.1f}%")
            # 缓跌（>-5%）只降分；暴跌才淘汰
            if change_pct < -5:
                passed = False
                reasons[-1] = reasons[-1] + " ←暴跌淘汰"
        elif vol_ma5 and vol_today < vol_ma5 * 0.8:
            passed = False
            reasons.append(f"缩量异常(今{vol_today/1e6:.1f}万<均{vol_ma5/1e6:.1f}万)")
        else:
            reasons.append(f"量价正常(涨{change_pct:.1f}%,量{vol_today/vol_ma5:.1f}倍)")
    except Exception as e:
        reasons.append(f"量价数据缺失")
    
    # ── 精筛2：箱体位置 ───────────────
    try:
        price = float(rt.get("price", 0)) if rt.get("price") else float(tech.get("current_price", 0))
        # 优先20日高低点；否则用5日高低点（akshare_batch 特有）
        high_val = tech.get("high_20") or tech.get("high_5d")
        low_val = tech.get("low_20") or tech.get("low_5d")
        
        if price and high_val and low_val and high_val != low_val:
            range_pct = (price - low_val) / (high_val - low_val) * 100
            reasons.append(f"箱体:{range_pct:.0f}%")
            if range_pct < 40:
                reasons[-1] = reasons[-1] + "(低位优选)"
            elif range_pct > 90:
                reasons[-1] = reasons[-1] + "(高位+警示)"
                # 核心池：高位降分不淘汰（已持仓可持有）；初筛：直接淘汰
                passed = False  # 统一：高位直接淘汰，保持一致性
        else:
            reasons.append("箱体:数据缺失")
    except Exception as e:
        reasons.append(f"箱体:异常")
    
    # ── 精筛3：现金流 ───────────────
    try:
        cfp = fund.get("cash_flow_positive")
        if cfp is False:
            passed = False
            reasons.append("现金流为负 ←淘汰")
        elif cfp is True:
            reasons.append("现金流正")
        else:
            reasons.append("现金流:暂无数据")  # None 不淘汰
    except Exception:
        reasons.append("现金流:数据缺失")
    
    return {"pass": passed, "reasons": reasons}


def score_stock(name: str, code: str, ak_all: dict, catalyst_result: dict) -> dict:
    """
    四维评分（满分10分）
    行业逻辑 0-3 | 基本面 0-3 | 技术面 0-2 | 主力资金 0-2
    数据源：akshare（技术面+主力+基本面）+ mx-finance-search（催化）
    """
    score = {"total": 0, "行业逻辑": 0, "基本面": 0, "技术面": 0, "主力资金": 0, "notes": [], "eliminated": False, "eliminate_reason": ""}
    
    d = ak_all.get(code, {})
    tech = d.get("tech", {})
    rt = d.get("realtime", {})
    mf = d.get("main_flow", {})
    fund = d.get("fundamental", {})
    
    # ── 精筛1：量价确认 ───────────────
    # realtime 收盘后为空，从 technical 获取
    change_pct = 0.0
    try:
        rt_chg = rt.get("change_pct") if rt else None
        change_pct = float(rt_chg) if rt_chg is not None else float(tech.get("change_pct", 0))
    except Exception:
        change_pct = float(tech.get("change_pct", 0))
    
    # ── 先过三道精筛门槛 ───────────────
    fine = apply_fine_filters(name, code, ak_all)
    score["fine_filters"] = fine
    if not fine["pass"]:
        score["eliminated"] = True
        score["eliminate_reason"] = "; ".join(fine["reasons"])
        score["notes"] = fine["reasons"]
        return score
    
    score["notes"] = fine["reasons"]
    
    # ── 技术面（0-2分）────────────────────
    tech_score = 0
    try:
        ma20 = tech.get("ma20")
        ma60_dir = tech.get("ma60_direction", "")
        price = rt.get("price", tech.get("current_price", 0))
        if price and ma20 and float(price) > float(ma20):
            tech_score += 1
            score["notes"].append("站上MA20")
        if ma60_dir in ["向上", "走平"]:
            tech_score += 1
            score["notes"].append(f"MA60{ma60_dir}")
    except Exception:
        pass
    score["技术面"] = min(tech_score, 2)
    score["total"] += tech_score
    
    # ── 基本面（0-3分）────────────────────
    fund_score = 0
    try:
        roe_list = fund.get("roe_recent", [])
        rev_list = fund.get("revenue_growth", [])
        
        # ROE
        if roe_list:
            try:
                roe_vals = []
                for v in roe_list:
                    try:
                        roe_vals.append(float(str(v).replace("%", "")) if isinstance(v, str) else float(v))
                    except Exception:
                        pass
                roe_latest = roe_vals[-1] if len(roe_vals) >= 1 else 0
                roe_annualized = roe_latest * 4
                roe_val = max(roe_latest, roe_annualized)
                if roe_val >= 8: fund_score += 1
                if roe_val >= 15: fund_score += 1
                score["notes"].append(f"ROE:{roe_val:.1f}%")
            except Exception:
                pass
        
        # 营收增长
        if rev_list:
            try:
                rev_val = float(str(rev_list[0]).replace("%", "")) if isinstance(rev_list[0], str) else float(rev_list[0])
                if rev_val > 0: fund_score += 1
                if rev_val >= 15: fund_score += 1
                score["notes"].append(f"营收增长:{rev_val:.1f}%")
            except Exception:
                pass
        
        # 现金流（已通过精筛3；计分时明确为负才减分）
        cfp = fund.get("cash_flow_positive")
        if cfp is True:
            fund_score += 1
            score["notes"].append("现金流正")
        elif cfp is False:
            fund_score -= 0.5
            score["notes"].append("现金流负")
        # cfp is None → 不加分不扣分
    except Exception:
        pass
    
    score["基本面"] = min(fund_score, 3)
    score["total"] += fund_score
    
    # ── 主力资金（0-2分）──────────────────
    flow_score = 0
    try:
        flows = mf.get("recent_flows", [])
        if flows:
            today = float(flows[-1].get("main_net_flow", 0))
            total_5d = sum(float(f.get("main_net_flow", 0)) for f in flows)
            if today > 0: flow_score += 1
            if total_5d > 0: flow_score += 1
            score["notes"].append(f"主力:{today/1e8:.2f}亿")
        else:
            score["notes"].append("主力:—")
    except Exception:
        pass
    
    score["主力资金"] = min(flow_score, 2)
    score["total"] += flow_score
    
    # ── 行业逻辑（0-3分）──────────────────
    logic_score = 0
    if catalyst_result.get("has_catalyst"):
        mc = catalyst_result.get("match_count", 0)
        if mc >= 5: logic_score = 3
        elif mc >= 3: logic_score = 2
        else: logic_score = 1
    else:
        logic_score = 1
    
    score["行业逻辑"] = logic_score
    score["total"] += logic_score
    
    score["passed"] = score["total"] >= 7 and score["行业逻辑"] > 0 and score["基本面"] >= 2
    
    return score


def format_obsidian_pool(stocks: list, scores: dict, pool_label: str) -> str:
    """生成 Obsidian 核心池/观察池 markdown 内容"""
    today = datetime.now().strftime("%Y-%m-%d")
    
    header_lines = [
        "---",
        f"date: {today}",
        f"type: watchlist_{'core' if '核心' in pool_label else 'observe'}",
        "tags: [" + ("核心池, 选股" if "核心" in pool_label else "观察池, 选股") + "]",
        f"updated_at: {today}",
        "---",
        "",
        f"# {'核心' if '核心' in pool_label else '观察'}池（{'四维评分' if scores else '手动维护'}）",
        "",
    ]
    
    if not stocks:
        header_lines.append("*池为空，请通过选股 Pipeline 补充。*")
        return "\n".join(header_lines)
    
    # 表格头
    table_lines = [
        "| # | 股票 | 代码 | 今日涨跌 | 四维总分 | 行业 | 基本面 | 技术 | 主力 | 通过 | 备注 |",
        "|---|------|------|---------|---------|------|--------|------|------|------|------|"
    ]
    
    for i, stk in enumerate(stocks, 1):
        name = stk["name"]
        code = stk["code"]
        s = scores.get(code, {})
        
        if s:
            total = s.get("total", 0)
            il = s.get("行业逻辑", 0)
            fu = s.get("基本面", 0)
            tc = s.get("技术面", 0)
            mf = s.get("主力资金", 0)
            passed = "✅" if s.get("passed") else "❌"
            notes = " | ".join(s.get("notes", [])[:3])
        else:
            total = il = fu = tc = mf = 0
            passed = "—"
            notes = "未评分"
        
        table_lines.append(
            f"| {i} | {name} | {code} | — | "
            f"{'✅' if passed == '✅' else ''}{total} | {il} | {fu} | {tc} | {mf} | {passed} | {notes} |"
        )
    
    # 历史记录 section（保留原有）
    history_lines = [
        "",
        "## 历史记录",
        "",
        "| 周次 | 入池 | 出池 | 原因 |",
        "|------|------|------|------|",
    ]
    
    return "\n".join(header_lines + table_lines + history_lines)


def format_score_report(stocks: list, scores: dict, pool_label: str) -> str:
    """生成详细评分报告（Markdown）"""
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"# {'核心池' if '核心' in pool_label else '观察池'}四维评分报告 — {today}",
        "",
        f"| # | 股票 | 代码 | 总分 | 行业 | 基本面 | 技术 | 主力 | 通过 | 备注 |",
        f"|---|------|------|------|------|--------|------|------|------|------|",
    ]
    
    sorted_stocks = sorted(stocks, key=lambda s: scores.get(s["code"], {}).get("total", 0), reverse=True)
    
    for i, stk in enumerate(sorted_stocks, 1):
        name = stk["name"]
        code = stk["code"]
        s = scores.get(code, {})
        
        if s:
            total = s.get("total", 0)
            il = s.get("行业逻辑", 0)
            fu = s.get("基本面", 0)
            tc = s.get("技术面", 0)
            mf = s.get("主力资金", 0)
            passed = "✅" if s.get("passed") else "❌"
            notes = " | ".join(s.get("notes", [])[:3])
        else:
            total = il = fu = tc = mf = 0
            passed = "—"
            notes = "—"
        
        lines.append(
            f"| {i} | {name}({code}) | {total} | {il} | {fu} | {tc} | {mf} | {passed} | {notes} |"
        )
    
    passed_list = [s for s in sorted_stocks if scores.get(s["code"], {}).get("passed")]
    if passed_list:
        lines.append("")
        lines.append(f"**🎯 通过名单（{len(passed_list)}只）**：")
        lines.append("、".join([f"{s['name']}({s['code']})" for s in passed_list]))
    
    return "\n".join(lines)


# ─────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="核心池/观察池四维评分")
    parser.add_argument("--pool", choices=["core", "observe", "all"], default="all",
                        help="跑哪个池（core=核心池，observe=观察池，all=全部）")
    parser.add_argument("--dry-run", action="store_true", help="不写文件，只打印")
    parser.add_argument("--criteria", type=str,
                        default="股价站上20日均线 且 60日均线向上或走平 且 ROE大于8% 且 营收增长",
                        help="选股条件")
    args = parser.parse_args()
    
    if not EM_API_KEY:
        print("❌ EM_API_KEY 未设置")
        sys.exit(1)
    
    os.makedirs(OUTPUT_RESULTS_DIR, exist_ok=True)
    
    pools = []
    if args.pool in ["core", "all"]:
        pools.append(("核心池", CORE_POOL_PATH))
    if args.pool in ["observe", "all"]:
        pools.append(("观察池", OBSERVE_POOL_PATH))
    
    all_scores = {}
    all_stocks = {}
    today_str = datetime.now().strftime("%Y%m%d_%H%M")
    
    for pool_name, pool_path in pools:
        print(f"\n{'='*60}")
        print(f"📊 评分中：{pool_name}")
        print(f"{'='*60}")
        
        stocks = load_pool(pool_path, pool_name)
        print(f"   候选股票: {len(stocks)} 只")
        
        if not stocks:
            print(f"   {pool_name}为空，跳过")
            continue
        
        # 提取代码列表
        codes = [s["code"] for s in stocks]
        
        # Step 1: akshare 批量获取技术面+主力资金+基本面
        print(f"   📡 获取全部数据（技术面+主力+基本面）...")
        ak_all = akshare_data_batch(codes)
        print(f"   数据获取: {len(ak_all)}/{len(codes)} 只")
        
        pool_scores = {}
        
        for i, stk in enumerate(stocks, 1):
            name = stk["name"]
            code = stk["code"]
            
            print(f"   [{i}/{len(stocks)}] {name}({code})...", end=" ", flush=True)
            
            # Step 2: mx-finance-search 催化剂
            catalyst = mx_search_catalyst(name, code)
            
            # Step 3: 四维评分（含三道精筛门槛）
            s = score_stock(name, code, ak_all, catalyst)
            pool_scores[code] = s
            
            if s.get("eliminated"):
                print(f"❌ 淘汰 | {' | '.join(s.get('eliminate_reason', [])[:2])}")
            else:
                tag = "✅" if s.get("passed") else "  "
                print(f"→ {tag}总分 {s['total']} | {' '.join(s.get('notes',[])[:3])}")
            
            tag = "✅" if s.get("passed") else "  "
            print(f"→ {tag}总分 {s['total']} | {' '.join(s.get('notes',[])[:3])}")
        
        all_scores.update(pool_scores)
        all_stocks[pool_name] = stocks
        
        # 生成 Obsidian 池文件
        pool_md = format_obsidian_pool(stocks, pool_scores, pool_name)
        obsidian_path = pool_path
        if args.dry_run:
            print(f"\n[DRY-RUN] 不写入文件")
            print(f"   {obsidian_path}:")
            print(pool_md[:500])
        elif len(stocks) == 0:
            print(f"\n   ⚠️ 空候选，不覆盖文件")
        elif len(pool_scores) == 0:
            print(f"\n   ⚠️ 评分为空，不写入文件")
        else:
            with open(obsidian_path, "w", encoding="utf-8") as f:
                f.write(pool_md)
            print(f"\n   💾 已更新: {obsidian_path}")
        
        # 生成详细报告
        report = format_score_report(stocks, pool_scores, pool_name)
        report_path = OUTPUT_RESULTS_DIR / f"{pool_name}_评分报告_{today_str}.md"
        if not args.dry_run:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(report)
            print(f"   💾 报告已保存: {report_path}")
        
        # 打印汇总
        passed = [s for s in stocks if pool_scores.get(s["code"], {}).get("passed")]
        print(f"\n   🎯 通过（≥7分）: {len(passed)}/{len(stocks)} 只")
        if passed:
            names = "、".join([f"{s['name']}({s['code']})" for s in passed])
            print(f"   {names}")
    
    print(f"\n{'='*60}")
    print("✅ 评分完成")
    print(f"{'='*60}")
