#!/usr/bin/env python3
"""
A股量化选股 Pipeline
结合 mx-finance-data / mx-stocks-screener / mx-finance-search / akshare 主力资金
四维评分模型输出股票评分排名

用法:
  python3 stock_screener_pipeline.py --criteria "ROE大于8% 且 营收增长" --market A股 --top 20
  python3 stock_screener_pipeline.py --criteria "股价站上20日均线 且 60日均线向上" --top 10
"""

import os
import sys
import json
import warnings
import argparse
from datetime import datetime

warnings.filterwarnings('ignore')

# 路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

EM_API_KEY = os.environ.get("EM_API_KEY", "")
OUTPUT_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "data", "04-选股", "筛选结果")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────

def run_cmd(cmd: str, timeout=60) -> str:
    """执行命令，返回 stdout"""
    import subprocess
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, timeout=timeout,
        env={**os.environ, "EM_API_KEY": EM_API_KEY}
    )
    return result.stdout + result.stderr


def call_mx_screener(query: str, select_type: str = "A股") -> list:
    """调用 mx-stocks-screener 选股"""
    script = os.path.join(SCRIPT_DIR, "..", "..", "..", ".openclaw", "workspace", "skills", "mx-stocks-screener", "scripts", "get_data.py")
    if not os.path.exists(script):
        # 尝试本地安装路径
        script = os.path.expanduser("~/.openclaw/workspace/skills/mx-stocks-screener/scripts/get_data.py")
    
    cmd = f'{sys.executable} "{script}" --query "{query}" --select-type {select_type}'
    output = run_cmd(cmd, timeout=90)
    
    # 解析 CSV 路径
    csv_path = None
    for line in output.split("\n"):
        if "CSV:" in line and ".csv" in line:
            csv_path = line.split("CSV:")[-1].strip()
            break
    
    if not csv_path or not os.path.exists(csv_path):
        return []
    
    # 解析 CSV
    import csv
    stocks = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                code = row.get("代码", "").strip()
                name = row.get("名称", "").strip()
                if not code:
                    continue
                stocks.append({
                    "code": code,
                    "name": name,
                    "raw": row
                })
            except Exception:
                continue
    return stocks


def call_mx_finance_data(query: str) -> dict:
    """调用 mx-finance-data 查询财务数据"""
    script = os.path.join(SCRIPT_DIR, "..", "..", "..", ".openclaw", "workspace", "skills", "mx-finance-data", "scripts", "get_data.py")
    if not os.path.exists(script):
        script = os.path.expanduser("~/.openclaw/workspace/skills/mx-finance-data/scripts/get_data.py")
    
    cmd = f'{sys.executable} "{script}" --query "{query}"'
    output = run_cmd(cmd, timeout=90)
    
    # 找 xlsx 路径
    xlsx_path = None
    desc_path = None
    for line in output.split("\n"):
        if "文件:" in line:
            xlsx_path = line.split("文件:")[-1].strip()
        if "描述:" in line:
            desc_path = line.split("描述:")[-1].strip()
    
    return {"xlsx": xlsx_path, "desc": desc_path, "raw": output}


def call_mx_search(query: str) -> str:
    """调用 mx-finance-search 搜索资讯/催化"""
    script = os.path.join(SCRIPT_DIR, "..", "..", "..", ".openclaw", "workspace", "skills", "mx-finance-search", "scripts", "get_data.py")
    if not os.path.exists(script):
        script = os.path.expanduser("~/.openclaw/workspace/skills/mx-finance-search/scripts/get_data.py")
    
    cmd = f'{sys.executable} "{script}" {query}'
    output = run_cmd(cmd, timeout=90)
    return output


def get_main_flow_akshare(code: str) -> dict:
    """用 akshare 获取主力资金流向"""
    try:
        from akshare_data import get_fund_flow
        flow = get_fund_flow(code, 5)
        return flow or {}
    except Exception:
        return {}


def score_stock(stock: dict, search_result: str, main_flow: dict) -> dict:
    """
    四维评分：
    - 行业逻辑 0-3分
    - 基本面 0-3分
    - 技术面 0-2分
    - 主力资金 0-2分
    """
    raw = stock.get("raw", {})
    score = {"total": 0, "行业逻辑": 0, "基本面": 0, "技术面": 0, "主力资金": 0, "notes": []}
    
    # ── 技术面（0-2分）────────────────────
    tech_score = 0
    price_col = [c for c in raw.keys() if "最新价" in c and "2026.04.08" in c]
    ma20_col = [c for c in raw.keys() if "20日均线" in c and "2026.04.08" in c]
    
    if price_col and ma20_col:
        try:
            price = float(str(raw[price_col[0]]).replace(",", ""))
            ma20 = float(str(raw[ma20_col[0]]).replace(",", ""))
            if price > ma20:
                tech_score += 1  # 站上MA20
        except Exception:
            pass
        
        # 60日均线方向
        ma60_up_cols = [c for c in raw.keys() if "60日均线上移" in c]
        if ma60_up_cols and "符合" in str(raw[ma60_up_cols[0]]):
            tech_score += 1  # 60日均线向上
    score["技术面"] = min(tech_score, 2)
    score["total"] += tech_score
    
    # ── 基本面（0-3分）────────────────────
    fund_score = 0
    roe_col = [c for c in raw.keys() if "ROE" in c or "净资产收益率" in c]
    rev_col = [c for c in raw.keys() if "营业收入" in c and "同比" in c]
    
    if roe_col:
        try:
            roe_val = float(str(raw[roe_col[0]]).split("|")[0].replace("%", ""))
            if roe_val >= 8:
                fund_score += 1
            if roe_val >= 15:
                fund_score += 1
            score["notes"].append(f"ROE: {roe_val:.2f}%")
        except Exception:
            pass
    
    if rev_col:
        try:
            rev_val = float(str(raw[rev_col[0]]).split("|")[0].replace("%", ""))
            if rev_val > 0:
                fund_score += 1
            if rev_val >= 15:
                fund_score += 1
            score["notes"].append(f"营收增长: {rev_val:.2f}%")
        except Exception:
            pass
    
    score["基本面"] = min(fund_score, 3)
    score["total"] += fund_score
    
    # ── 主力资金（0-2分）──────────────────
    flow_score = 0
    flows = main_flow.get("recent_flows", [])
    if flows:
        today_flow = flows[-1].get("main_net_flow", 0) if len(flows) > 0 else 0
        try:
            today_flow = float(today_flow)
            if today_flow > 0:
                flow_score += 1  # 今日主力净流入
            # 近5日总体
            total_5d = sum(float(f.get("main_net_flow", 0)) for f in flows)
            if total_5d > 0:
                flow_score += 1  # 5日净流入
            score["notes"].append(f"主力流入: {today_flow/1e8:.2f}亿")
        except Exception:
            pass
    score["主力资金"] = min(flow_score, 2)
    score["total"] += flow_score
    
    # ── 行业逻辑（0-3分）──────────────────
    logic_score = 0
    if search_result:
        result_lower = search_result.lower()
        # 政策催化关键词
        catalyst_kw = ["政策", "催化", "AI", "算力", "国产替代", "数据要素", "机器人", "半导体"]
        count = sum(1 for kw in catalyst_kw if kw in result_lower)
        if count >= 3:
            logic_score = 3
        elif count >= 1:
            logic_score = 2
        else:
            logic_score = 1
    score["行业逻辑"] = logic_score
    score["total"] += logic_score
    
    # ── 通过标记 ──────────────────────────
    score["passed"] = score["total"] >= 7 and score["行业逻辑"] > 0 and score["基本面"] >= 2
    
    return score


def format_row(stock: dict, score: dict) -> str:
    """格式化一行输出"""
    name = stock["name"]
    code = stock["code"]
    raw = stock.get("raw", {})
    
    # 涨跌幅
    change_col = [c for c in raw.keys() if "涨跌幅" in c and "2026.04.08" in c]
    change = raw.get(change_col[0], "N/A") if change_col else "N/A"
    
    # 价格
    price_col = [c for c in raw.keys() if "最新价" in c and "2026.04.08" in c]
    price = raw.get(price_col[0], "N/A") if price_col else "N/A"
    
    passed_mark = "✅" if score.get("passed") else "❌"
    return {
        "rank": 0,
        "name": name,
        "code": code,
        "price": price,
        "change": change,
        "total": score["total"],
        "industry_logic": score["行业逻辑"],
        "fundamental": score["基本面"],
        "technical": score["技术面"],
        "main_flow": score["主力资金"],
        "notes": " | ".join(score.get("notes", [])),
        "passed": score.get("passed", False)
    }


def print_leaderboard(results: list, top_n: int = 20):
    """打印排行榜"""
    # 按总分排序
    results.sort(key=lambda x: x["total"], reverse=True)
    
    header = f"{'#':<3} {'股票':<10} {'代码':<8} {'今涨':>7} {'总分':>4} {'行业':>3} {'基本面':>4} {'技术':>3} {'主力':>3} {'说明'}"
    separator = "-" * len(header)
    
    print(f"\n{'='*80}")
    print(f" A股量化选股 Pipeline — 四维评分结果  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
    print(f"{'='*80}")
    print(f" 筛选条件: {args.criteria}")
    print(f" 符合条件总数: {len(results)}")
    print(f" 通过标准(≥7分): {sum(1 for r in results if r['passed'])}")
    print()
    print(header)
    print(separator)
    
    for i, r in enumerate(results[:top_n], 1):
        rank_icon = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
        passed_icon = "✅" if r['passed'] else "  "
        print(
            f"{rank_icon}{i:<2} {r['name']:<10} {r['code']:<8} "
            f"{r['change']:>7} {passed_icon}{r['total']:>3}  "
            f"{r['industry_logic']:>3}   {r['fundamental']:>4}    {r['technical']:>3}   {r['main_flow']:>3}   {r['notes'][:40]}"
        )
    
    print(separator)
    
    # 汇总通过名单
    passed = [r for r in results if r['passed']]
    if passed:
        print(f"\n🎯 通过名单（{len(passed)}只）：")
        names = "、".join([f"{r['name']}({r['code']})" for r in passed])
        print(f"  {names}")
    
    return results


# ─────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A股四维量化选股 Pipeline")
    parser.add_argument("--criteria", "-c", type=str, 
                        default="股价站上20日均线 且 60日均线向上或走平 且 ROE大于8% 且 营收增长",
                        help="选股条件（自然语言）")
    parser.add_argument("--market", "-m", type=str, default="A股",
                        help="市场类型：A股/港股/美股/基金/ETF/可转债/板块")
    parser.add_argument("--top", "-t", type=int, default=30,
                        help="输出前N只")
    parser.add_argument("--output", "-o", type=str, default=None,
                        help="结果保存路径（默认自动命名）")
    args = parser.parse_args()
    
    if not EM_API_KEY:
        print("❌ EM_API_KEY 未设置，请先设置环境变量")
        sys.exit(1)
    
    print(f"🔍 条件: {args.criteria}")
    print(f"📡 市场: {args.market}")
    print()
    
    # Step 1: mx-stocks-screener 筛选候选股票
    print("📊 Step 1: 调用 mx-stocks-screener 筛选候选股票...")
    candidates = call_mx_screener(args.criteria, args.market)
    print(f"   候选股票: {len(candidates)} 只")
    
    if not candidates:
        print("❌ 未筛选到任何股票，请检查条件是否合理")
        sys.exit(1)
    
    scored = []
    
    # Step 2: 批量评分（每5只一组，避免请求过快）
    print(f"\n📈 Step 2: 四维评分中（共 {len(candidates)} 只）...")
    
    for i, stock in enumerate(candidates, 1):
        name = stock["name"]
        code = stock["code"]
        
        print(f"   [{i}/{len(candidates)}] 评分 {name}({code})...", end=" ", flush=True)
        
        # 主力资金
        main_flow = get_main_flow_akshare(code)
        
        # 搜索催化
        search_result = call_mx_search(f"{name} 政策 利好 催化剂")
        
        # 评分
        score = score_stock(stock, search_result, main_flow)
        row = format_row(stock, score)
        row["rank"] = i
        scored.append(row)
        
        print(f"→ 总分 {score['total']} {'✅' if score.get('passed') else ''}")
    
    # Step 3: 排序输出
    print(f"\n🎯 Step 3: 排序输出...")
    results = print_leaderboard(scored, top_n=args.top)
    
    # 保存结果
    if args.output:
        save_path = args.output
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_path = os.path.join(OUTPUT_DIR, f"筛选结果_{ts}.md")
    
    # 生成 Markdown 报告
    md_lines = [
        f"# A股量化选股结果 — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"",
        f"**筛选条件**: {args.criteria}",
        f"**候选总数**: {len(candidates)}",
        f"**通过标准(≥7分)**: {sum(1 for r in results if r['passed'])}",
        f"",
        f"| # | 股票 | 代码 | 今涨 | 总分 | 行业 | 基本面 | 技术面 | 主力 | 说明 |",
        f"|---|------|------|------|------|------|--------|------|------|------|",
    ]
    
    results.sort(key=lambda x: x["total"], reverse=True)
    for i, r in enumerate(results[:args.top], 1):
        md_lines.append(
            f"| {i} | {r['name']} | {r['code']} | {r['change']} | "
            f"{'✅' if r['passed'] else ''}{r['total']} | {r['industry_logic']} | "
            f"{r['fundamental']} | {r['technical']} | {r['main_flow']} | {r['notes'][:50]} |"
        )
    
    md_content = "\n".join(md_lines)
    with open(save_path, "w", encoding="utf-8") as f:
        f.write(md_content)
    
    print(f"\n💾 结果已保存: {save_path}")
