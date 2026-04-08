#!/usr/bin/env python3
"""生成清晰的 Markdown 格式交易报告"""
import os, sys, warnings
os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")
sys.path.insert(0, "/Users/hxh/Documents/a-stock-trading/scripts")

from akshare_data import get_market_status
from parser import parse_portfolio, parse_frontmatter
from calculator import MAX_BUY_PER_WEEK
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path("/Users/hxh/Documents/a-stock-trading/scripts")
DATA_DIR = SCRIPT_DIR.parent / "data"
JOURNAL_DIR = DATA_DIR / "02-日志"
PORTFOLIO_PATH = DATA_DIR / "01-持仓" / "portfolio.md"
CORE_POOL_PATH = DATA_DIR / "04-选股" / "核心池.md"

def load_legacy_portfolio():
    """加载港股遗留仓位（独立管理，不计入新系统持仓）"""
    try:
        with open(PORTFOLIO_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
        holdings = []
        in_table = False
        for line in content.split('\n'):
            line = line.strip()
            if '## 港股持仓明细（独立管理）' in line:
                in_table = True
                continue
            if in_table and line.startswith('##'):
                break
            if in_table and line.startswith('|') and not line.startswith('|--') and '股票' not in line:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 10:
                    stock = parts[1]
                    code = parts[2]
                    shares = parts[4].strip()
                    if stock and stock not in ['', '—'] and shares not in ['0', '—', '']:
                        holdings.append({'股票': stock, '代码': code, '持有股数': shares})
        return holdings
    except:
        return []

def load_core_pool():
    """解析核心池markdown表格，只取## 当前核心池表格中的股票"""
    try:
        with open(CORE_POOL_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
        stocks = []
        in_current_section = False
        for line in content.split('\n'):
            line = line.strip()
            # 遇到"## 当前核心池"就进入解析模式
            if '## 当前核心池' in line:
                in_current_section = True
                continue
            # 遇到其他 ## 标题就停止
            if in_current_section and line.startswith('##'):
                break
            if in_current_section and line.startswith('|'):
                if '股票' in line and '代码' in line:
                    continue  # 表头
                if '--' in line or '---' in line:
                    continue
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 3:
                    name = parts[2].strip()
                    code = parts[3].strip() if len(parts) > 3 else ''
                    if name and name not in ['股票', ''] and code.isdigit():
                        stocks.append({'name': name, 'code': code})
        return stocks
    except:
        return []

def get_weekly_buy_count():
    try:
        import subprocess
        result = subprocess.run(
            ["git", "log", "--since=week", "--pretty=format:", "--", str(JOURNAL_DIR)],
            capture_output=True, text=True, timeout=5
        )
        return result.stdout.count("买入")
    except:
        return 0

def format_market(sh, cy, summary):
    from datetime import date
    is_today = sh.get("date") == date.today().isoformat()
    rt = "[实时]" if sh.get("realtime") else ("[今日收盘]" if is_today else "[昨日]")
    sh_ma = "✅ MA20上方" if sh.get("above_MA20") else "❌ MA20下方"
    cy_ma = "✅ MA20上方" if cy.get("above_MA20") else "❌ MA20下方"
    sh_chg = f"{sh.get('change_pct', 0):+.2f}%" if sh.get('change_pct') is not None else "—"
    cy_chg = f"{cy.get('change_pct', 0):+.2f}%" if cy.get('change_pct') is not None else "—"
    
    status_map = {"CLEAR": "⚠️ 清仓信号", "BUY": "✅ 可买入", "WARY": "🔶 谨慎"}
    status_text = status_map.get(summary.get('status', ''), summary.get('status', 'UNKNOWN'))
    
    lines = [
        f"**上证指数** {sh.get('close')} ({sh_chg}) {rt}",
        f"  {sh_ma} | MA20={sh.get('MA20')} | MA60下方{sh.get('below_MA60_days', 0)}天",
        f"**创业板指** {cy.get('close')} ({cy_chg}) {rt}",
        f"  {cy_ma} | MA20={cy.get('MA20')} | MA60下方{cy.get('below_MA60_days', 0)}天",
        f"",
        f"**判定: {status_text}**",
    ]
    return "\n".join(lines)

def format_holdings(holdings, legacy=None):
    if not holdings and not legacy:
        return "空仓"
    lines = []
    if holdings:
        lines += [f"- {h.get('股票','')} ({h.get('代码','')})" for h in holdings]
    if legacy:
        lines.append("")
        lines.append("**港股遗留仓位（独立管理）**")
        lines += [f"- {h.get('股票','')} {h.get('持有股数','')}股" for h in legacy]
    return "\n".join(lines)

def format_core_pool(stocks):
    if not stocks:
        return "暂无"
    return "\n".join([f"- {s['name']} ({s['code']})" for s in stocks])

def generate(mode="morning"):
    from akshare_data import get_technical
    
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    mode_labels = {
        "morning": ("MORNING", "🌅", "盘前检核"),
        "noon": ("NOON", "☀️", "午间复盘"),
        "evening": ("EVEVING", "🌙", "盘后总结"),
        "weekly": ("WEEKLY", "📊", "周复盘"),
        "monthly": ("MONTHLY", "📈", "月复盘"),
    }
    tag, emoji, title = mode_labels.get(mode, ("MISC", "📋", "交易报告"))
    
    market = get_market_status()
    sh = market.get("上证指数", {})
    cy = market.get("创业板指", {})
    summary = market.get("_summary", {})
    new_holdings = []  # 新系统持仓（目前为空）
    legacy = load_legacy_portfolio()
    codes = load_core_pool()
    weekly_buys = get_weekly_buy_count()
    
    sections = [
        f"## {emoji} {title} — {ts}",
        "",
        "### 大盘状态",
        format_market(sh, cy, summary),
        "",
        "### 持仓状态",
        format_holdings(new_holdings, legacy),
        "",
        "### 核心池",
        format_core_pool(codes),
        "",
        "### 风控",
        f"本周买入: {weekly_buys}/{MAX_BUY_PER_WEEK}",
    ]
    
    if mode == "morning":
        sections += [
            "",
            "### 今日操作",
            "- 等待大盘MA20收复后操作" if summary.get('status') == "CLEAR" else "- 关注核心池标的择机入场",
        ]
    
    sections += [
        "",
        "---",
        "*A股交易系统 v1.4 | 旺财*",
    ]
    
    return "\n".join(sections)

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "morning"
    print(generate(mode))
