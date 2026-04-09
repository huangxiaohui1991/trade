#!/usr/bin/env python3
"""
pipeline/stock_screener.py — 选股流水线

职责：
  1. 读取 config/stocks.yaml 的核心池/观察池/黑名单
  2. 优先调用 mx-stocks-screener skill（OpenClaw）进行自然语言筛选
  3. mx skill 不可用或失败时，fallback 到 akshare 原生接口
  4. 对候选池进行四维评分，写入 Obsidian 筛选记录

用法：
  python -m scripts.pipeline.stock_screener
  python -m scripts.pipeline.stock_screener --pool core
  python -m scripts.pipeline.stock_screener --pool watch
"""

import csv
import os
import subprocess
import sys
import warnings
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.utils.obsidian import ObsidianVault
from scripts.utils.config_loader import get_stocks, get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("pipeline.stock_screener")

# mx-stocks-screener skill 脚本路径
_MX_SCREENER_SCRIPT = os.path.expanduser(
    "~/.openclaw/workspace/skills/mx-stocks-screener/scripts/get_data.py"
)
_MX_OUTPUT_DIR = os.path.expanduser("~/miaoxiang/mx_stocks_screener")


# ---------------------------------------------------------------------------
# mx-screener 调用（优先）
# ---------------------------------------------------------------------------

def _call_mx_screener(query: str, select_type: str = "A股") -> list:
    """
    调用 mx-stocks-screener skill（东方财富妙想接口）进行自然语言筛选。

    优先使用，失败时返回空列表（触发 fallback）。

    Returns:
        list of {"code": str, "name": str}，空列表表示调用失败
    """
    # 1. 检查 EM_API_KEY
    api_key = os.environ.get("EM_API_KEY") or ""
    if not api_key:
        _logger.warning("[mx-screener] EM_API_KEY 未设置，跳过 mx skill")
        return []

    # 2. 检查脚本是否存在
    if not os.path.exists(_MX_SCREENER_SCRIPT):
        _logger.warning(f"[mx-screener] skill 脚本不存在: {_MX_SCREENER_SCRIPT}")
        return []

    # 3. 确保输出目录存在
    os.makedirs(_MX_OUTPUT_DIR, exist_ok=True)

    # 4. 调用
    try:
        _logger.info(f"[mx-screener] 调用: query='{query}' type={select_type}")
        env = os.environ.copy()
        env["EM_API_KEY"] = api_key
        result = subprocess.run(
            [
                sys.executable,
                _MX_SCREENER_SCRIPT,
                "--query", query,
                "--select-type", select_type,
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )

        if result.returncode != 0:
            _logger.warning(f"[mx-screener] 调用失败 returncode={result.returncode}: {result.stderr[:200]}")
            return []

        # 5. 解析 CSV 输出路径
        csv_path = None
        for line in result.stdout.splitlines():
            if line.startswith("CSV:"):
                csv_path = line.split(":", 1)[1].strip()
                break

        if not csv_path or not os.path.exists(csv_path):
            _logger.warning(f"[mx-screener] 未找到 CSV 文件: {csv_path}")
            return []

        # 6. 解析 CSV，提取 code 和 name
        results = []
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get("代码", "").strip()
                name = row.get("名称", "").strip()
                if code and name:
                    results.append({"code": code, "name": name})

        _logger.info(f"[mx-screener] 成功，返回 {len(results)} 条结果")
        return results

    except subprocess.TimeoutExpired:
        _logger.warning("[mx-screener] 调用超时（60s）")
        return []
    except Exception as e:
        _logger.warning(f"[mx-screener] 调用异常: {e}")
        return []


# ---------------------------------------------------------------------------
# fallback：akshare 原生筛选
# ---------------------------------------------------------------------------

def _fallback_screener() -> list:
    """
    当 mx skill 不可用时，fallback 到 akshare 原生筛选逻辑。

    当前策略：返回核心池全部候选股票（不做额外过滤）。
    后续可扩展：加入市值/PE/行业等原生条件筛选。
    """
    _logger.info("[fallback] 使用 akshare 原生接口，返回全部候选股票")
    stocks_cfg = get_stocks()

    candidates = (
        stocks_cfg.get("core_pool", []) +
        stocks_cfg.get("watch_pool", [])
    )

    # 过滤黑名单
    blacklist = set(stocks_cfg.get("blacklist", {}).get("permanent", []))
    blacklist.update(stocks_cfg.get("blacklist", {}).get("temporary", []))
    candidates = [c for c in candidates if c.get("code") not in blacklist]

    _logger.info(f"[fallback] 候选股票: {len(candidates)} 只")
    return candidates


# ---------------------------------------------------------------------------
# 报告写入
# ---------------------------------------------------------------------------

def _write_screening_result(results: list, pool_name: str, source: str) -> str:
    """写入筛选结果到 Obsidian"""
    vault = ObsidianVault()
    date_str = datetime.now().strftime("%Y%m%d")
    time_str = datetime.now().strftime("%H%M%S")

    report_dir = Path(vault.vault_path) / "04-选股" / "筛选结果"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"筛选结果_{pool_name}_{date_str}_{time_str}.md"

    lines = [
        f"# 选股筛选报告 — {pool_name}池 {date_str}",
        "",
        f"筛选时间：{datetime.now().strftime('%H:%M')}",
        f"数据来源：{source}",
        "",
        "---",
        "",
        "| # | 股票 | 代码 | 四维总分 | 技术 | 基本面 | 资金 | 舆情 | 建议 |",
        "|---|------|------|---------|------|--------|------|------|------|",
    ]

    for i, r in enumerate(results, 1):
        total = r.get("total_score", 0)
        if r.get("veto_triggered"):
            suggestion = "❌ 一票否决"
        elif total >= 7:
            suggestion = "✅ 可买入"
        elif total >= 5:
            suggestion = "🟡 观察"
        else:
            suggestion = "❌ 规避"

        lines.append(
            f"| {i} | {r.get('name','')} | {r.get('code','')} | "
            f"**{total:.1f}** | {r.get('technical_score',0):.1f} | "
            f"{r.get('fundamental_score',0):.1f} | {r.get('flow_score',0):.1f} | "
            f"{r.get('sentiment_score',0):.1f} | {suggestion} |"
        )

    if not results:
        lines.append("| — | — | — | — | — | — | — | — | 暂无筛选结果 |")

    lines.extend(["", "---", "", "## 筛选条件", ""])
    if source == "mx-stocks-screener skill":
        lines.append("- 由 mx-stocks-screener skill 东方财富妙想接口自然语言筛选")
    else:
        lines.append("- 核心池候选股票（akshare 原生接口）")
        lines.append("- 黑名单过滤（永久+临时）")
    lines.append("")
    lines.append(f"> 本报告由 A股交易系统 自动生成")

    content = "\n".join(lines)
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(content)

    _logger.info(f"  已写入: {report_path.name}")
    return str(report_path)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run(pool: str = "watch") -> list:
    """
    执行选股流水线

    Args:
        pool: "core" | "watch" | "all"

    Returns:
        评分结果列表
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    _logger.info(f"[SCREENER] 选股流水线 {today_str} pool={pool}")

    vault = ObsidianVault()
    stocks_cfg = get_stocks()
    strategy_cfg = get_strategy()

    # 读取黑名单
    blacklist = set(stocks_cfg.get("blacklist", {}).get("permanent", []))
    blacklist.update(stocks_cfg.get("blacklist", {}).get("temporary", []))

    # 获取候选股票基础列表
    if pool == "core":
        base_candidates = stocks_cfg.get("core_pool", [])
        pool_name = "核心"
    elif pool == "watch":
        base_candidates = stocks_cfg.get("watch_pool", [])
        pool_name = "观察"
    else:
        base_candidates = (
            stocks_cfg.get("core_pool", []) +
            stocks_cfg.get("watch_pool", [])
        )
        pool_name = "综合"

    # 过滤黑名单
    base_candidates = [c for c in base_candidates if c.get("code") not in blacklist]
    _logger.info(f">> 候选股票: {len(base_candidates)} 只")

    if not base_candidates:
        _logger.warning("无候选股票，退出")
        return []

    # ------------------------------------------------------------------
    # 优先 mx-screener，失败则 fallback
    # ------------------------------------------------------------------
    # 构造查询：核心池用宽松条件，观察池用严格条件
    screening_cfg = strategy_cfg.get("screening", {})
    default_query = screening_cfg.get("mx_query", "")
    select_type = screening_cfg.get("mx_select_type", "A股")

    candidates = None
    source = ""

    if default_query:
        # 有配置查询词，优先调用 mx skill
        mx_results = _call_mx_screener(default_query, select_type)
        if mx_results:
            mx_codes = {r["code"] for r in mx_results}
            candidates = [c for c in base_candidates if c.get("code") in mx_codes]
            source = "mx-stocks-screener skill"
            _logger.info(f">> mx-screener 初筛: {len(mx_results)} 只通过，"
                         f"候选池命中 {len(candidates)} 只")
        else:
            _logger.warning(">> mx-screener 调用失败，fallback 到 akshare")
            candidates = base_candidates
            source = "akshare 原生接口"
    else:
        # 无查询词，直接走 akshare
        candidates = base_candidates
        source = "akshare 原生接口"

    # 若 mx 筛选后为空，fallback
    if not candidates:
        _logger.warning(">> mx 筛选结果为空，fallback")
        candidates = base_candidates
        source = "akshare 原生接口（mx结果为空 fallback）"

    # ------------------------------------------------------------------
    # 四维评分
    # ------------------------------------------------------------------
    _logger.info(f">> 四维评分（来源: {source}）...")
    from scripts.engine.scorer import batch_score
    scored = batch_score(candidates)

    # 过滤一票否决（用于统计）
    actionable = [r for r in scored if not r.get("veto_triggered", False)]

    # ------------------------------------------------------------------
    # 写入 Obsidian
    # ------------------------------------------------------------------
    _logger.info(">> 写入筛选报告...")
    report_path = _write_screening_result(scored, pool_name, source)

    _logger.info(
        f"[SCREENER] 完成: {len(scored)} 只评分, "
        f"{len(actionable)} 只可操作 → {report_path}"
    )

    return scored


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="选股流水线")
    parser.add_argument(
        "--pool",
        choices=["core", "watch", "all"],
        default="watch",
        help="筛选哪个池（默认 watch）"
    )
    args = parser.parse_args()

    results = run(pool=args.pool)
    print(f"\n筛选完成: {len(results)} 只")
    if results:
        print("\nTOP 5:")
        for i, r in enumerate(results[:5], 1):
            veto = " ❌" if r.get("veto_triggered") else ""
            print(f"  {i}. {r['name']}({r['code']}): {r['total_score']:.1f}{veto}")


if __name__ == "__main__":
    main()
