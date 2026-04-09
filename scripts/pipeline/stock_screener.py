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
from time import sleep

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

import akshare as ak

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

def _get_blacklist(stocks_cfg: dict) -> set:
    """读取永久/临时黑名单代码。"""
    blacklist = set(stocks_cfg.get("blacklist", {}).get("permanent", []))
    blacklist.update(stocks_cfg.get("blacklist", {}).get("temporary", []))
    return blacklist


def _dedupe_candidates(candidates: list) -> list:
    """按代码去重并保留原始顺序。"""
    deduped = []
    seen = set()
    for item in candidates:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append({"code": code, "name": name})
    return deduped


def _tracked_candidates(pool: str, stocks_cfg: dict, blacklist: set) -> tuple[list, str]:
    """返回现有核心池/观察池候选集。"""
    if pool == "core":
        candidates = stocks_cfg.get("core_pool", [])
        pool_name = "核心"
    elif pool == "watch":
        candidates = stocks_cfg.get("watch_pool", [])
        pool_name = "观察"
    else:
        candidates = stocks_cfg.get("core_pool", []) + stocks_cfg.get("watch_pool", [])
        pool_name = "综合"

    candidates = [c for c in candidates if c.get("code") not in blacklist]
    return _dedupe_candidates(candidates), pool_name


def _fallback_tracked_candidates(stocks_cfg: dict, blacklist: set) -> list:
    """tracked 模式下的 fallback，直接回退到已跟踪股票池。"""
    _logger.info("[fallback] 使用已跟踪股票池作为候选")
    candidates = stocks_cfg.get("core_pool", []) + stocks_cfg.get("watch_pool", [])
    candidates = [c for c in candidates if c.get("code") not in blacklist]
    candidates = _dedupe_candidates(candidates)
    _logger.info(f"[fallback] 候选股票: {len(candidates)} 只")
    return candidates


def _safe_float(value, default=0.0) -> float:
    try:
        return float(value) if value is not None and value != "" else default
    except (ValueError, TypeError):
        return default


def _fallback_market_candidates(strategy_cfg: dict, blacklist: set) -> list:
    """
    当 mx skill 不可用时，使用 akshare 全市场快照做一轮轻筛。
    先用价格/成交额/PE/ST 过滤，再按成交额排序截断到固定数量。
    """
    screening_cfg = strategy_cfg.get("screening", {})
    filters = screening_cfg.get("fallback_filters", {})
    min_price = filters.get("min_price", 5)
    max_price = filters.get("max_price", 200)
    min_amount = filters.get("min_amount", 100_000_000)
    max_pe = filters.get("max_pe_ttm", 40)
    exclude_st = filters.get("exclude_st", True)
    market_limit = int(screening_cfg.get("market_scan_limit", 30))

    last_error = None
    for attempt in range(1, 4):
        try:
            _logger.info(
                "[fallback-market] 使用 akshare 全市场快照初筛 "
                f"(attempt {attempt}/3, limit={market_limit})"
            )
            df = ak.stock_zh_a_spot_em()
            break
        except Exception as e:
            last_error = e
            if attempt < 3:
                sleep(1)
    else:
        _logger.warning(f"[fallback-market] 获取全市场快照失败: {last_error}")
        return []

    candidates = []
    for _, row in df.iterrows():
        code = str(row.get("代码", "")).strip()
        name = str(row.get("名称", "")).strip()
        if not code or code in blacklist:
            continue
        if exclude_st and ("ST" in name.upper() or "退" in name):
            continue

        price = _safe_float(row.get("最新价"))
        amount = _safe_float(row.get("成交额"))
        pe = _safe_float(row.get("市盈率-动态"), default=-1)

        if not (min_price <= price <= max_price):
            continue
        if amount < min_amount:
            continue
        if pe > 0 and pe > max_pe:
            continue

        candidates.append({
            "code": code,
            "name": name,
            "_amount": amount,
        })

    candidates.sort(key=lambda item: item.get("_amount", 0), reverse=True)
    trimmed = [{"code": item["code"], "name": item["name"]} for item in candidates[:market_limit]]
    _logger.info(f"[fallback-market] 初筛通过 {len(candidates)} 只，送评估 {len(trimmed)} 只")
    return trimmed


def _write_market_scan_watchlist(results: list) -> str:
    """将市场扫描中可操作的股票写入观察池候选文件。"""
    vault = ObsidianVault()
    report_dir = Path(vault.vault_path) / "04-选股" / "筛选结果"
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"市场扫描候选_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    actionable = [r for r in results if not r.get("veto_triggered") and r.get("total_score", 0) >= 5]
    lines = [
        f"# 市场扫描候选 — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "| # | 股票 | 代码 | 总分 | 建议 |",
        "|---|------|------|------|------|",
    ]
    for i, row in enumerate(actionable, 1):
        suggestion = "可买入" if row.get("total_score", 0) >= 7 else "观察"
        lines.append(f"| {i} | {row.get('name','')} | {row.get('code','')} | {row.get('total_score',0):.1f} | {suggestion} |")
    if not actionable:
        lines.append("| — | — | — | — | 本次无满足条件候选 |")

    path.write_text("\n".join(lines), encoding="utf-8")
    _logger.info(f"  市场扫描候选已写入: {path.name}")
    return str(path)


def _resolve_market_candidates(default_query: str, select_type: str,
                               strategy_cfg: dict, blacklist: set) -> tuple[list, str]:
    """全市场模式：优先 mx-screener，失败则使用 akshare 轻筛。"""
    if default_query:
        mx_results = _call_mx_screener(default_query, select_type)
        if mx_results:
            candidates = [c for c in _dedupe_candidates(mx_results) if c.get("code") not in blacklist]
            _logger.info(f">> mx-screener 全市场初筛返回 {len(candidates)} 只")
            return candidates, "mx-stocks-screener skill（全市场）"

        _logger.warning(">> mx-screener 调用失败，fallback 到 akshare 全市场轻筛")

    candidates = _fallback_market_candidates(strategy_cfg, blacklist)
    return candidates, "akshare 全市场轻筛"


def _resolve_tracked_candidates(pool: str, stocks_cfg: dict, blacklist: set,
                                default_query: str, select_type: str) -> tuple[list, str, str]:
    """已跟踪模式：先从 core/watch 取池子，再做命中筛选。"""
    base_candidates, pool_name = _tracked_candidates(pool, stocks_cfg, blacklist)
    _logger.info(f">> 候选股票: {len(base_candidates)} 只")
    if not base_candidates:
        return [], pool_name, ""

    if default_query:
        mx_results = _call_mx_screener(default_query, select_type)
        if mx_results:
            mx_codes = {r["code"] for r in mx_results}
            candidates = [c for c in base_candidates if c.get("code") in mx_codes]
            _logger.info(f">> mx-screener 初筛: {len(mx_results)} 只通过，候选池命中 {len(candidates)} 只")
            if candidates:
                return candidates, pool_name, "mx-stocks-screener skill"
            _logger.warning(">> mx 筛选结果为空，fallback")
        else:
            _logger.warning(">> mx-screener 调用失败，fallback 到 akshare")

    return _fallback_tracked_candidates(stocks_cfg, blacklist), pool_name, "akshare 原生接口"


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
    elif "全市场" in source:
        lines.append("- 全市场候选池初筛（价格 / 成交额 / 市盈率 / ST 过滤）")
        lines.append(f"- 数据来源：{source}")
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

def run(pool: str = "watch", universe: str = "tracked") -> list:
    """
    执行选股流水线

    Args:
        pool: "core" | "watch" | "all"
        universe: "tracked" | "market"

    Returns:
        评分结果列表
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    _logger.info(f"[SCREENER] 选股流水线 {today_str} pool={pool} universe={universe}")

    vault = ObsidianVault()
    stocks_cfg = get_stocks()
    strategy_cfg = get_strategy()

    blacklist = _get_blacklist(stocks_cfg)

    # ------------------------------------------------------------------
    # 优先 mx-screener，失败则 fallback
    # ------------------------------------------------------------------
    # 构造查询：核心池用宽松条件，观察池用严格条件
    screening_cfg = strategy_cfg.get("screening", {})
    default_query = screening_cfg.get("mx_query", "")
    select_type = screening_cfg.get("mx_select_type", "A股")

    if universe == "market":
        candidates, source = _resolve_market_candidates(default_query, select_type, strategy_cfg, blacklist)
        pool_name = "市场扫描"
        _logger.info(f">> 市场扫描候选: {len(candidates)} 只")
    else:
        candidates, pool_name, source = _resolve_tracked_candidates(
            pool, stocks_cfg, blacklist, default_query, select_type
        )

    if not candidates:
        _logger.warning("无候选股票，退出")
        return []

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

    if universe == "market":
        _write_market_scan_watchlist(scored)

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
    parser.add_argument(
        "--universe",
        choices=["tracked", "market"],
        default="tracked",
        help="候选集范围：tracked=现有池，market=全市场扫描"
    )
    args = parser.parse_args()

    results = run(pool=args.pool, universe=args.universe)
    print(f"\n筛选完成: {len(results)} 只")
    if results:
        print("\nTOP 5:")
        for i, r in enumerate(results[:5], 1):
            veto = " ❌" if r.get("veto_triggered") else ""
            print(f"  {i}. {r['name']}({r['code']}): {r['total_score']:.1f}{veto}")


if __name__ == "__main__":
    main()
