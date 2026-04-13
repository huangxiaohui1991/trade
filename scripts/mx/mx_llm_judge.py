#!/usr/bin/env python3
"""
mx_llm_judge — MiniMax LLM 情感判断

调用 MiniMax Claude-compatible API，对个股新闻做结构化情感分析。

用法：
  python -m scripts.mx.mx_llm_judge --stock 300938 --name "信测标准" \
    --title "信测标准2025年点评..." --content "公司实现营收8.04亿元..."
"""

import json
import os
import sys
import time
import requests
from pathlib import Path
from typing import Any, Dict, List, Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.utils.logger import get_logger

_logger = get_logger("mx.llm_judge")

_BASE_URL = os.environ.get(
    "MINIMAX_BASE_URL", "https://api.minimaxi.com/anthropic"
).rstrip("/")

_APIKEY = os.environ.get("MINIMAX_APIKEY", "")
_MODEL = "mini-max-claude"

# 系统 prompt：A股舆情分析师
SYSTEM_PROMPT = """你是一个专业的A股舆情分析师。
给定一只股票的新闻/公告内容，你需要判断其对股价的潜在影响。

返回严格的JSON格式，不要有其他内容：
{
  "sentiment": "negative|neutral|positive",
  "level": "high|medium|low",
  "reason": "一句话说明判断理由（10字以内）",
  "should_alert": true|false,
  "risk_keywords": ["关键词1", "关键词2"]
}

判断标准：
- should_alert=true: 对持仓有实质负面影响，需人工复核（业绩暴雷、监管处罚、股东清仓减持、重大诉讼、产品事故等）
- should_alert=false: 正向新闻、中性公告，或影响轻微不足以触发告警
- 关键词要具体，如"大比例减持"而不是"减持" """


def _call_llm(messages: List[Dict], timeout: int = 30, max_retries: int = 3) -> str:
    """调用 MiniMax Claude-compatible API，返回纯文本；失败时指数退避重试。"""
    if not _APIKEY:
        raise ValueError("MINIMAX_APIKEY 未配置，请在 .env 中设置")

    url = f"{_BASE_URL}/v1/messages"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_APIKEY}",
        "anthropic-version": "2023-06-01",
    }
    payload = {
        "model": _MODEL,
        "max_tokens": 512,
        "system": SYSTEM_PROMPT,
        "messages": messages,
    }

    for attempt in range(max_retries):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
            if resp.status_code == 529:
                # 服务端过载，指数退避后重试
                wait = 2 ** attempt
                _logger.warning(f"[llm_judge] API 过载(529)，{wait}s 后重试（第{attempt+1}/{max_retries}次）...")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                _logger.warning(f"[llm_judge] API 错误 {resp.status_code}: {resp.text[:200]}")
                resp.raise_for_status()
            data = resp.json()
            # MiniMax 可能返回 thinking 块，优先找 text 块
            content = data.get("content", [])
            text_blocks = [b for b in content if b.get("type") == "text"]
            if not text_blocks:
                raise ValueError(f"API 返回无 text 块: {content[0].get('type', 'unknown')}")
            return text_blocks[0]["text"].strip()
        except (requests.exceptions.RequestException, KeyError, IndexError, ValueError) as exc:
            exc_name = type(exc).__name__
            if attempt == max_retries - 1:
                _logger.warning(f"[llm_judge] API 调用最终失败（{exc_name}）: {exc}")
                raise
            wait = 2 ** attempt
            _logger.warning(f"[llm_judge] API 异常（{exc_name}），{wait}s 后重试（第{attempt+1}/{max_retries}次）: {exc}")
            time.sleep(wait)
    raise RuntimeError("LLM 调用超过最大重试次数")


def parse_json_reply(raw: str) -> Dict[str, Any]:
    """从 LLM 输出中解析 JSON（处理 markdown code block）"""
    text = raw.strip()
    if text.startswith("```"):
        # 去掉 markdown code block 标记
        text = text.strip("`")
        # 去掉 ```json 或 ``` 前缀
        text = text.lstrip("json\n").lstrip("json\n").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        _logger.warning(f"[llm_judge] JSON 解析失败，原始输出: {text[:200]}")
        return {
            "sentiment": "neutral",
            "level": "low",
            "reason": "解析失败",
            "should_alert": False,
            "risk_keywords": [],
            "_raw": text,
        }


class LLMJudge:
    """MiniMax LLM 情感判断 — 供 MX dispatch 调用的类封装"""

    def judge(
        self,
        stock_code: str,
        stock_name: str,
        title: str,
        content: str,
        url: str = "",
    ) -> Dict[str, Any]:
        return judge(stock_code, stock_name, title, content, url)


def judge(
    stock_code: str,
    stock_name: str,
    title: str,
    content: str,
    url: str = "",
) -> Dict[str, Any]:
    """
    对单条新闻做 LLM 情感判断。

    Returns:
        dict: {
            "sentiment": "negative" | "neutral" | "positive",
            "level": "high" | "medium" | "low",
            "reason": str,
            "should_alert": bool,
            "risk_keywords": list[str],
        }
    """
    user_content = f"""股票：{stock_name}（{stock_code}）
标题：{title}
正文：{content[:2000]}
链接：{url}"""

    messages = [{"role": "user", "content": user_content}]
    try:
        raw = _call_llm(messages)
        result = parse_json_reply(raw)
        result["stock_code"] = stock_code
        result["stock_name"] = stock_name
        result["title"] = title[:100]
        result["url"] = url
        return result
    except Exception as e:
        _logger.warning(f"[llm_judge] 调用失败 {stock_name}({stock_code}): {e}")
        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "title": title[:100],
            "url": url,
            "sentiment": "neutral",
            "level": "low",
            "reason": f"LLM调用失败: {e}",
            "should_alert": False,
            "risk_keywords": [],
            "_error": str(e),
        }


def batch_judge(news_list: List[Dict[str, str]], max_concurrent: int = 5) -> List[Dict[str, Any]]:
    """
    批量新闻情感判断。

    news_list 元素：{stock_code, stock_name, title, content, url}
    """
    results: List[Dict[str, Any]] = []
    for item in news_list:
        result = judge(
            stock_code=item["stock_code"],
            stock_name=item["stock_name"],
            title=item["title"],
            content=item["content"],
            url=item.get("url", ""),
        )
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="LLM 舆情情感判断")
    parser.add_argument("--stock", required=True, help="股票代码")
    parser.add_argument("--name", required=True, help="股票名称")
    parser.add_argument("--title", required=True, help="新闻标题")
    parser.add_argument("--content", default="", help="新闻正文（可为空）")
    parser.add_argument("--url", default="", help="链接")
    args = parser.parse_args()

    result = judge(args.stock, args.name, args.title, args.content, args.url)
    print(json.dumps(result, ensure_ascii=False, indent=2))
