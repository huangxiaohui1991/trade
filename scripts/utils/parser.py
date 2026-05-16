"""Markdown and reply parsers for legacy scripts."""

from __future__ import annotations


def parse_md_table(content: str) -> list[dict]:
    lines = [line.strip() for line in str(content or "").splitlines() if line.strip()]
    tables: list[dict] = []
    idx = 0
    while idx < len(lines):
        if "|" not in lines[idx] or idx + 1 >= len(lines) or "---" not in lines[idx + 1]:
            idx += 1
            continue
        headers = [cell.strip() for cell in lines[idx].strip("|").split("|")]
        rows = []
        idx += 2
        while idx < len(lines) and "|" in lines[idx]:
            cells = [cell.strip() for cell in lines[idx].strip("|").split("|")]
            cells = (cells + [""] * len(headers))[: len(headers)]
            rows.append(dict(zip(headers, cells)))
            idx += 1
        tables.append({"headers": headers, "rows": rows})
    return tables


def parse_user_reply(reply_text: str) -> dict:
    return {"raw_text": str(reply_text or "").strip()}
