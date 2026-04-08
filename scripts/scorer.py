#!/usr/bin/env python3
"""
A股交易系统 v1.3 - 选股打分引擎
用法: python scorer.py <json_input>

输入 JSON 格式:
{
    "stock_name": "示例A",
    "stock_code": "600xxx",
    "is_main_sector": true,          // 1. 属于当前主线板块
    "has_catalyst": true,            // 2. 有政策或产业催化
    "roe_pass": true,                // 3. ROE≥8%（连续两年）
    "revenue_growth": true,          // 4. 营收连续两季度增长
    "cash_flow_positive": true,      // 5. 经营现金流为正
    "above_ma20": true,              // 6. 股价站上20日均线（一票否决）
    "ma60_up_or_flat": true,         // 7. 60日均线向上或走平
    "volume_score": 1.0,             // 8. 成交量得分: 1.0/0.5/0
    "northbound_inflow": true,       // 9. 北向资金近5日净流入
    "no_major_outflow": true,        // 10. 无连续主力大额卖出
    "volume_breakout": false         // 11. 加分项: 突破量>5日均量1.5倍
}
"""

import sys
import json


def score_stock(data: dict) -> dict:
    """
    选股打分

    返回:
        score: 总分
        passed: 是否通过
        details: 各项得分明细
        reject_reason: 未通过原因
    """
    details = []
    total_score = 0.0

    # 评分项定义: (key, name, score, is_veto)
    items = [
        ("is_main_sector", "属于当前主线板块", 1.0, False),
        ("has_catalyst", "有政策或产业催化", 1.0, False),
        ("roe_pass", "ROE≥8%（连续两年）", 1.0, False),
        ("revenue_growth", "营收连续两季度增长", 1.0, False),
        ("cash_flow_positive", "经营现金流为正", 1.0, False),
        ("above_ma20", "股价站上20日均线", 1.0, True),  # 一票否决
        ("ma60_up_or_flat", "60日均线向上或走平", 1.0, False),
        ("northbound_inflow", "北向资金近5日净流入", 1.0, False),
        ("no_major_outflow", "无连续主力大额卖出", 1.0, False),
    ]

    veto_failed = False

    for key, name, score, is_veto in items:
        value = data.get(key, False)
        item_score = score if value else 0
        total_score += item_score
        status = "✅" if value else ("❌ [一票否决]" if is_veto else "❌")
        details.append({
            "item": name,
            "passed": value,
            "score": item_score,
            "status": status
        })
        if is_veto and not value:
            veto_failed = True

    # 成交量条件（特殊评分）
    volume_score = data.get("volume_score", 0)
    volume_passed = volume_score >= 0.5
    total_score += volume_score
    details.append({
        "item": "成交量条件",
        "passed": volume_passed,
        "score": volume_score,
        "status": f"{'✅' if volume_passed else '❌'} (得分: {volume_score})"
    })

    # 加分项
    volume_breakout = data.get("volume_breakout", False)
    bonus = 0.5 if volume_breakout else 0
    total_score += bonus
    details.append({
        "item": "加分项: 突破量>5日均量1.5倍",
        "passed": volume_breakout,
        "score": bonus,
        "status": f"{'✅ +0.5' if volume_breakout else '—'}"
    })

    # 判断是否通过
    reject_reasons = []
    if veto_failed:
        reject_reasons.append("一票否决: 股价未站上20日均线")
    if total_score < 7:
        reject_reasons.append(f"总分{total_score}分，未达到7分通过线")
    if not volume_passed:
        reject_reasons.append("成交量条件未满足（得分<0.5）")

    passed = len(reject_reasons) == 0

    result = {
        "stock_name": data.get("stock_name", "未知"),
        "stock_code": data.get("stock_code", ""),
        "total_score": total_score,
        "max_score": 10.5,
        "passed": passed,
        "grade": "A" if total_score >= 9 else "B" if total_score >= 7 else "C",
        "reject_reasons": reject_reasons,
        "details": details,
        "recommendation": "可加入核心池" if passed and total_score >= 8 else "可加入观察池" if passed else "不建议买入"
    }
    return result


def format_score_table(result: dict) -> str:
    """格式化为 Obsidian MD 表格"""
    lines = []
    lines.append(f"## {result['stock_name']}（{result['stock_code']}）选股打分")
    lines.append("")
    lines.append(f"**总分: {result['total_score']}/{result['max_score']}** | "
                 f"**等级: {result['grade']}** | "
                 f"**结论: {'✅ 通过' if result['passed'] else '❌ 未通过'}**")
    lines.append("")
    lines.append("| 检查项 | 状态 | 得分 |")
    lines.append("|--------|------|------|")
    for d in result["details"]:
        lines.append(f"| {d['item']} | {d['status']} | {d['score']} |")
    lines.append("")

    if result["reject_reasons"]:
        lines.append("**未通过原因:**")
        for r in result["reject_reasons"]:
            lines.append(f"- {r}")
    else:
        lines.append(f"**建议:** {result['recommendation']}")

    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        # 输出示例
        example = {
            "stock_name": "示例A",
            "stock_code": "600xxx",
            "is_main_sector": True,
            "has_catalyst": True,
            "roe_pass": True,
            "revenue_growth": True,
            "cash_flow_positive": True,
            "above_ma20": True,
            "ma60_up_or_flat": True,
            "volume_score": 1.0,
            "northbound_inflow": True,
            "no_major_outflow": True,
            "volume_breakout": False
        }
        print(f"\n示例输入:\npython scorer.py '{json.dumps(example, ensure_ascii=False)}'")
        sys.exit(1)

    data = json.loads(sys.argv[1])
    result = score_stock(data)

    # 输出 JSON 结果
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("\n---\n")
    # 输出 MD 格式
    print(format_score_table(result))


if __name__ == "__main__":
    main()
