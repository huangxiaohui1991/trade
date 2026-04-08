#!/usr/bin/env python3
"""批量获取股票详细打分信息，输出精简表格"""
import sys, json, subprocess

codes = sys.argv[1].split(',')
results = []

for code in codes:
    try:
        out = subprocess.check_output(
            ['python3', '.kiro/skills/a-stock-trading/scripts/akshare_data.py', 'score', code],
            stderr=subprocess.DEVNULL, timeout=30
        )
        data = json.loads(out)
        raw = data.get('raw_data', {})
        tech = raw.get('technical', {})
        fund = raw.get('fundamental', {})
        flow = raw.get('fund_flow', {})
        north = raw.get('northbound', {})
        score_input = data.get('score_input', {})
        
        ma = tech.get('ma', {})
        vol = tech.get('volume_analysis', {})
        
        # ROE 判断
        roe_list = fund.get('roe_recent', [])
        roe_val = roe_list[0] if roe_list else None
        roe_ok = '✅' if (roe_val and roe_val >= 8) else ('❌' if roe_val is not None else '待确认')
        
        results.append({
            'code': code,
            'name': tech.get('name', code),
            'price': tech.get('current_price', ''),
            'change_pct': tech.get('change_pct', ''),
            'above_ma20': tech.get('above_ma20', False),
            'above_ma60': tech.get('above_ma60', False),
            'ma20': ma.get('MA20', ''),
            'ma60': ma.get('MA60', ''),
            'ma60_dir': tech.get('ma60_direction', ''),
            'vol_score': vol.get('score', 0),
            'vol_above_ma20': vol.get('above_ma20', False),
            'breakout': vol.get('breakout_1_5x', False),
            'roe': roe_val,
            'roe_ok': roe_ok,
            'amplitude_5d': tech.get('amplitude_5d', ''),
            'auto_score': data.get('auto_score', 0),
            'auto_details': data.get('auto_details', []),
            'no_main_sell': score_input.get('no_major_outflow', None),
            'north_inflow': score_input.get('northbound_inflow', None),
        })
    except Exception as e:
        results.append({'code': code, 'name': code, 'error': str(e), 'auto_score': 0})

# 按自动得分排序
results.sort(key=lambda x: x.get('auto_score', 0) or 0, reverse=True)
print(json.dumps(results, ensure_ascii=False, indent=2))
