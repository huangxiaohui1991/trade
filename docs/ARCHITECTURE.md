# A股交易系统 · Hermes 原生架构方案

> 分支：`feature/agent-native`
> 版本：v2.1（妙想 API 集成版）
> 日期：2026-04-09

---

## 一、设计原则

**只用 Hermes**，不引入第二个 Agent。

- cron 调度、Discord 推送、舆情评分全在同一套流程里
- 东方财富妙想 API（mx-skills）作为优先数据源，akshare/新浪作为 fallback
- Obsidian 是唯一事实来源，所有数据读写都通过它

---

## 二、架构总览

```
┌─────────────────────────────────────────────┐
│             Discord（用户交互层）              │
│   定时推送报告 │ 自然语言指令 │ 舆情提醒        │
└──────────────────────────┬──────────────────┘
                           │
┌──────────────────────────▼──────────────────┐
│                 Hermes-Agent                  │
│                                             │
│   cron 调度（盘前/收盘/舆情/周报）            │
│   Discord 推送（格式化报告）                  │
│   自然语言处理（选股/评分/查询/复盘）          │
│   妙想 API 调用（mx-data/search/xuangu）     │
│   Obsidian 读写（持仓/日志/评分/配置）         │
└──────────────────────────┬──────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Obsidian    │  │  妙想 API    │  │  AKShare /   │
│  Vault       │  │  (优先数据源) │  │  新浪        │
│  （数据中枢）  │  │  mx-skills   │  │  (fallback)  │
└──────────────┘  └──────────────┘  └──────────────┘
                           │
                           ▼
                   ┌──────────────┐
                   │   A股市场     │
                   └──────────────┘
```

---

## 三、Vault 结构

```
a-stock-trading/
│
├── 00-系统/
│   ├── 仪表盘.md
│   ├── 使用指南.md
│   ├── 自动化方案.md
│   ├── 配置手册.md          # config.yaml 参数说明
│   └── 模板/
│       ├── 每日日志.md
│       ├── 周复盘.md
│       └── 选股打分.md
│
├── 01-持仓/
│   ├── portfolio.md          # 唯一事实来源（总资金/持仓/条件单）
│   └── 持仓概览.md
│
├── 02-日志/
│   ├── 2026-04-09.md        # 每日自动生成
│   └── ...
│
├── 03-复盘/
│   ├── 周/2026-W14.md        # 每周日自动生成
│   └── 月/2026-04.md
│
├── 04-选股/
│   ├── 核心池.md
│   ├── 观察池.md
│   ├── 筛选记录/             # 每次筛选结果
│   └── 评分报告/              # 每日核心池评分
│
├── 05-资料/
│   ├── 行业研究/
│   └── 公司深度/
│
├── 06-统计/
│   ├── 交易记录.md            # 所有买卖记录 + P&L
│   └── 月度表现.md
│
├── scripts/                   # Python 引擎
│   ├── engine/
│   │   ├── data_engine.py     # 数据获取（MX优先 → akshare fallback）
│   │   ├── technical.py        # 技术指标（akshare，MX fallback）
│   │   ├── financial.py        # 基本面数据（MX优先 → akshare → 新浪）
│   │   ├── flow.py            # 资金流向（MX优先 → akshare → 新浪）
│   │   ├── market_timer.py     # 大盘择时
│   │   ├── scorer.py           # 四维评分（舆情用 MX 搜索）
│   │   ├── risk_model.py       # 风控校验
│   │   └── composite.py        # 综合决策
│   ├── mx/                    # 妙想 API 集成模块
│   │   ├── client.py           # 公共基类（API Key / 请求 / 错误处理）
│   │   ├── mx_data.py          # 金融数据查询（行情/财务/关系）
│   │   ├── mx_search.py        # 资讯搜索（研报/新闻/公告）
│   │   ├── mx_xuangu.py        # 智能选股（自然语言条件筛选）
│   │   ├── mx_zixuan.py        # 自选股管理（增删查）
│   │   └── mx_moni.py          # 模拟交易（买卖/持仓/资金/撤单）
│   ├── pipeline/
│   │   ├── morning.py          # 盘前流程
│   │   ├── evening.py          # 收盘流程
│   │   ├── noon.py             # 午休检查
│   │   ├── weekly_review.py    # 周报
│   │   ├── core_pool_scoring.py # 核心池评分
│   │   └── stock_screener.py   # 选股流水线（MX智能选股优先）
│   ├── cli/                   # 命令行工具
│   └── utils/
│       ├── obsidian.py         # Obsidian 读写
│       ├── discord_push.py     # Discord 推送
│       ├── config_loader.py    # 配置读取
│       └── logger.py
│
├── .env                        # 环境变量（MX_APIKEY 等，已 gitignore）
├── config/
│   ├── strategy.yaml           # 评分/风控/仓位参数
│   ├── stocks.yaml             # 核心池/观察池/黑名单
│   └── notification.yaml       # 推送规则
│
├── data/
│   ├── prices/                 # 价格快照
│   ├── backtest/               # 回测结果
│   └── cron.log                # cron 执行日志
│
└── docs/
    ├── ARCHITECTURE.md          # 本文档
    └── 技术优化方案.md
```

---

## 四、配置文件

### config/strategy.yaml

```yaml
# 评分权重（满分10）
scoring:
  weights:
    technical: 2      # 技术信号（MA20/MA60/量能）
    fundamental: 3    # 基本面（ROE/营收/现金流）
    flow: 2           # 资金（主力/北向）
    sentiment: 3     # 舆情（TrendRadar）

  thresholds:
    buy: 7            # ≥7 可买入
    watch: 5          # ≥5 可观察池
    reject: 4         # ≤4 一票否决

  veto:               # 任一触发直接不买
    - below_ma20
    - limit_up_today
    - consecutive_outflow
    - red_market

# 风控参数
risk:
  stop_loss: 0.04     # 4% 止损
  absolute_stop: 0.07  # 7% 绝对止损
  take_profit:
    t1_pct: 0.15       # +15% 卖 1/3
    t1_drawdown: 0.05  # 回撤 5% 卖第二批
    t2_drawdown: 0.08  # 回撤 8% 清仓
  time_stop_days: 15
  position:
    total_max: 0.60    # 总仓位上限
    single_max: 0.20   # 单只上限
    weekly_max: 2      # 每周最多2笔

# 大盘择时
market_timer:
  green_days: 3        # 连续3日站上MA20 → GREEN
  red_days: 5          # 连续5日跌破MA20 → RED
  clear_days_ma60: 15  # MA60下方15日 → CLEAR

capital: 450286
```

### config/stocks.yaml

```yaml
core_pool:
  - code: 002487
    name: 大金重工
    added: 2026-04-08
  - code: 002353
    name: 杰瑞股份
    added: 2026-04-09
  - code: 300870
    name: 欧陆通
    added: 2026-04-09

watch_pool: []

blacklist:
  permanent: []
  temporary: []
```

---

## 五、Cron 任务（Hermes）

| 任务 | 时间 | 做什么 | 输出 |
|------|------|--------|------|
| 盘前摘要 | 8:25（周一~五） | 拉大盘数据 + 持仓状态 + 核心池异动 | → Discord |
| 午休检查 | 11:55（周一~五） | 持仓涨跌 + 加仓机会 + 剩余次数 | → Discord |
| 舆情监控 | 每30分钟 | TrendRadar 检查赛力斯等关键词 | → Discord（有匹配才发）|
| 收盘报告 | 15:35（周一~五） | 更新持仓价格 + 止损止盈 + 生成明日计划 | → Discord + Obsidian |
| 核心池评分 | 15:40（周一~五） | 批量评分核心池所有股 | → Obsidian |
| 周报 | 周日 20:00 | 统计本周 P&L + 胜率 + 盈亏比 | → Obsidian + Discord |

---

## 六、自然语言指令

| 你说 | Hermes 做什么 |
|------|------------|
| "今天盘前" | 立即跑盘前流程，推 Discord |
| "收盘" | 立即跑收盘流程 |
| "选股" | 调妙想智能选股 API 跑选股流水线，结果写 Obsidian |
| "评分 XXX" | 查实时价格 + 四维评分，推 Discord |
| "持仓" | 读 portfolio.md，报告当前状态 |
| "回测 XXX" | 用 backtest.py 跑，输出结果 |
| "加仓计算" | 读 calculator，输出建议 |
| "复盘" | 生成周报 |
| "帮我研究 XXX" | 妙想资讯搜索 + 金融数据查询，出报告 |
| "XXX 最新研报" | 调妙想资讯搜索，返回研报/新闻/公告 |
| "查数据 XXX" | 调妙想金融数据查询，返回行情/财务数据 |

---

## 七、每日流程详解

### 盘前（8:25 自动）

```
1. AKShare 拉上证/创业板实时数据
2. 判断 vs MA20/MA60，输出 GREEN/YELLOW/RED/CLEAR
3. 读 portfolio.md 报告持仓状态
4. 查核心池是否有异动（跌破MA20 / 主力大幅流出）
5. 格式化盘前摘要 → Discord 推送
```

### 收盘（15:35 自动）

```
1. 更新持仓最新价格（MX优先 → akshare fallback，写 portfolio.md）
2. 重算止损/止盈价（写 portfolio.md）
3. 检查是否触发止损/止盈
4. 跑 core_pool_scoring.py（MX数据+搜索 → 四维评分 → 写 Obsidian）
5. 生成明日计划（写明天的日志 MD）
6. 格式化收盘摘要 → Discord
```

### 核心池评分（15:40 自动）

```
1. 读 config/stocks.yaml 的核心池列表
2. 批量拉取：
   - 技术指标（akshare 日线历史 → 均线/成交量/动量）
   - 基本面（MX优先 → akshare → 新浪）
   - 资金流向（MX优先 → akshare → 新浪成交量估算）
   - 舆情（MX资讯搜索 → 研报/评级统计）
3. 四维评分
4. 输出到 vault/04-选股/评分报告/核心池_评分_YYYYMMDD.md
5. 更新 vault/04-选股/核心池.md 的评分列
```

---

## 八、数据流

```
          妙想 API（优先）     AKShare / 新浪（fallback）
               │                        │
               └────────┬───────────────┘
                        ▼
┌──────────────────────────────────────────┐
│           data_engine.py                  │
│   MX优先 → akshare → 新浪 · 失败日志     │
└─────────────────────┬────────────────────┘
                      │
     ┌────────────────┼────────────────┐
     ▼                ▼                ▼
┌─────────┐    ┌──────────┐    ┌────────────┐
│ scorer  │    │risk_model│    │market_timer│
│ 四维评分 │    │ 风控校验  │    │ 大盘择时   │
│(MX舆情) │    │          │    │            │
└────┬────┘    └────┬─────┘    └─────┬──────┘
     └──────────────┼────────────────┘
                    ▼
            ┌──────────────┐
            │ composite.py │
            │  综合决策     │
            │ BUY/HOLD/SELL│
            └──────┬───────┘
                   │
                   ▼
          ┌──────────────────┐
          │    Obsidian      │
          │ portfolio.md      │
          │ 核心池评分报告     │
          │ 日志 / 复盘      │
          └──────────────────┘
                   │
                   ▼
            ┌──────────────────┐
            │    Discord       │
            │ 格式化报告推送    │
            └──────────────────┘
```

### 数据源优先级

| 模块 | 优先数据源 | Fallback 1 | Fallback 2 |
|------|-----------|-----------|-----------|
| 基本面（financial.py） | 妙想 mx_data | 东财 akshare | 新浪财经 |
| 资金流向（flow.py） | 妙想 mx_data | 东财 akshare | 成交量估算 |
| 舆情评分（scorer.py） | 妙想 mx_search | — | 默认 1.5 分 |
| 智能选股（stock_screener.py） | 妙想 mx_xuangu | akshare 全市场轻筛 | — |
| 实时行情（data_engine.py） | 东财 akshare | 妙想 mx_data | 历史日线 |
| 技术指标（technical.py） | 东财 akshare | 新浪日线 | — |
| 大盘择时（market_timer.py） | 东财 akshare | — | — |

---

## 九、Discord 推送模板

### 盘前摘要（8:25）

```
📊 盘前摘要 — 2026-04-09（周四）

━━━━━━━━━━━━━━━━━━━━
🟢 大盘
━━━━━━━━━━━━━━━━━━━━
  上证: 3964.72 (+0.15%)
     vs MA20: +0.84% ✅
     vs MA60: -2.54% 🔴（15日）
  创业板: 3312.71 (+0.32%)
     vs MA20: +1.17% ✅
     vs MA60: +0.56% ✅
  🔔 GREEN

━━━━━━━━━━━━━━━━━━━━
💼 持仓
━━━━━━━━━━━━━━━━━━━━
  港股赛力斯 4500股 @ HK$80.1
  A股: 空仓

━━━━━━━━━━━━━━━━━━━━
🎯 核心池
━━━━━━━━━━━━━━━━━━━━
  杰瑞股份 8.5 ✅ | 涨停，明日观察低开
  欧陆通 7.2 ✅ | 高位震荡
  大金重工 4 ❌ | 等突破MA20

━━━━━━━━━━━━━━━━━━━━
📋 今日计划
━━━━━━━━━━━━━━━━━━━━
  本周买入: 0/2 | 可正常买入
```

### 收盘报告（15:35）

```
📈 收盘报告 — 2026-04-09（周四）

━━━━━━━━━━━━━━━━━━━━
📊 大盘
━━━━━━━━━━━━━━━━━━━━
  上证: 3958.21 (-0.16%) 🔔 GREEN
  创业板: 3308.45 (-0.13%)

━━━━━━━━━━━━━━━━━━━━
💰 持仓
━━━━━━━━━━━━━━━━━━━━
  港股赛力斯: ¥331,650（持仓中）
  A股: 空仓
  账户总值: ~¥781,936

━━━━━━━━━━━━━━━━━━━━
⚠️ 触发事项
━━━━━━━━━━━━━━━━━━━━
  无

━━━━━━━━━━━━━━━━━━━━
🎯 核心池今日评分
━━━━━━━━━━━━━━━━━━━━
  杰瑞股份 8.5 ✅ | 北美订单持续落地
  欧陆通 7.8 ✅ | 机构买入评级
  大金重工 4 ❌ | 等突破MA20

━━━━━━━━━━━━━━━━━━━━
📋 明日计划
━━━━━━━━━━━━━━━━━━━━
  🔔 GREEN，可正常买入
  关注杰瑞股份低开（< 3%），首单 ≤2.5万
  止损: ¥103.4 | 止盈1: ¥128.6
```

---

## 十、实施计划

### Phase 1：基础设施（1-2天）

- [ ] `config/strategy.yaml` + `config/stocks.yaml` 初始化
- [ ] `scripts/engine/data_engine.py` 重构（多源 fallback）
- [ ] `scripts/utils/obsidian.py` Obsidian 读写工具
- [ ] `scripts/utils/discord_push.py` 格式化推送
- [ ] Hermes cron 任务创建（盘前/收盘/舆情/周报）

### Phase 2：核心流程跑通（2-3天）

- [ ] `pipeline/morning.py` — 盘前流程
- [ ] `pipeline/evening.py` — 收盘流程
- [ ] `pipeline/core_pool_scoring.py` — 每日自动评分
- [ ] Discord 推送模板（盘前 + 收盘）
- [ ] 端到端测试

### Phase 3：高级功能（2-3天）

- [ ] `market_timer.py` — 大盘择时集成
- [ ] `composite.py` — 综合决策（评分 × 大盘 → 仓位）
- [ ] `pipeline/stock_screener.py` — 选股流水线（调 mx-skills）
- [ ] `scripts/06-统计/交易记录.md` — P&L 自动追踪
- [ ] `pipeline/weekly_review.py` — 周报生成

### Phase 4：优化闭环（持续）

- [x] 妙想 API 集成（mx-data/search/xuangu/zixuan/moni）
- [x] 数据源优先级：MX → akshare → 新浪
- [x] 舆情评分接入 MX 资讯搜索（替代 TrendRadar 默认值）
- [ ] 回测参数校准实盘参数
- [ ] 评分权重回测验证
- [ ] Discord 消息监听机制（Hermes 接收用户回复）
- [ ] 超时未确认提醒（T+1 再提醒 / T+2 异常标记）
- [ ] MX API 调用成功率监控 + 缓存机制

---

## 十一、用户闭环机制

### 条件单状态机

持仓有三种状态：

| 状态 | 说明 | Portfolio 显示 |
|------|------|---------------|
| 持仓中 | 正常持有 | 股票 + 数量 + 成本价 |
| 条件单挂出 | 止损/止盈单已挂，等待触发 | 股票 + 数量 + 条件单价 + 🔔 |
| 已成交 | 条件单触发并成交 | 现金 + 盈亏记录 |

### 用户回复格式

用户通过 Discord 回复，系统解析后更新：

```
# 止损/止盈单挂出
"止损挂了 杰瑞股份 ¥103.5"
→ 解析：股票、挂单价、类型（止损/止盈）
→ 更新 portfolio：该持仓标记为「条件单挂出中」

# 条件单触发（实际成交）
"止损触发了 杰瑞股份 成交¥103.2"
→ 解析：股票、成交价
→ 更新 portfolio：持仓→现金，计算盈亏，写入交易记录

# 取消条件单
"取消止损 杰瑞股份"
→ 解析：取消该持仓的条件单
→ 更新 portfolio：恢复为「持仓中」
```

### 每日对账流程

```
收盘报告（15:35）
   ↓
检查所有「条件单挂出中」的持仓
   ↓
如果有 → Discord 提示：「以下条件单待确认：杰瑞股份 ¥103.5（止损）」
   ↓
用户回复「止损触发了」or「取消」
   ↓
系统更新状态
```

### parser.py 需新增的消息解析

`scripts/parser.py` 需新增函数处理用户回复：

```python
def parse_user_reply(text: str) -> dict:
    """解析用户 Discord 回复

    支持格式：
      "止损挂了 {股票名} ¥{价格}"
      "止损触发了 {股票名} 成交¥{价格}"
      "取消止损 {股票名}"
      "止盈挂了 {股票名} ¥{价格}"
      "止盈触发了 {股票名} 成交¥{价格}"
      "取消止盈 {股票名}"

    返回: {"action": "挂单"|"触发"|"取消", "type": "止损"|"止盈",
           "stock": "股票名", "price": float, "filled_price": float}
    """
    pass  # TODO: 实现
```

### 11.4 Discord 消息监听

Hermes 通过 Discord Webhook 接收用户消息：

```
Discord 用户发送消息 @Hermes
       ↓
Hermes 监听频道新消息
       ↓
判断消息类型：
  ├── @Hermes 开头的指令 → 执行对应 pipeline
  └── 普通消息 → 检查是否匹配条件单回复格式
                → 是 → parser.parse_user_reply() → 更新 portfolio
                → 否 → 忽略或回复"未识别，请使用格式：止损挂了..."
```

### 11.5 超时未确认处理

| 场景 | 处理方式 |
|------|---------|
| 条件单已挂出（T日），次交易日 9:15（T+1）未确认 | Discord 再提醒一次 |
| 条件单已触发，T+2 日 15:00 前未确认成交结果 | 标记「异常」，需人工介入确认 |
