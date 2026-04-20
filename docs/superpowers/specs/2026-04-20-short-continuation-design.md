# 短线强势续涨评分系统设计

## 1. 背景与目标

当前 Hermes 的评分体系位于 [src/hermes/strategy/scorer.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/strategy/scorer.py:1)，属于面向通用候选池的四维综合评分。该体系覆盖技术面、基本面、资金流和舆情，适合中频筛选和波段风格判断，但不适合回答以下核心问题：

- 哪些股票在 `T+1` 到 `T+3` 最可能延续强势
- 哪些“昨天看起来很强”的股票次日其实容易分歧、回落或不可执行
- 在胜率优先前提下，如何把每日出手收缩到极少数高置信标的

本设计的目标是新增一套独立于现有综合评分的 `short_continuation_v1` 策略，用于服务以下场景：

- 市场：A 股
- 风格：短线强势延续
- 持有窗口：`1-3` 个交易日
- 优先级：胜率优先，收益弹性次之
- 股票池：不预设大中小盘限制，但通过可执行性和质量过滤控制风险

该策略不替换现有综合评分，而是作为并行研究与验证能力落地，在验证通过后再考虑接入候选池排序、自动交易或独立 pipeline。

## 2. 设计原则

### 2.1 目标纯化

新的评分系统不再追求“综合优秀”，而是只回答一个问题：`这只股票在未来 1-3 天是否更可能继续强势`。因此：

- 不以中线基本面优势作为主要加分来源
- 不以慢周期趋势因子作为主要 alpha 来源
- 不为了提高覆盖面而牺牲胜率
- 不为了保持交易频率而放宽筛选

### 2.2 两段式决策

策略分为两个阶段：

1. `资格筛选`
   先过滤掉最容易导致次日失真、闷杀、追高失败或不可执行的票。
2. `续涨评分`
   只在合格样本上做短线续涨概率排序，并通过过热惩罚控制尾部风险。

### 2.3 研究先于执行

第一阶段只建设研究和验证能力，不直接改写现有 [src/hermes/pipeline/scoring.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/pipeline/scoring.py:1) 或 [src/hermes/pipeline/auto_trade.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/pipeline/auto_trade.py:1)。只有在以下条件满足后，才进入交易回测和执行接入：

- 高分层对 `T+1/T+2/T+3` 胜率有显著区分力
- 每日 `Top 1-3` 明显优于更宽的 `Top 5-10`
- 不同买入口径下结果不完全塌陷

## 3. 现状问题

### 3.1 当前评分与目标错位

现有 [src/hermes/strategy/scorer.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/strategy/scorer.py:1) 有以下特征：

- 技术因子偏中频：`golden_cross`、`ma20/ma60`、`momentum_5d`
- 基本面和舆情权重较高
- veto 更偏中线排雷，不够偏短线质量筛选
- 输出总分适合“综合够好就买”，不适合“短线形态特别对才买”

这会导致系统容易选出“看起来不错的股票”，但不一定是“次日最容易继续强”的股票。

### 3.2 当前决策层过于统一

现有 [src/hermes/strategy/decider.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/strategy/decider.py:1) 基于统一阈值决定 `BUY/WATCH/CLEAR`。该模式默认“总分足够高即可买入”，不区分策略风格，也没有对短线过热、不可执行、过度加速等问题做专门压制，不符合胜率优先的短线续涨思路。

## 4. 目标策略结构

### 4.1 阶段一：资格筛选

资格筛选的目标不是预先限制市值风格，而是过滤掉“表面强、实际质量差”的样本。第一版建议包含以下硬条件：

- `amount_min`
  设定成交额下限，避免流动性不足带来的不可执行性和高噪声。
- `change_pct_min`
  当日必须有足够强度，排除弱反抽或普通震荡阳线。
- `close_near_high_min`
  收盘位置必须足够高，避免冲高回落型假强势。
- `max_intraday_retrace`
  日内回撤不能过深，避免炸板式或承接差的形态。
- `not_limit_up_locked`
  排除一字、封死涨停或难以成交的极端形态。
- `above_ma5_or_ma10`
  至少短线结构未破坏。
- `volume_ratio_range`
  要有量能确认，但排除异常衰竭天量。
- `exclude_long_upper_shadow`
  明显长上影样本直接过滤。

资格筛选是二元结果：`qualified=true/false`。未通过资格筛选的样本不进入后续排序，也不参与交易回测。

### 4.2 阶段二：续涨评分

续涨评分只在 `qualified=true` 的样本上进行，第一版维度如下。

#### `strength_score`

衡量当前强度是否足够明确，建议包含：

- 当日涨幅 `change_pct_1d`
- 近两日累计涨幅 `change_pct_2d`
- 收盘位置 `close_near_high`
- 实体占比 `body_ratio`

该分数解决“够不够强”的问题。

#### `continuity_score`

衡量强势是否连续，而不是单日脉冲，建议包含：

- 连续两日或三日强势延续
- 是否持续站上短均线
- 低点是否抬高
- 连阳一致性

该分数解决“是不是在走趋势延续”的问题。

#### `quality_score`

衡量上涨质量与承接，而非只看涨幅，建议包含：

- 日内回撤深度 `intraday_retrace`
- 上影线比例
- 尾盘是否维持强势
- 放量上攻是否协调

该分数解决“强得有没有质量”的问题。

#### `flow_score`

保留短线资金承接确认，但显著降权，仅作为辅助项。可复用现有短线资金流字段，例如主力净流入方向、北向资金方向、短期净流入状态。

该分数解决“强势是否有人接”的问题。

#### `stability_score`

衡量次日更容易活下来的结构，建议包含：

- 波动是否失真
- 是否出现连续过度加速
- 是否接近短线衰竭结构

该分数解决“这票能不能稳住”的问题。

#### `overheat_penalty`

这是胜率优先场景下的关键组件。建议在总分之外单独建模，用于集中扣分。触发场景包括：

- 单日涨幅过大
- 量比过大
- 乖离过大
- 连续加速
- 长上影
- 尾盘偷拉

该惩罚项解决“太强但已经过热”的问题。

### 4.3 最终排序与出手逻辑

第一版最终排序逻辑：

- 只对 `qualified=true` 的样本计算 `continuation_score`
- `continuation_score = strength + continuity + quality + flow + stability - overheat_penalty`
- 每日只保留 `Top 1-3`
- 即使总分达标，只要 `overheat_penalty` 超阈值，也降级为 `WATCH`
- 大盘弱势时不放宽标准，宁可不出手

这意味着系统会主动接受“今天没有足够好的短线续涨机会”的结果。

## 5. 因子范围与减法策略

### 5.1 第一版建议先上的 8 个具体因子

第一版不追求覆盖所有形态，只上最容易验证、最贴近目标的核心因子：

- `change_pct_1d`
- `change_pct_2d`
- `close_near_high`
- `intraday_retrace`
- `body_ratio`
- `volume_ratio`
- `above_ma5`
- `overheat_penalty`

这批因子的共同目标是筛出：

- 收得高
- 回撤浅
- 有量但不过热
- 结构没坏

### 5.2 第一版应主动降权或移出主评分的因子

以下内容可保留在研究输出、风险备注或展示层，但不应继续主导 `1-3` 天短线续涨排序：

- `fundamental_score`
- `sentiment_score`
- `ma20/ma60` 相关慢趋势主加分
- `golden_cross` 作为主触发信号

原因不是这些因子完全无效，而是它们与“次日继续强”的相关性弱于短线结构质量。

## 6. 系统模块设计

建议在现有策略体系旁边平行增加短线续涨子模块，避免污染当前综合评分。

### 6.1 新增模块

- `src/hermes/strategy/continuation_models.py`
  定义资格筛选结果、续涨维度分数、惩罚项、最终评分结果等模型。

- `src/hermes/strategy/continuation_filters.py`
  封装资格筛选逻辑与硬过滤规则。

- `src/hermes/strategy/continuation_scorer.py`
  负责对合格样本计算各维度分数和总分。

- `src/hermes/research/continuation_validation.py`
  负责历史样本验证、分层分析、因子拆解和输出报告。

- `src/hermes/backtest/continuation_backtest.py`
  在验证通过后，用真实可执行买入口径回测 `Top N` 样本的交易表现。

### 6.2 复用现有模块

以下模块应优先复用：

- [src/hermes/market/models.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/market/models.py:1)
- [src/hermes/backtest/engine.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/backtest/engine.py:1)
- [src/hermes/platform/config.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/platform/config.py:1)
- [src/hermes/platform/cli.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/platform/cli.py:1)

### 6.3 与现有 pipeline 的关系

第一阶段不直接改动：

- [src/hermes/pipeline/scoring.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/pipeline/scoring.py:1)
- [src/hermes/pipeline/auto_trade.py](/Users/huangxiaohui/Documents/workspace/trade/src/hermes/pipeline/auto_trade.py:1)

接入顺序必须是：

1. 研究验证
2. 交易回测
3. 候选输出或独立策略 pipeline
4. 模拟盘执行

## 7. 验证设计

### 7.1 验证目标

验证的第一目标不是收益曲线，而是 `评分是否真的有排序能力`。需要先证明高分样本在 `T+1/T+2/T+3` 上显著优于中低分样本。

### 7.2 报告输出

第一版验证脚本应固定输出以下结果。

#### `score_bucket_report`

按分数分层，输出每层的：

- 样本数
- `T+1` 胜率
- `T+2` 胜率
- `T+3` 胜率
- 平均收益
- 中位数收益
- 最大回撤

#### `top_n_report`

分别比较每日 `Top 1`、`Top 2`、`Top 3`、`Top 5` 的表现，确认最合适的出手密度。

#### `factor_ablation_report`

逐个移除因子，比较分层效果是否恶化，用于确认哪些因子是真有效，哪些只是看起来合理。

#### `market_regime_report`

按市场环境拆分表现，至少区分：

- 强趋势市
- 震荡市
- 退潮市

这一步用于确认策略是否依赖特定市场 regime。

#### `execution_report`

同一套高分样本需要用不同口径比较：

- 次日开盘买
- 次日开盘后 30 分钟均价买
- 限制不追高超过阈值后再买

这一步用于验证纸面 alpha 是否具备真实可执行性。

### 7.3 第一版通过标准

进入下一阶段前，至少满足以下标准：

- `Top` 分层的 `T+1` 胜率显著高于中低分层
- 每日 `Top 1-3` 明显优于更宽的 `Top 5-10`
- 更换执行口径后结果不完全塌陷

若上述标准未满足，不应继续调仓位和卖点，而应回到评分因子层重新设计。

## 8. 参数策略

### 8.1 先固定的约束

第一版建议直接固定以下约束，避免无边界调参：

- `above_ma5 = true`
- `not_limit_up_locked = true`
- `exclude_long_upper_shadow = true`
- `top_n <= 3`
- `holding_days in {1, 2, 3}`

### 8.2 允许实验的关键参数

第一版只开放少量高价值参数做网格测试：

- `amount_min`
- `close_near_high_min`
- `volume_ratio` 上下界
- `overheat_penalty` 触发条件
- `entry_score_threshold`

这样可以把实验空间控制在可解释范围内，降低过拟合概率。

## 9. 错误处理与风控边界

### 9.1 数据缺失

若关键输入字段缺失，例如：

- 当日 OHLC 缺失
- 成交额缺失
- 量比无法计算
- 短均线无法计算

则该样本直接视为 `unqualified`，而不是用默认值硬补后继续排序。

### 9.2 过热与不可执行优先于高分

若样本满足强度条件但存在明显过热、封板不可买、长上影衰竭等问题，应优先过滤或降级，而不是因为涨幅高继续保留。

### 9.3 与现有系统隔离

在研究验证阶段，新的短线续涨模块只输出研究结果，不回写现有候选池、不影响现有策略排序，也不改变模拟盘行为。

## 10. 测试策略

第一版实现应至少覆盖以下测试类型：

- `continuation_filters` 单元测试
  验证资格筛选各阈值与边界样本。

- `continuation_scorer` 单元测试
  验证各维度分数、总分和惩罚项计算。

- `continuation_validation` 集成测试
  验证分层统计、Top N 报告和执行口径报告生成。

- `parameter regression tests`
  验证关键参数变化不会破坏基本排序逻辑。

测试重点不在于追求覆盖率数字，而在于保证：

- 排序规则稳定
- 过滤规则明确
- 报告结果可复现

## 11. 实施顺序

建议按以下顺序推进：

1. 新增研究模型与验证脚手架
2. 落地第一批 `6-8` 个核心因子
3. 固定大部分阈值，仅开放少量关键参数实验
4. 验证排序能力，再验证交易可执行性
5. 通过后新增独立回测入口
6. 最后再讨论接入候选池、日报或模拟盘

## 12. 非目标

本设计第一阶段不解决以下问题：

- 多策略组合优化
- 实盘撮合细节优化
- 高频盘口级别择时
- 行业轮动和题材识别系统化建模
- 重写现有综合评分框架

这些方向可能在后续迭代中有价值，但不属于当前“提升短线强势续涨胜率”的最短路径。

## 13. 结论

当前项目若以收益提升为第一优先级，且问题定位为“高分票在 `T+1` 到 `T+3` 没有足够预测力”，则最合理的迭代方向不是继续调现有综合评分，而是并行建设一套专用于 `1-3` 天窗口的短线强势续涨评分系统。

这套系统的核心不是预测“谁总体更好”，而是预测“谁明天更容易继续强”。为此，必须：

- 从综合评分转向短线资格筛选 + 续涨排序
- 从堆因子转向少量高价值因子验证
- 从直接交易回测转向先证明分层有效性
- 从追求出手频率转向高置信收缩出手

在验证框架证明其具备稳定区分力前，不应接入现有自动交易流程。
