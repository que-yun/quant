# 量化交易系统数据库结构说明

## 概述
本文档详细说明了量化交易系统中各个数据表的结构和用途，包括表的功能说明、字段定义及其含义。

## 数据表列表

### 1. stock_basic_info（个股基本面信息表）
存储个股的基本面信息，包括股票代码、名称、股价、市值等基础数据。

| 字段名 | 类型 | 说明 | 备注 |
|--------|------|------|------|
| symbol | TEXT | 股票代码 | 主键，格式：sh600000/sz000001 |
| name | TEXT | 股票名称 | |
| industry | TEXT | 所属行业 | |
| market | TEXT | 市场类型 | sh（上海）/sz（深圳）|
| total_share | REAL | 总股本 | 单位：万股 |
| circulating_share | REAL | 流通股本 | 单位：万股 |
| market_cap | REAL | 总市值 | 单位：万元 |
| circulating_market_cap | REAL | 流通市值 | 单位：万元 |
| pe_ratio | REAL | 市盈率 | |
| pb_ratio | REAL | 市净率 | |
| ps_ratio | REAL | 市销率 | |
| pcf_ratio | REAL | 市现率 | |
| update_time | TEXT | 数据更新时间 | 格式：YYYY-MM-DD HH:MM:SS |

### 2. daily_bars（股票日线数据表）
存储股票的日线级别交易数据。

| 字段名 | 类型 | 说明 | 备注 |
|--------|------|------|------|
| date | TEXT | 交易日期 | 主键，格式：YYYY-MM-DD |
| symbol | TEXT | 股票代码 | 主键，格式：sh600000/sz000001 |
| open | REAL | 开盘价 | 单位：元 |
| high | REAL | 最高价 | 单位：元 |
| low | REAL | 最低价 | 单位：元 |
| close | REAL | 收盘价 | 单位：元 |
| volume | REAL | 成交量 | 单位：手 |
| amount | REAL | 成交额 | 单位：元 |
| amplitude | REAL | 振幅 | 单位：% |
| pct_change | REAL | 涨跌幅 | 单位：% |
| price_change | REAL | 涨跌额 | 单位：元 |
| turnover_rate | REAL | 换手率 | 单位：% |
| update_time | TEXT | 数据更新时间 | 格式：YYYY-MM-DD HH:MM:SS |

### 3. weekly_bars（股票周线数据表）
存储股票的周线级别交易数据。

| 字段名 | 类型 | 说明 | 备注 |
|--------|------|------|------|
| date | TEXT | 交易日期 | 主键，格式：YYYY-MM-DD |
| symbol | TEXT | 股票代码 | 主键，格式：sh600000/sz000001 |
| open | REAL | 开盘价 | 单位：元 |
| high | REAL | 最高价 | 单位：元 |
| low | REAL | 最低价 | 单位：元 |
| close | REAL | 收盘价 | 单位：元 |
| volume | REAL | 成交量 | 单位：手 |
| amount | REAL | 成交额 | 单位：元 |
| amplitude | REAL | 振幅 | 单位：% |
| pct_change | REAL | 涨跌幅 | 单位：% |
| price_change | REAL | 涨跌额 | 单位：元 |
| turnover_rate | REAL | 换手率 | 单位：% |
| update_time | TEXT | 数据更新时间 | 格式：YYYY-MM-DD HH:MM:SS |

### 4. monthly_bars（股票月线数据表）
存储股票的月线级别交易数据。

| 字段名 | 类型 | 说明 | 备注 |
|--------|------|------|------|
| date | TEXT | 交易日期 | 主键，格式：YYYY-MM-DD |
| symbol | TEXT | 股票代码 | 主键，格式：sh600000/sz000001 |
| open | REAL | 开盘价 | 单位：元 |
| high | REAL | 最高价 | 单位：元 |
| low | REAL | 最低价 | 单位：元 |
| close | REAL | 收盘价 | 单位：元 |
| volume | REAL | 成交量 | 单位：手 |
| amount | REAL | 成交额 | 单位：元 |
| amplitude | REAL | 振幅 | 单位：% |
| pct_change | REAL | 涨跌幅 | 单位：% |
| price_change | REAL | 涨跌额 | 单位：元 |
| turnover_rate | REAL | 换手率 | 单位：% |
| update_time | TEXT | 数据更新时间 | 格式：YYYY-MM-DD HH:MM:SS |

### 5. minute_bars（股票分钟数据表）
存储股票的分钟级别交易数据。

| 字段名 | 类型 | 说明 | 备注 |
|--------|------|------|------|
| date | TEXT | 交易时间 | 主键，格式：YYYY-MM-DD HH:MM:SS |
| symbol | TEXT | 股票代码 | 主键，格式：sh600000/sz000001 |
| freq | TEXT | 分钟频率 | 主键，如：1min/5min/15min/30min/60min |
| open | REAL | 开盘价 | 单位：元 |
| high | REAL | 最高价 | 单位：元 |
| low | REAL | 最低价 | 单位：元 |
| close | REAL | 收盘价 | 单位：元 |
| volume | REAL | 成交量 | 单位：手 |
| amount | REAL | 成交额 | 单位：元 |
| amplitude | REAL | 振幅 | 单位：% |
| pct_change | REAL | 涨跌幅 | 单位：% |
| price_change | REAL | 涨跌额 | 单位：元 |
| turnover_rate | REAL | 换手率 | 单位：% |
| update_time | TEXT | 数据更新时间 | 格式：YYYY-MM-DD HH:MM:SS |

### 6. strategy_performance（策略表现表）
记录策略回测或实盘交易的表现指标。

| 字段名 | 类型 | 说明 | 备注 |
|--------|------|------|------|
| strategy_name | TEXT | 策略名称 | 主键 |
| start_date | TEXT | 回测开始日期 | 主键，格式：YYYY-MM-DD |
| end_date | TEXT | 回测结束日期 | 主键，格式：YYYY-MM-DD |
| initial_capital | REAL | 初始资金 | 单位：元 |
| final_capital | REAL | 最终资金 | 单位：元 |
| total_return | REAL | 总收益率 | 单位：% |
| annual_return | REAL | 年化收益率 | 单位：% |
| max_drawdown | REAL | 最大回撤 | 单位：% |
| win_rate | REAL | 胜率 | 单位：% |
| sharpe_ratio | REAL | 夏普比率 | 无单位 |
| update_time | TEXT | 数据更新时间 | 格式：YYYY-MM-DD HH:MM:SS |

### 7. fund_flow（个股资金流向表）
记录股票的资金流向数据。

| 字段名 | 类型 | 说明 | 备注 |
|--------|------|------|------|
| date | TEXT | 交易日期 | 主键，格式：YYYY-MM-DD |
| symbol | TEXT | 股票代码 | 主键，格式：sh600000/sz000001 |
| market | TEXT | 市场类型 | sh（上海）/sz（深圳）|
| close | REAL | 收盘价 | 单位：元 |
| pct_change | REAL | 涨跌幅 | 单位：% |
| main_net_flow | REAL | 主力净流入金额 | 单位：元，正值表示流入，负值表示流出 |
| main_net_flow_rate | REAL | 主力净流入占比 | 单位：%，正值表示流入，负值表示流出 |
| super_big_net_flow | REAL | 超大单净流入金额 | 单位：元，正值表示流入，负值表示流出 |
| super_big_net_flow_rate | REAL | 超大单净流入占比 | 单位：%，正值表示流入，负值表示流出 |
| big_net_flow | REAL | 大单净流入金额 | 单位：元，正值表示流入，负值表示流出 |
| big_net_flow_rate | REAL | 大单净流入占比 | 单位：%，正值表示流入，负值表示流出 |
| medium_net_flow | REAL | 中单净流入金额 | 单位：元，正值表示流入，负值表示流出 |
| medium_net_flow_rate | REAL | 中单净流入占比 | 单位：%，正值表示流入，负值表示流出 |
| small_net_flow | REAL | 小单净流入金额 | 单位：元，正值表示流入，负值表示流出 |
| small_net_flow_rate | REAL | 小单净流入占比 | 单位：%，正值表示流入，负值表示流出 |
| update_time | TEXT | 数据更新时间 | 格式：YYYY-MM-DD HH:MM:SS |

## 注意事项
1. 所有日期时间字段均采用文本格式存储，使用标准的格式便于查询和处理
2. 金额相关的字段均使用REAL类型，保留小数点后4位
3. 比率类字段（如涨跌幅、换手率等）使用REAL类型，实际值为百分比
4. 每个表都包含update_time字段，记录数据的最后更新时间
5. 主键组合确保数据的唯一性，防止重复记录