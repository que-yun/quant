-- 创建股票基本信息表
CREATE TABLE IF NOT EXISTS stock_basic_info  (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    industry TEXT,
    total_share REAL,
    circulating_share REAL,
    market_cap REAL,
    circulating_market_cap REAL,
    pe_ratio REAL,
    pb_ratio REAL,
    ps_ratio REAL,
    pcf_ratio REAL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL,
    amplitude REAL,
    pct_change REAL,
    price_change REAL,
    turnover_rate REAL,
    update_time TEXT
);

-- 创建指数基本信息表
CREATE TABLE IF NOT EXISTS index_basic_info (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    type TEXT,
    update_time TEXT
);

INSERT OR REPLACE INTO index_basic_info (symbol, name, type, update_time)
VALUES
    ('000001', '上证综指', 'market', '2023-10-05 15:00:00'),
    ('399001', '深证成指', 'market', '2023-10-05 15:00:00'),
    ('000300', '沪深300指数', 'market', '2023-10-05 15:00:00'),
    ('000905', '中证500指数', 'market', '2023-10-05 15:00:00'),
    ('399006', '创业板指', 'market', '2023-10-05 15:00:00'),
    ('000688', '科创50指数', 'market', '2023-10-05 15:00:00'),
    ('000016', '上证50指数', 'market', '2023-10-05 15:00:00'),
    ('000852', '中证1000指数', 'market', '2023-10-05 15:00:00');

-- 创建日线数据表
CREATE TABLE IF NOT EXISTS daily_bars (
    date TEXT,
    symbol TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL,
    amplitude REAL,
    pct_change REAL,
    price_change REAL,
    turnover_rate REAL,
    update_time TEXT,
    PRIMARY KEY (date, symbol)
);

-- 创建周线数据表
CREATE TABLE IF NOT EXISTS weekly_bars (
    date TEXT,
    symbol TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL,
    amplitude REAL,
    pct_change REAL,
    price_change REAL,
    turnover_rate REAL,
    update_time TEXT,
    PRIMARY KEY (date, symbol)
);

-- 创建分钟数据表
CREATE TABLE IF NOT EXISTS minute_bars (
    date TEXT,
    symbol TEXT,
    freq TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL,
    amplitude REAL,
    pct_change REAL,
    price_change REAL,
    turnover_rate REAL,
    update_time TEXT,
    PRIMARY KEY (date, symbol, freq)
);

-- 创建指数日线数据表
CREATE TABLE IF NOT EXISTS index_daily_data (
    date TEXT,
    symbol TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL,
    amplitude REAL,
    pct_change REAL,
    price_change REAL,
    turnover_rate REAL,
    update_time TEXT,
    PRIMARY KEY (date, symbol)
);

-- 创建月线数据表
CREATE TABLE IF NOT EXISTS monthly_bars (
    date TEXT,
    symbol TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL,
    amplitude REAL,
    pct_change REAL,
    price_change REAL,
    turnover_rate REAL,
    update_time TEXT,
    PRIMARY KEY (date, symbol)
);

-- 创建策略表现表
CREATE TABLE IF NOT EXISTS strategy_performance (
    strategy_name TEXT,
    start_date TEXT,
    end_date TEXT,
    initial_capital REAL,
    final_capital REAL,
    total_return REAL,
    annual_return REAL,
    max_drawdown REAL,
    win_rate REAL,
    sharpe_ratio REAL,
    update_time TEXT,
    PRIMARY KEY (strategy_name, start_date, end_date)
);

-- 创建资金流向表
CREATE TABLE IF NOT EXISTS fund_flow (
    date TEXT,
    symbol TEXT,
    close REAL,
    pct_change REAL,
    main_net_flow REAL,
    main_net_flow_rate REAL,
    super_big_net_flow REAL,
    super_big_net_flow_rate REAL,
    big_net_flow REAL,
    big_net_flow_rate REAL,
    medium_net_flow REAL,
    medium_net_flow_rate REAL,
    small_net_flow REAL,
    small_net_flow_rate REAL,
    update_time TEXT,
    PRIMARY KEY (date, symbol)
);