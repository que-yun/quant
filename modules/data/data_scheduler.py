import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Lock

import akshare as ak
import pandas as pd
import schedule
import yaml
import sqlalchemy as sa
from loguru import logger
from sqlalchemy import text

from .data_fetcher import AStockData
from .market_data import MarketData


class DataCollector:
    def __init__(self):
        self.config = self._load_config()
        self.fetcher = AStockData()
        self.market_data = MarketData()
        self.max_retries = 3
        self.all_symbols = []
        # 确保数据表存在
        self._ensure_tables_exist()
        self.all_symbols = self._load_symbols()
        self.max_retries = self.config['data_collection']['max_retries']
        self.request_timeout = self.config['data_collection']['request_timeout']
        self.rate_limit = self.config['data_collection']['rate_limit']
        
        # 添加线程池和锁
        self.thread_pool = ThreadPoolExecutor(max_workers=20)  # 增加并发数到20
        self.db_lock = Lock()  # 数据库操作锁
    
    def _load_config(self) -> dict:
        """加载配置文件"""
        try:
            with open('config.yaml', 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            return {}

        
    def _load_symbols(self) -> list:
        """获取全市场股票代码"""
        try:
            df = ak.stock_zh_a_spot_em()
            # 过滤掉退市股票（通过检查最新价是否为空或0来判断）
            df = df[df['最新价'].notna() & (df['最新价'] != 0)]
            # 添加市场前缀
            df['代码'] = df.apply(lambda x: f"sh{x['代码']}" if x['代码'].startswith('6') else f"sz{x['代码']}", axis=1)
            logger.info(f"获取股票列表成功，共{len(df)}只股票（已过滤退市股票）")
            return df['代码'].tolist()
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []
    
    def _get_trade_dates(self) -> list:
        """获取最近N个交易日"""
        try:
            df = ak.tool_trade_date_hist_sina()
            if df is None or df.empty:
                logger.error("获取交易日历数据为空")
                return []
            
            # 确保trade_date列存在且不为空
            if 'trade_date' not in df.columns:
                logger.error("交易日历数据格式错误：缺少trade_date列")
                return []
                
            # 先将日期字符串转换为datetime对象
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='mixed')
            # 然后再转换为指定格式的字符串
            trade_dates = df['trade_date'].dt.strftime('%Y%m%d').tolist()
            if not trade_dates:
                logger.error("交易日历数据转换后为空")
                return []
                
            return trade_dates[-30:]
        except Exception as e:
            logger.error(f"获取交易日历失败: {str(e)}")
            return []
    
    def initialize_historical_data(self, start_date=None):
        """一次性获取历史数据
        Args:
            start_date: 起始日期，格式YYYYMMDD，默认为一年前
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        
        logger.info(f"开始初始化历史数据，起始日期：{start_date}")
        
        # 1. 获取股票列表
        self.all_symbols = self._load_symbols()
        if not self.all_symbols:
            logger.error("获取股票列表失败，初始化终止")
            return
        
        # 2. 获取日线数据
        logger.info("开始获取日线数据...")
        for symbol in self.all_symbols:
            try:
                self.fetcher.get_daily_data(
                    symbol=symbol
                )
                time.sleep(0.3)  # 控制请求频率
            except Exception as e:
                logger.error(f"{symbol} 日线数据获取失败: {str(e)}")
        
        # 3. 获取分钟数据（最近30天）
        logger.info("开始获取分钟数据...")
        for symbol in self.all_symbols:
            try:
                self.fetcher.get_minute_data(
                    symbol=symbol,
                    freq="5",
                    days=30
                )
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"{symbol} 分钟数据获取失败: {str(e)}")
        
        # 4. 获取高级数据
        logger.info("开始获取高级数据...")
        self.collect_advanced_data()
        
        # 5. 生成周线和月线数据
        logger.info("开始生成周线月线数据...")
        self.collect_weekly_monthly()
        
        logger.success("历史数据初始化完成")

    def collect_minute_data(self):
        """采集全市场分钟数据"""
        logger.info("开始采集分钟数据...")
        
        # 判断当前是否为交易日
        current_date = datetime.now().strftime('%Y%m%d')
        if not self.fetcher._is_trading_day(current_date):
            logger.info(f"{current_date}为非交易日，跳过数据采集")
            return
        
        # 计算请求间隔时间，确保不超过速率限制
        request_interval = 60.0 / self.rate_limit
        
        def process_symbol(symbol):
            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    # 检查数据库中是否已存在当天的分钟数据
                    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    existing_data = pd.read_sql(
                        f"SELECT date FROM minute_bars WHERE symbol='{symbol}' AND freq='5min' AND date >= '{today_start.strftime('%Y-%m-%d %H:%M:%S')}'",
                        self.fetcher.engine
                    )
                    
                    if not existing_data.empty:
                        logger.info(f"{symbol}今日的分钟数据已存在，跳过获取")
                        return True
                    
                    # 采集5分钟数据
                    data = self.fetcher.get_minute_data(
                        symbol=symbol,
                        freq="5",
                        days=1  # 仅采集当天数据
                    )
                    
                    if data is False:
                        retry_count += 1
                        if retry_count < self.max_retries:
                            logger.warning(f"{symbol} 分钟数据获取失败，第{retry_count}次重试...")
                            time.sleep(2)
                            continue
                        else:
                            logger.error(f"{symbol} 分钟数据采集失败：达到最大重试次数")
                            return False
                    
                    time.sleep(request_interval)
                    return True
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count < self.max_retries:
                        logger.warning(f"{symbol} 分钟数据采集失败: {str(e)}，第{retry_count}次重试...")
                        time.sleep(2)
                    else:
                        logger.error(f"{symbol} 分钟数据采集失败: {str(e)}，达到最大重试次数")
                        return False
        
        # 使用线程池并发处理
        futures = []
        for symbol in self.all_symbols:
            futures.append(self.thread_pool.submit(process_symbol, symbol))
        
        # 等待所有任务完成
        for future in futures:
            future.result()
        
        logger.success("分钟数据采集完成")
    
    def collect_daily_data(self):
        logger.info("开始采集日线数据... ")
        # 先获取最新的股票列表
        latest_symbols = self.market_data.get_stock_list()
        if not latest_symbols:
            logger.error("获取最新股票列表失败，跳过日线数据采集")
            return
            
        # 获取交易日历
        trade_dates = self._get_trade_dates()
        if not trade_dates:
            logger.error("未获取到有效的交易日历数据，跳过日线数据采集")
            return
            
        # 计算请求间隔时间
        request_interval = 30.0 / self.rate_limit  # 减少请求间隔时间
        
        def process_symbol(symbol):
            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    # 检查数据库中是否已存在当天的日线数据
                    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
                    start_date1 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                    end_date = datetime.now().strftime('%Y-%m-%d')
                    
                    existing_data = pd.read_sql(
                        f"SELECT date FROM daily_bars WHERE symbol='{symbol}' AND date BETWEEN '{start_date1}' AND '{end_date}'",
                        self.fetcher.engine
                    )
                    
                    # 计算时间段内的交易日总数
                    all_dates = pd.date_range(start=start_date1, end=end_date, freq='D')
                    trading_days = [d.strftime('%Y-%m-%d') for d in all_dates if self.fetcher._is_trading_day(d.strftime('%Y%m%d'))]
                    
                    if not existing_data.empty and len(existing_data) >= len(trading_days):
                        logger.info(f"{symbol}在{start_date1}至{end_date}期间的数据已存在，跳过获取")
                        return True
                    
                    # 获取日线数据
                    df = ak.stock_zh_a_hist(
                        symbol=symbol[2:] if symbol.startswith('sh') or symbol.startswith('sz') else symbol,
                        period="daily",
                        start_date=start_date.replace('-', ''),
                        end_date=end_date.replace('-', ''),
                        adjust=""
                    )
                    
                    if df is None or df.empty:
                        retry_count += 1
                        if retry_count < self.max_retries:
                            logger.warning(f"{symbol} 日线数据为空，第{retry_count}次重试...")
                            time.sleep(2)
                            continue
                        else:
                            logger.error(f"{symbol} 日线数据采集失败：数据为空")
                            return False
                    
                    # 标准化数据字段
                    df = df.rename(columns={
                        "日期": "date",
                        "股票代码": "symbol",
                        "开盘": "open",
                        "收盘": "close",
                        "最高": "high",
                        "最低": "low",
                        "成交量": "volume",
                        "成交额": "amount",
                        "振幅": "amplitude",
                        "涨跌幅": "pct_change",
                        "涨跌额": "price_change",
                        "换手率": "turnover_rate"
                    })
                    
                    # 添加symbol和更新时间
                    df["symbol"] = symbol
                    df["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    try:
                        with self.db_lock:
                            # 检查数据库中最新的数据日期
                            latest_date_query = pd.read_sql(
                                f"SELECT MAX(date) as latest_date FROM daily_bars WHERE symbol = '{symbol}'",
                                self.fetcher.engine
                            )
                            
                            if not latest_date_query.empty and latest_date_query['latest_date'].iloc[0] is not None:
                                latest_db_date = pd.to_datetime(latest_date_query['latest_date'].iloc[0])
                                df['date'] = pd.to_datetime(df['date'])
                                
                                # 检查是否有新数据
                                if df['date'].max() <= latest_db_date:
                                    logger.info(f"{symbol}的日线数据已是最新（最新日期：{latest_db_date.strftime('%Y-%m-%d')}），无需更新")
                                    return True
                                
                                # 只保留新数据
                                df = df[df['date'] > latest_db_date]
                            
                            if not df.empty:
                                # 使用分批插入方式提高性能
                                df.to_sql(
                                    "daily_bars",
                                    self.fetcher.engine,
                                    if_exists="append",
                                    index=False,
                                    method="multi",
                                    chunksize=1000  # 增加批量写入大小
                                )
                                logger.success(f"成功更新{symbol}的日线数据，新增{len(df)}条记录")
                            else:
                                logger.info(f"{symbol}没有新的日线数据需要更新")
                    except Exception as e:
                        logger.error(f"{symbol} 日线数据更新失败: {str(e)}")
                        return False
                    time.sleep(request_interval)  # 控制请求频率
                    return True
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count < self.max_retries:
                        logger.warning(f"{symbol} 日线数据采集失败: {str(e)}，第{retry_count}次重试...")
                        time.sleep(2)
                    else:
                        logger.error(f"{symbol} 日线数据采集失败: {str(e)}，达到最大重试次数")
                        return False
        
        # 使用线程池并发处理
        futures = []
        for symbol in latest_symbols:
            futures.append(self.thread_pool.submit(process_symbol, symbol))
        
        # 等待所有任务完成
        for future in futures:
            future.result()
        
        logger.success("日线数据采集完成")
    
    def _get_market_type(self, symbol: str) -> str:
        """根据symbol判断市场类型"""
        if symbol.startswith('sh688'):
            return '科创板'
        elif symbol.startswith('sh'):
            return '沪A'
        elif symbol.startswith('sz30'):
            return '创业板'
        elif symbol.startswith('sz'):
            return '深A'
        elif symbol.startswith('bj'):
            return '北交所'
        else:
            return '沪A'  # 默认值

    def collect_advanced_data(self):
        """采集资金流出数据"""
        logger.info("开始采集资金流出数据...")
        try:
            # 计算请求间隔时间
            request_interval = 60.0 / self.rate_limit
            
            def process_symbol(symbol):
                retry_count = 0
                while retry_count < self.max_retries:
                    try:
                        # 从symbol中提取市场代码
                        market = 'sh' if symbol.startswith('sh') else 'sz' if symbol.startswith('sz') else 'bj'
                        stock_code = symbol[2:]  # 去除市场前缀
                        
                        # 获取资金流数据
                        fund_flow = ak.stock_individual_fund_flow(
                            stock=stock_code,
                            market=market
                        )
                        
                        if fund_flow is None or fund_flow.empty:
                            retry_count += 1
                            if retry_count < self.max_retries:
                                logger.warning(f"{symbol} 资金流数据为空，第{retry_count}次重试...")
                                time.sleep(2)
                                continue
                            else:
                                logger.error(f"{symbol} 资金流数据采集失败：数据为空")
                                return False
                        
                        # 标准化字段名
                        fund_flow = fund_flow.rename(columns={
                            '日期': 'date',
                            '收盘价': 'close',
                            '涨跌幅': 'pct_change',
                            '主力净流入-净额': 'main_net_flow',
                            '主力净流入-净占比': 'main_net_flow_rate',
                            '超大单净流入-净额': 'super_big_net_flow',
                            '超大单净流入-净占比': 'super_big_net_flow_rate',
                            '大单净流入-净额': 'big_net_flow',
                            '大单净流入-净占比': 'big_net_flow_rate',
                            '中单净流入-净额': 'medium_net_flow',
                            '中单净流入-净占比': 'medium_net_flow_rate',
                            '小单净流入-净额': 'small_net_flow',
                            '小单净流入-净占比': 'small_net_flow_rate'
                        })
                        
                        # 添加symbol和market信息
                        fund_flow['symbol'] = symbol
                        fund_flow['market'] = market
                        fund_flow['date'] = pd.to_datetime(fund_flow['date'])
                        fund_flow['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        with self.db_lock:
                            # 去重存储
                            existing = pd.read_sql(
                                f"SELECT date FROM fund_flow WHERE symbol='{symbol}'",
                                self.fetcher.engine
                            )
                            if not existing.empty:
                                fund_flow = fund_flow[~fund_flow['date'].isin(pd.to_datetime(existing['date']))]
                            
                            if not fund_flow.empty:
                                fund_flow.to_sql(
                                    'fund_flow',
                                    self.fetcher.engine,
                                    if_exists='append',
                                    index=False
                                )
                                logger.info(f"更新资金流出数据：{symbol}")
                        
                        time.sleep(request_interval)  # 控制请求频率
                        return True
                        
                    except Exception as e:
                        retry_count += 1
                        if retry_count < self.max_retries:
                            logger.warning(f"{symbol} 资金流数据采集失败: {str(e)}，第{retry_count}次重试...")
                            time.sleep(2)
                        else:
                            logger.error(f"{symbol} 资金流数据采集失败: {str(e)}，达到最大重试次数")
                            return False
            
            # 使用线程池并发处理
            futures = []
            for symbol in self.all_symbols:
                futures.append(self.thread_pool.submit(process_symbol, symbol))
            
            # 等待所有任务完成
            for future in futures:
                future.result()
                
            logger.success("资金流出数据采集完成")
            
        except Exception as e:
            logger.error(f"资金流出数据采集失败: {str(e)}")

    def collect_weekly_monthly(self):
        """生成周线、月线数据"""
        logger.info("开始生成周线、月线数据...")
        try:
            with self.db_lock:
                # 从日线生成周线
                df = pd.read_sql("SELECT * FROM daily_bars", self.fetcher.engine)
                df['date'] = pd.to_datetime(df['date'])
                
                # 周线
                weekly = df.groupby('symbol').resample('W-Mon', on='date').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum',
                    'amount': 'sum',
                    'amplitude': 'max',
                    'pct_change': 'sum',
                    'price_change': 'sum',
                    'turnover_rate': 'sum'
                }).reset_index()
                
                # 添加更新时间
                weekly['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                weekly.to_sql(
                    "weekly_bars",
                    self.fetcher.engine,
                    if_exists="replace",
                    index=False
                )
                
                # 月线
                monthly = df.groupby('symbol').resample('ME', on='date').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum',
                    'amount': 'sum',
                    'amplitude': 'max',
                    'pct_change': 'sum',
                    'price_change': 'sum',
                    'turnover_rate': 'sum'
                }).reset_index()
                
                # 添加更新时间
                monthly['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                monthly.to_sql(
                    "monthly_bars",
                    self.fetcher.engine,
                    if_exists="replace",
                    index=False
                )
                logger.success("周线、月线数据生成完成")
        except Exception as e:
            logger.error(f"生成周月线数据失败: {str(e)}")

    def _ensure_tables_exist(self):
        """确保数据库中存在所需的数据表"""
        try:
            # 获取数据库连接
            engine = self.fetcher.engine
            conn = engine.connect()
            
            # 创建个股基本面信息表
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_basic_info (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                industry TEXT,
                market TEXT,
                total_share REAL,
                circulating_share REAL,
                market_cap REAL,
                circulating_market_cap REAL,
                pe_ratio REAL,
                pb_ratio REAL,
                ps_ratio REAL,
                pcf_ratio REAL,
                update_time TEXT
            )
            """))
            
            # 创建日线数据表
            conn.execute(text("""
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
            )
            """))
            
            # 创建周线数据表
            conn.execute(text("""
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
            )
            """))
            
            # 创建月线数据表
            conn.execute(text("""
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
            )
            """))
            
            # 创建分钟数据表
            conn.execute(text("""
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
            )
            """))
            
            # 创建策略表现表
            conn.execute(text("""
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
            )
            """))
            
            # 创建资金流出表
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fund_flow (
                date TEXT,
                symbol TEXT,
                market TEXT,
                close REAL,
                pct_change REAL,
                main_net_outflow REAL,
                main_net_outflow_rate REAL,
                super_big_net_outflow REAL,
                super_big_net_outflow_rate REAL,
                big_net_outflow REAL,
                big_net_outflow_rate REAL,
                medium_net_outflow REAL,
                medium_net_outflow_rate REAL,
                small_net_outflow REAL,
                small_net_outflow_rate REAL,
                update_time TEXT,
                PRIMARY KEY (date, symbol)
            )
            """))
            
            conn.commit()
            conn.close()
            logger.info("数据表初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"数据表初始化失败: {str(e)}")
            return False

def run_collection():
    collector = DataCollector()
    # 首次运行时初始化历史数据
    collector.initialize_historical_data('20240101')
    # 启动常规数据采集
    collector.collect_minute_data()
    collector.collect_daily_data()
    collector.collect_advanced_data()
    collector.collect_weekly_monthly()

# 定时任务配置
def parse_interval(interval: str) -> tuple:
    """解析时间间隔配置"""
    value = int(interval[:-1])
    unit = interval[-1]
    return value, unit

collector = DataCollector()
config = collector.config['data_collection']

# 根据配置设置定时任务
minute_value, _ = parse_interval(config['minute_data_interval'])
schedule.every(minute_value).minutes.do(collector.collect_minute_data)

# 日线数据采集
schedule.every().day.at("15:30").do(collector.collect_daily_data)

# 高级数据采集
advanced_value, unit = parse_interval(config['advanced_data_interval'])
if unit == 'h':
    schedule.every(advanced_value).hours.do(collector.collect_advanced_data)
else:
    schedule.every().day.do(collector.collect_advanced_data)

if __name__ == "__main__":
    logger.add("data_collect.log", rotation="1 week")
    logger.info("数据采集服务启动...")
    
    # 首次启动时立即执行一次数据采集
    logger.info("执行首次数据采集...")
    # collector.collect_minute_data()
    collector.collect_daily_data()
    collector.collect_advanced_data()
    # collector.collect_weekly_monthly()
    logger.success("首次数据采集完成")
    
    logger.info("进入定时任务循环...")
    while True:
        schedule.run_pending()
        time.sleep(1)