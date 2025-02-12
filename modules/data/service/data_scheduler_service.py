from datetime import datetime, timedelta
from typing import Dict, Any
import time
import schedule
import pandas as pd
from loguru import logger
from ..collector.stock.stock_data_collector import StockDataCollector
from ..collector.market.market_data_collector import MarketDataCollector
from ..storage.database_storage import DatabaseStorage
from ...utils.log_manager import log_manager


class DataSchedulerService:
    """数据调度服务，负责协调数据采集和存储"""

    def __init__(self):
        self.stock_collector = StockDataCollector()
        self.market_collector = MarketDataCollector()
        self.storage = DatabaseStorage()
        self.logger = logger
        self.config = None

    def initialize(self) -> bool:
        """初始化数据服务"""
        try:
            # 初始化数据库
            if not self.storage.initialize():
                self.logger.error("数据库初始化失败")
                return False

            # 验证数据采集器
            if not self._validate_collectors():
                self.logger.error("数据采集器验证失败")
                return False

            # 获取配置信息
            from modules.data.service.market_data_service import MarketDataService
            market_service = MarketDataService()
            self.config = market_service.get_config()
            if not self.config:
                self.logger.error("无法获取配置信息")
                return False

            self.logger.info("数据调度服务初始化成功")
            return True

        except Exception as e:
            self.logger.error(f"数据调度服务初始化失败: {str(e)}")
            return False

    def _validate_collectors(self) -> bool:
        """验证数据采集器"""
        try:
            # 尝试获取股票列表
            data = self.market_collector.collect(data_type='stock_list')
            if not data or not self.market_collector.validate(data):
                return False

            return True

        except Exception as e:
            self.logger.error(f"验证数据采集器失败: {str(e)}")
            return False

    def _is_trading_day(self) -> bool:
        """判断当前是否为交易日"""
        try:
            from modules.data.service.market_data_service import MarketDataService
            market_service = MarketDataService()
            current_date = datetime.now().strftime('%Y%m%d')
            return market_service.is_trading_day(current_date)
        except Exception as e:
            self.logger.error(f"判断交易日失败: {str(e)}")
            return False

    def _is_trading_time(self) -> bool:
        """判断当前是否在交易时间内"""
        try:
            if not self.config:
                return False

            now = datetime.now().time()
            morning_start = datetime.strptime(self.config['market']['trading_hours']['morning']['start'], '%H:%M').time()
            morning_end = datetime.strptime(self.config['market']['trading_hours']['morning']['end'], '%H:%M').time()
            afternoon_start = datetime.strptime(self.config['market']['trading_hours']['afternoon']['start'], '%H:%M').time()
            afternoon_end = datetime.strptime(self.config['market']['trading_hours']['afternoon']['end'], '%H:%M').time()

            return (morning_start <= now <= morning_end) or (afternoon_start <= now <= afternoon_end)
        except Exception as e:
            self.logger.error(f"判断交易时间失败: {str(e)}")
            return False

    def collect_minute_data(self):
        """采集分钟级数据"""
        if not self._is_trading_day() or not self._is_trading_time():
            return

        try:
            from modules.data.service.market_data_service import MarketDataService
            market_service = MarketDataService()
            stock_list = market_service.get_stock_list()
            if stock_list is None or stock_list.empty:
                self.logger.error("获取股票列表失败")
                return

            for _, row in stock_list.iterrows():
                symbol = row['symbol']
                self.schedule_stock_data_collection(
                    symbol=symbol,
                    freq='5min',  # 5分钟级别数据
                    start_date=datetime.now().strftime('%Y%m%d'),
                    end_date=datetime.now().strftime('%Y%m%d')
                )
                time.sleep(0.1)  # 控制请求频率
        except Exception as e:
            self.logger.error(f"采集分钟级数据失败: {str(e)}")

    def collect_daily_data(self):
        """采集日线数据"""
        if not self._is_trading_day():
            return

        try:
            from modules.data.service.market_data_service import MarketDataService
            market_service = MarketDataService()
            stock_list = market_service.get_stock_list()
            if stock_list is None or stock_list.empty:
                self.logger.error("获取股票列表失败")
                return

            for _, row in stock_list.iterrows():
                symbol = row['symbol']
                self.schedule_stock_data_collection(
                    symbol=symbol,
                    freq='D',
                    start_date=(datetime.now() - timedelta(days=5)).strftime('%Y%m%d'),  # 获取最近5天数据
                    end_date=datetime.now().strftime('%Y%m%d')
                )
                time.sleep(0.1)  # 控制请求频率
        except Exception as e:
            self.logger.error(f"采集日线数据失败: {str(e)}")

    def collect_weekly_data(self):
        """采集周线数据"""
        try:
            from modules.data.service.market_data_service import MarketDataService
            market_service = MarketDataService()
            stock_list = market_service.get_stock_list()
            if stock_list is None or stock_list.empty:
                self.logger.error("获取股票列表失败")
                return

            for _, row in stock_list.iterrows():
                symbol = row['symbol']
                self.schedule_stock_data_collection(
                    symbol=symbol,
                    freq='W',
                    start_date=(datetime.now() - timedelta(days=30)).strftime('%Y%m%d'),  # 获取最近一个月数据
                    end_date=datetime.now().strftime('%Y%m%d')
                )
                time.sleep(0.1)  # 控制请求频率
        except Exception as e:
            self.logger.error(f"采集周线数据失败: {str(e)}")

    def collect_monthly_data(self):
        """采集月线数据"""
        try:
            from modules.data.service.market_data_service import MarketDataService
            market_service = MarketDataService()
            stock_list = market_service.get_stock_list()
            if stock_list is None or stock_list.empty:
                self.logger.error("获取股票列表失败")
                return

            for _, row in stock_list.iterrows():
                symbol = row['symbol']
                self.schedule_stock_data_collection(
                    symbol=symbol,
                    freq='M',
                    start_date=(datetime.now() - timedelta(days=90)).strftime('%Y%m%d'),  # 获取最近三个月数据
                    end_date=datetime.now().strftime('%Y%m%d')
                )
                time.sleep(0.1)  # 控制请求频率
        except Exception as e:
            self.logger.error(f"采集月线数据失败: {str(e)}")

    def setup_schedule(self):
        """设置定时任务"""
        # 交易时段内每5分钟采集一次分钟级数据
        schedule.every(5).minutes.do(self.collect_minute_data)

        # 每个交易日16:00采集日线数据
        schedule.every().day.at("16:00").do(self.collect_daily_data)

        # 每周五16:30采集周线数据
        schedule.every().friday.at("16:30").do(self.collect_weekly_data)

        # 每月最后一个交易日17:00采集月线数据
        schedule.every().day.at("17:00").do(self.collect_monthly_data)

        self.logger.info("定时任务设置完成")

    def schedule_stock_data_collection(self, symbol: str, start_date: str = None, end_date: str = None,
                                       freq: str = 'D') -> bool:
        """调度股票数据采集
        Args:
            symbol: 股票代码
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            freq: 数据频率，D-日线，W-周线，M-月线，1min/5min/15min/30min/60min-分钟线
        Returns:
            bool: 是否成功
        """
        try:
            # 采集数据
            data = self.stock_collector.collect(
                data_type='daily' if freq in ['D', 'W', 'M'] else 'minute',
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                freq=freq
            )

            if not data or not self.stock_collector.validate(data):
                self.logger.error(f"采集{symbol}的{freq}周期数据失败")
                return False

            # 保存数据
            return self.storage.save_stock_data(symbol, data['data'], freq)

        except Exception as e:
            self.logger.error(f"调度股票数据采集失败: {str(e)}")
            return False

    def schedule_market_data_collection(self) -> bool:
        """调度市场数据采集"""
        try:
            # 采集股票列表
            data = self.market_collector.collect(data_type='stock_list')
            if not data or not self.market_collector.validate(data):
                self.logger.error("采集市场数据失败")
                return False

            # 更新每只股票的基本信息
            for _, row in data['data'].iterrows():
                symbol = row['symbol']
                stock_info = {
                    'symbol': symbol,
                    'name': row['name'],
                    'total_shares': row.get('total_shares', None),
                    'circulating_shares': row.get('circulating_shares', None),
                    'market_cap': row.get('market_cap', None),
                    'circulating_market_cap': row.get('circulating_market_cap', None),
                    'pe_ratio': row.get('pe_ratio', None),
                    'pb_ratio': row.get('pb_ratio', None),
                    'industry': row.get('industry', None),
                    'region': row.get('region', None),
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                # 保存股票基本信息
                if not self._save_stock_info(stock_info):
                    self.logger.warning(f"保存{symbol}的基本信息失败")

            return True

        except Exception as e:
            self.logger.error(f"调度市场数据采集失败: {str(e)}")
            return False

    def _save_stock_info(self, stock_info: Dict[str, Any]) -> bool:
        """保存股票基本信息
        Args:
            stock_info: 股票基本信息
        Returns:
            bool: 是否成功
        """
        try:
            # 检查是否已存在该股票的记录
            try:
                existing_df = pd.read_sql(f"SELECT * FROM stock_basic_info WHERE symbol = '{stock_info['symbol']}'" , self.storage.engine)
                if not existing_df.empty:
                    # 如果记录已存在，则跳过更新
                    self.logger.info(f"股票{stock_info['symbol']}的基本信息已存在，跳过更新")
                    return True

                # 保存新记录
                df = pd.DataFrame([stock_info])
                df.to_sql('stock_basic_info', self.storage.engine, if_exists='append', index=False)
                self.logger.info(f"成功保存股票{stock_info['symbol']}的基本信息")
                return True

            except Exception as e:
                self.logger.warning(f"检查或保存记录失败: {str(e)}")
                return False

        except Exception as e:
            self.logger.error(f"保存股票基本信息失败: {str(e)}")
            return False
