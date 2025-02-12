import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import time
import schedule
from modules.data.service.data_scheduler_service import DataSchedulerService
from modules.data.service.market_data_service import MarketDataService
from modules.utils.log_manager import log_manager

# 设置项目根目录
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(ROOT_DIR)

# 导入自定义模块
from modules.data.storage.database_storage import DatabaseStorage
from modules.indicators.basic.technical_indicators import TechnicalIndicators
from modules.data.collector.stock.stock_data_collector import StockDataCollector

# 获取日志实例
logger = log_manager.get_logger()

class TradingSystem:
    def __init__(self):
        from modules.data.service.stock_data_service import StockDataService
        from modules.data.service.market_data_service import MarketDataService
        
        self.stock_service = StockDataService()
        self.market_service = MarketDataService()
        self.database = DatabaseStorage()
        self.indicators = TechnicalIndicators()

    def initialize(self):
        """初始化交易系统"""
        try:
            # 确保数据库和必要的表已创建
            if not self.database.initialize():
                logger.error("数据库初始化失败")
                return False
                
            # 尝试获取股票列表以验证数据访问
            stock_list = self.market_service.get_stock_list()
            if stock_list is None:
                logger.error("无法获取股票列表")
                return False
                
            logger.info("交易系统初始化成功")
            return True
        except Exception as e:
            logger.error(f"交易系统初始化失败: {str(e)}")
            return False

    def initialize_data(self, start_date: str = None, end_date: str = None):
        """初始化历史数据
        Args:
            start_date: 开始日期，格式YYYYMMDD，默认为一年前
            end_date: 结束日期，格式YYYYMMDD，默认为当前日期
        Returns:
            bool: 是否初始化成功
        """
        return self.database.initialize_history_data(start_date, end_date)

def main():
    # 初始化数据库
    database = DatabaseStorage()
    if not database.initialize():
        logger.error("数据库初始化失败")
        sys.exit(1)
    
    # 初始化交易系统
    trading_system = TradingSystem()
    if not trading_system.initialize():
        logger.error("交易系统初始化失败")
        sys.exit(1)
    
    # 初始化历史数据
    if not trading_system.initialize_data():
        logger.error("历史数据初始化失败")
        sys.exit(1)
    
    # 初始化并启动数据调度服务
    data_scheduler = DataSchedulerService()
    if not data_scheduler.initialize():
        logger.error("数据调度服务初始化失败")
        sys.exit(1)
    
    # 设置定时任务
    data_scheduler.setup_schedule()
    
    logger.info("交易系统启动成功，开始运行定时任务...")
    
    # 运行定时任务
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.info("收到退出信号，系统正在关闭...")
            break
        except Exception as e:
            logger.error(f"定时任务执行出错: {str(e)}")
            time.sleep(60)  # 发生错误时等待一分钟后继续
    
    logger.info("系统已安全关闭")

if __name__ == "__main__":
    main()