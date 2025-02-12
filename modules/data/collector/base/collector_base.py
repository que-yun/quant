from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime

import pandas as pd
from loguru import logger
from ...storage.db_pool import DatabasePool

class CollectorBase(ABC):
    """增强版数据采集器基类，整合了数据库连接、缓存、验证等通用功能"""
    
    _instance = {}
    _initialized = {}
    
    def __new__(cls):
        if cls not in cls._instance:
            cls._instance[cls] = super(CollectorBase, cls).__new__(cls)
            cls._initialized[cls] = False
        return cls._instance[cls]
    
    def __init__(self):
        if not self._initialized.get(self.__class__):
            self.name = self.__class__.__name__
            self.logger = logger
            self.db_pool = DatabasePool()
            self.engine = self.db_pool.get_engine()
            self._initialize_database()
            self._initialized[self.__class__] = True
    
    def _initialize_database(self):
        """初始化数据库连接"""
        try:
            from config.config_manager import ConfigManager
            from modules.data.storage.database_storage import DatabaseStorage
            
            config = ConfigManager()
            self.db_path = config.database_path
            
            # 初始化数据表
            storage = DatabaseStorage()
            if not storage.initialize():
                raise Exception("数据库初始化失败")
                
        except Exception as e:
            self.logger.error(f"数据库初始化失败: {str(e)}")
            raise
    
    def _check_data_exists(self, symbol: str, table_name: str, start_date: str = None, end_date: str = None, freq: str = None) -> bool:
        """检查数据是否已存在
        Args:
            symbol: 股票代码
            table_name: 表名
            start_date: 开始日期
            end_date: 结束日期
            freq: 频率（分钟数据专用）
        Returns:
            bool: 是否已存在数据
        """
        try:
            query = f"SELECT MAX(update_time) as latest_update FROM {table_name} WHERE symbol='{symbol}'"
            if freq:
                query += f" AND freq='{freq}'"
            if start_date and end_date:
                query += f" AND date BETWEEN '{start_date}' AND '{end_date}'"

            result = pd.read_sql(query, self.engine)
            if not result.empty and result['latest_update'].iloc[0] is not None:
                latest_update = pd.to_datetime(result['latest_update'].iloc[0])
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                latest_date = latest_update.replace(hour=0, minute=0, second=0, microsecond=0)
                if latest_date >= today:
                    return True
            return False
        except Exception as e:
            self.logger.error(f"检查数据存在性失败: {str(e)}")
            return False
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """数据验证方法
        Args:
            data: 待验证的数据
        Returns:
            bool: 验证是否通过
        """
        if not data or not isinstance(data, dict):
            return False
            
        required_fields = ['data_type', 'data']
        if not all(field in data for field in required_fields):
            return False
            
        if data['data'] is None or (isinstance(data['data'], pd.DataFrame) and data['data'].empty):
            return False
            
        return True
    
    @abstractmethod
    def collect(self, **kwargs) -> Optional[Dict[str, Any]]:
        """数据采集方法
        Args:
            **kwargs: 采集参数
        Returns:
            Optional[Dict[str, Any]]: 采集到的数据
        """
        pass