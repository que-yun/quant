from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from loguru import logger
from functools import wraps
import pandas as pd

from ..storage.db_pool import DatabasePool

def cache_result(expire_seconds: int = 300):
    """缓存装饰器
    Args:
        expire_seconds: 缓存过期时间（秒）
    """
    def decorator(func):
        cache = {}
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            cache_key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # 检查缓存是否存在且未过期
            if cache_key in cache:
                result, timestamp = cache[cache_key]
                if datetime.now() - timestamp < timedelta(seconds=expire_seconds):
                    return result
            
            # 执行原函数
            result = func(*args, **kwargs)
            
            # 更新缓存
            cache[cache_key] = (result, datetime.now())
            
            return result
        return wrapper
    return decorator

class BaseService(ABC):
    """数据服务基类，提供统一的数据访问接口"""
    
    def __init__(self):
        self.name = self.__class__.__name__
        self.logger = logger
        self.db_pool = DatabasePool()
        self.engine = self.db_pool.get_engine()
    
    @abstractmethod
    def get_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """获取数据的抽象方法
        Args:
            **kwargs: 查询参数
        Returns:
            Optional[pd.DataFrame]: 查询结果
        """
        pass
    
    @abstractmethod
    def save_data(self, data: pd.DataFrame, **kwargs) -> bool:
        """保存数据的抽象方法
        Args:
            data: 待保存的数据
            **kwargs: 保存参数
        Returns:
            bool: 是否保存成功
        """
        pass
    
    def validate_params(self, params: Dict[str, Any]) -> bool:
        """验证参数的有效性
        Args:
            params: 待验证的参数
        Returns:
            bool: 验证是否通过
        """
        return True
    
    def format_query_result(self, data: pd.DataFrame) -> pd.DataFrame:
        """格式化查询结果
        Args:
            data: 查询结果数据
        Returns:
            pd.DataFrame: 格式化后的数据
        """
        return data
    
    @cache_result(expire_seconds=300)
    def query_with_cache(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """带缓存的数据查询
        Args:
            sql: SQL查询语句
            params: 查询参数
        Returns:
            Optional[pd.DataFrame]: 查询结果
        """
        try:
            if params:
                df = pd.read_sql(sql, self.engine, params=params)
            else:
                df = pd.read_sql(sql, self.engine)
            return df
        except Exception as e:
            self.logger.error(f"{self.name} 数据查询失败: {str(e)}")
            return None
    
    def execute_transaction(self, statements: List[str]) -> bool:
        """执行事务操作
        Args:
            statements: SQL语句列表
        Returns:
            bool: 是否执行成功
        """
        if not statements:
            self.logger.warning(f"{self.name} 没有需要执行的SQL语句")
            return False

        connection = None
        try:
            connection = self.db_pool.get_connection()
            if not connection:
                self.logger.error(f"{self.name} 无法获取数据库连接")
                return False

            with connection.begin() as transaction:
                try:
                    for statement in statements:
                        connection.execute(statement)
                    transaction.commit()
                    self.logger.info(f"{self.name} 事务执行成功，共执行{len(statements)}条SQL语句")
                    return True
                except Exception as e:
                    transaction.rollback()
                    self.logger.error(f"{self.name} 事务执行失败，已回滚: {str(e)}")
                    return False
        except Exception as e:
            self.logger.error(f"{self.name} 事务处理异常: {str(e)}")
            return False
        finally:
            if connection:
                try:
                    connection.close()
                except Exception as e:
                    self.logger.error(f"{self.name} 关闭数据库连接失败: {str(e)}")