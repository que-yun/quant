from datetime import datetime
from typing import Optional, Dict, Any, List, Set
import pandas as pd
from sqlalchemy import text
from modules.utils.log_manager import logger

class StockStorage:
    """股票数据存储类，负责所有与股票数据相关的数据库操作"""
    
    def __init__(self):
        self.storage = None
        self.logger = logger
        self.engine = None
        
    def _get_storage(self):
        """延迟加载DatabaseStorage实例"""
        if self.storage is None:
            from .database_storage import DatabaseStorage
            self.storage = DatabaseStorage()
            self.engine = self.storage.engine
        return self.storage

    def save_stock_data(self, df: pd.DataFrame, table_name: str) -> bool:
        """保存股票数据，在内存中进行数据合并后再写入数据库
        Args:
            df: 股票数据
            table_name: 表名
        Returns:
            bool: 是否成功
        """
        try:
            if df is None or df.empty:
                return False

            self._get_storage()
            # 添加更新时间
            df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 获取已存在的数据
            existing_data = None
            if 'symbol' in df.columns and 'date' in df.columns:
                symbols = df['symbol'].unique().tolist()  # 确保是列表格式
                min_date = pd.to_datetime(df['date'].min()).strftime('%Y-%m-%d')
                max_date = pd.to_datetime(df['date'].max()).strftime('%Y-%m-%d')
                
                # 构建参数字典
                params = {}
                for i, symbol in enumerate(symbols):
                    params[f'symbol_{i}'] = symbol
                params['min_date'] = min_date
                params['max_date'] = max_date
                
                # 使用命名参数构建查询
                placeholders = ','.join([f':symbol_{i}' for i in range(len(symbols))])
                query = text(f"SELECT * FROM {table_name} WHERE symbol IN ({placeholders}) AND date BETWEEN :min_date AND :max_date")
                
                try:
                    with self.engine.connect() as conn:
                        existing_data = pd.read_sql_query(query, conn, params=params)
                except Exception as e:
                    self.logger.warning(f"获取已存在数据失败: {str(e)}")
            
            # 在内存中合并数据
            if existing_data is not None and not existing_data.empty:
                # 使用 DataMergeStrategy 合并数据
                from .merge_strategy import DataMergeStrategy
                combined_data = DataMergeStrategy.merge_dataframes(
                    df,
                    existing_data,
                    merge_keys=['symbol', 'date']
                )
            else:
                combined_data = df
            
            # 使用 UPSERT 操作写入数据库
            with self.engine.begin() as conn:
                # 创建临时表
                temp_table = f'temp_{table_name}_{datetime.now().strftime("%Y%m%d%H%M%S")}'
                combined_data.to_sql(temp_table, conn, if_exists='replace', index=False)
                
                # 分别执行INSERT和DROP语句
                insert_query = text(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM {temp_table}")
                conn.execute(insert_query)
                
                drop_query = text(f"DROP TABLE {temp_table}")
                conn.execute(drop_query)

            self.logger.info(f"成功保存{len(combined_data)}条数据到{table_name}表")
            return True

        except Exception as e:
            self.logger.error(f"保存股票数据失败: {str(e)}")
            return False

    def load_stock_data(self, symbol: str, table_name: str, start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """加载股票数据
        Args:
            symbol: 股票代码
            table_name: 表名
            start_date: 开始日期
            end_date: 结束日期
        Returns:
            Optional[pd.DataFrame]: 股票数据
        """
        try:
            self._get_storage()
            query = "SELECT * FROM ? WHERE symbol = ?"
            params = [table_name, symbol]
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            query += " ORDER BY date"
            
            with self.engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params=params)
                return None if df.empty else df
            
        except Exception as e:
            self.logger.error(f"加载股票数据失败: {str(e)}")
            return None

    def get_stock_history(self, symbol: str, period: str = 'daily', start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """获取股票历史数据，支持日线、周线和月线
        Args:
            symbol: 股票代码
            period: 数据周期，支持 'daily'（日线）、'weekly'（周线）、'monthly'（月线）
            start_date: 开始日期，格式YYYY-MM-DD
            end_date: 结束日期，格式YYYY-MM-DD
        Returns:
            Optional[pd.DataFrame]: 股票历史数据
        """
        try:
            # 初始化 AKShareAPI
            from ..collector.api.akshare_api import AKShareAPI
            api = AKShareAPI()
            
            # 获取数据
            df = api.get_stock_history(symbol, period, start_date, end_date)
            if df is None or df.empty:
                self.logger.error(f"获取{symbol}的{period}数据失败")
                return None
            
            # 保存数据到数据库
            table_name = f"{period}_bars" if period != 'daily' else 'daily_bars'
            if self.save_stock_data(df, table_name):
                return df
            return None
            
        except Exception as e:
            self.logger.error(f"获取股票历史数据失败: {str(e)}")
            return None