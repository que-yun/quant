from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime
import pandas as pd
from loguru import logger

class DataAPIBase(ABC):
    """数据API基类，提供统一的数据获取接口和数据标准化功能"""
    
    def __init__(self):
        self.logger = logger
        self._column_mappings = {
            'stock': {
                '代码': 'symbol',
                '股票代码': 'symbol',
                '名称': 'name',
                '最新价': 'close',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '昨收': 'pre_close',
                '涨跌幅': 'pct_change',
                '涨跌额': 'price_change',
                '成交量': 'volume',
                '成交额': 'amount',
                '换手率': 'turnover_rate',
                '市盈率-动态': 'pe_ratio',
                '市净率': 'pb_ratio',
                '总市值': 'market_cap',
                '流通市值': 'circulating_market_cap',
                '今开': 'open',
                '年初至今涨跌幅': 'ytd_pct_change',
                '60日涨跌幅': 'pct_change_60d',
                '5分钟涨跌': 'pct_change_5m',
                '涨速': 'price_velocity',
                '量比': 'volume_ratio',
                '序号': 'index',
                '振幅': 'amplitude',
                '日期': 'date'
            },
            'index': {
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'price_change'
            }
        }
    
    def _standardize_data(self, df: pd.DataFrame, data_type: str = 'stock') -> pd.DataFrame:
        """标准化数据字段
        Args:
            df: 原始数据
            data_type: 数据类型，支持 'stock'、'index'
        Returns:
            pd.DataFrame: 标准化后的数据
        """
        if df is None or df.empty:
            return df
            
        mapping = self._column_mappings.get(data_type, {})
        if not mapping:
            return df
            
        # 只重命名存在的列
        existing_cols = set(df.columns) & set(mapping.keys())
        col_map = {k: v for k, v in mapping.items() if k in existing_cols}
        return df.rename(columns=col_map)
    
    def _add_market_info(self, df: pd.DataFrame, symbol_col: str = 'symbol') -> pd.DataFrame:
        """添加市场信息
        Args:
            df: 数据框
            symbol_col: 股票代码列名
        Returns:
            pd.DataFrame: 添加市场信息后的数据框
        """
        if df is None or df.empty or symbol_col not in df.columns:
            return df

        df['symbol'] = df['symbol'].apply(lambda x: f"{'sh' if x.startswith('6') else 'sz'}{x}")
        df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return df
    
    def _validate_symbol(self, symbol: str) -> bool:
        """验证股票代码格式
        Args:
            symbol: 股票代码
        Returns:
            bool: 是否有效
        """
        if not symbol:
            return False
            
        # 移除市场标识前缀
        if symbol.startswith(('sh', 'sz')):
            symbol = symbol[2:]
            
        return symbol.isdigit() and len(symbol) == 6
    
    @abstractmethod
    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """获取股票列表"""
        pass
    
    @abstractmethod
    def get_stock_history(self, symbol: str, period: str = 'daily', start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """获取历史行情数据"""
        pass
    
    @abstractmethod
    def get_minute_data(self, symbol: str, freq: str, start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """获取分钟级别数据"""
        pass
    
    @abstractmethod
    def get_index_data(self, symbol: str, start_date: str = None, end_date: str = None) -> Optional[Dict[str, Any]]:
        """获取指数数据"""
        pass
    
    def _check_date_range_exists(self, symbol: str, table_name: str, start_date: str = None, end_date: str = None, freq: str = None) -> bool:
        """检查指定日期范围的数据是否已存在
        Args:
            symbol: 股票代码
            table_name: 表名
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            freq: 数据频率（分钟数据专用）
        Returns:
            bool: 是否已存在数据
        """
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name} WHERE symbol='{symbol}'"
            if freq:
                query += f" AND freq='{freq}'"
            if start_date and end_date:
                query += f" AND date BETWEEN '{start_date}' AND '{end_date}'"

            result = pd.read_sql(query, self.engine)
            return result['count'].iloc[0] > 0

        except Exception as e:
            self.logger.error(f"检查日期范围数据存在性失败: {str(e)}")
            return False