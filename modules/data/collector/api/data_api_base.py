from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd
from loguru import logger

class DataAPIBase(ABC):
    """数据API基类，定义标准化的数据获取接口"""
    
    def __init__(self):
        self.logger = logger
    
    @abstractmethod
    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """获取股票列表
        Returns:
            Optional[pd.DataFrame]: 股票列表数据，包含以下字段：
                - symbol: 股票代码
                - name: 股票名称
                - volume: 成交量
                - close: 收盘价
        """
        pass
    
    def _validate_symbol(self, symbol: str) -> bool:
        """验证股票代码格式
        Args:
            symbol: 股票代码
        Returns:
            bool: 是否为有效的股票代码
        """
        if not isinstance(symbol, str):
            return False
        
        # 验证股票代码格式：sh/sz + 6位数字
        if not (symbol.startswith('sh') or symbol.startswith('sz')):
            return False
        
        code = symbol[2:]
        if not (code.isdigit() and len(code) == 6):
            return False
        
        return True
    
    def _standardize_data(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """标准化数据格式
        Args:
            df: 原始数据
            data_type: 数据类型，如'stock'、'index'
        Returns:
            pd.DataFrame: 标准化后的数据
        """
        # 重命名列
        column_mapping = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'pct_change',
            '涨跌额': 'price_change',
            '换手率': 'turnover'
        }
        
        df = df.rename(columns=column_mapping)
        return df
