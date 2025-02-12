from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime
from ..base.collector_base import CollectorBase
from ..api.akshare_api import AKShareAPI


def _compare_and_update(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    """比较新旧数据，返回需要更新的数据
    Args:
        new_df: 新采集的数据
        existing_df: 已存在的数据
    Returns:
        pd.DataFrame: 需要更新的数据
    """
    # 确保两个DataFrame具有相同的列
    common_columns = ['symbol', 'name']
    new_df = new_df[common_columns]
    existing_df = existing_df[common_columns]

    # 找出新增的股票
    new_symbols = set(new_df['symbol']) - set(existing_df['symbol'])
    new_records = new_df[new_df['symbol'].isin(new_symbols)]

    # 找出名称发生变化的股票
    merged_df = pd.merge(new_df, existing_df, on='symbol', suffixes=('_new', '_old'))
    changed_records = merged_df[merged_df['name_new'] != merged_df['name_old']]
    changed_records = new_df[new_df['symbol'].isin(changed_records['symbol'])]

    # 合并新增和变化的记录
    update_df = pd.concat([new_records, changed_records], ignore_index=True)
    return update_df


class MarketDataCollector(CollectorBase, AKShareAPI):
    """增强版市场数据采集器，负责采集股票列表、基本面等市场数据"""

    def __init__(self):
        CollectorBase.__init__(self)
        AKShareAPI.__init__(self)
        self.storage = None

    def _get_storage(self):
        """延迟加载 DatabaseStorage 实例"""
        if self.storage is None:
            from ...storage.database_storage import DatabaseStorage
            self.storage = DatabaseStorage()
            # 确保数据库已初始化
            if not self.storage.initialize():
                self.logger.error("数据库初始化失败")
                return None
        return self.storage

    def collect(self, **kwargs) -> Optional[Dict[str, Any]]:
        """数据采集方法
        Args:
            **kwargs: 采集参数，支持以下参数：
                - data_type: 数据类型，支持 'stock_list'、'stock_info'
                - symbol: 股票代码（获取股票信息时需要）
        Returns:
            Optional[Dict[str, Any]]: 采集到的数据
        """
        data_type = kwargs.get('data_type')

        if data_type == 'stock_list':
            return self._collect_stock_list()
        elif data_type == 'stock_info':
            return self._collect_stock_info(kwargs.get('symbol'))
        else:
            self.logger.error(f"不支持的数据类型：{data_type}")
            return None

    def _collect_stock_list(self) -> Optional[Dict[str, Any]]:
        """采集股票列表数据"""
        try:
            # 获取股票列表数据
            df = self.get_stock_list()
            if df is None or df.empty:
                self.logger.error("获取股票列表数据失败")
                return None

            # 检查退市状态并映射字段名
            df = self._process_stock_data(df)
            if df is None or df.empty:
                self.logger.error("处理股票数据失败")
                return None

            # 添加更新时间
            df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            return {
                'data_type': 'stock_list',
                'status': 'success',
                'message': f'成功获取{len(df)}条记录',
                'data': df
            }

        except Exception as e:
            self.logger.error(f"采集股票列表数据失败: {str(e)}")
            return None

    def _collect_stock_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """采集单个股票的基本信息"""
        try:
            # 验证股票代码格式
            if not self._validate_symbol(symbol):
                self.logger.error(f"无效的股票代码格式：{symbol}")
                return None

            # 从数据库获取股票信息
            sql = f"SELECT * FROM stock_basic_info WHERE symbol = '{symbol}'"
            df = pd.read_sql(sql, self.engine)

            if df.empty:
                return None

            return {
                'data_type': 'stock_info',
                'symbol': symbol,
                'data': df.iloc[0].to_dict()
            }

        except Exception as e:
            self.logger.error(f"获取股票{symbol}基本信息失败: {str(e)}")
            return None

    def _process_stock_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """处理股票数据，包括检查退市状态和字段映射
        Args:
            df: 原始股票数据
        Returns:
            Optional[pd.DataFrame]: 处理后的数据
        """
        try:
            # 检查并处理必要字段
            if 'symbol' not in df.columns:
                self.logger.error("数据缺少symbol字段")
                return None

            # 如果name字段不存在，使用symbol作为默认值
            if 'name' not in df.columns:
                self.logger.warning("数据缺少name字段，使用symbol作为默认值")
                df['name'] = df['symbol']

            # 过滤退市股票（成交量为0的股票视为退市）
            if 'volume' in df.columns:
                df = df[df['volume'] > 0]

            # 移除 "序号" 列
            if '序号' in df.columns:
                df = df.drop(columns=['序号'])

            return df

        except Exception as e:
            self.logger.error(f"处理股票数据失败: {str(e)}")
            return None
