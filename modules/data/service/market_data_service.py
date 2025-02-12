from typing import Optional, Dict, Any, List
import pandas as pd

from .base_service import BaseService, cache_result
from config.config_manager import ConfigManager

class MarketDataService(BaseService):
    """市场数据服务，提供市场数据的采集和查询接口"""
    
    def __init__(self):
        super().__init__()
        self.collector = None
        self.config_manager = ConfigManager()
    
    def _get_collector(self):
        """延迟加载 MarketDataCollector 实例"""
        if self.collector is None:
            from ..collector.market.market_data_collector import MarketDataCollector
            self.collector = MarketDataCollector()
        return self.collector
    
    def get_config(self) -> Dict[str, Any]:
        """获取数据初始化相关的配置信息
        Returns:
            Dict[str, Any]: 配置信息
        """
        try:
            # 获取数据采集相关的配置
            config = self.config_manager.get_config('data_collection')
            if not config:
                self.logger.error("无法获取数据采集配置信息")
                return {}
            
            # 获取初始化配置
            init_config = config.get('initialization', {})
            if not init_config:
                self.logger.warning("未找到初始化配置，将使用默认配置")
            
            # 设置默认配置
            default_config = {
                'init_daily_data': True,
                'init_weekly_data': True,
                'init_index_data': True,
                'index_list': ['000001', '000300', '399001', '399006']
            }
            
            # 合并默认配置和用户配置
            return {**default_config, **init_config}
            
        except Exception as e:
            self.logger.error(f"获取配置信息失败: {str(e)}")
            return {}
    
    @cache_result(expire_seconds=300)
    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """获取股票列表
        Returns:
            Optional[pd.DataFrame]: 股票列表，包含symbol等必要字段
        """
        try:
            collector = self._get_collector()
            # 从数据采集器获取原始数据
            result = collector.collect(data_type='stock_list')
            if not result or not isinstance(result, dict) or 'data' not in result:
                self.logger.error("获取股票列表数据失败")
                return None

            df = result['data']
            if not isinstance(df, pd.DataFrame) or df.empty:
                self.logger.error("股票列表数据为空")
                return None

            # 确保必要字段存在
            required_fields = ['symbol', 'name']
            if not all(field in df.columns for field in required_fields):
                self.logger.error(f"股票列表数据缺少必要字段: {required_fields}")
                return None

            # 添加更新时间
            df.loc[:, 'update_time'] = pd.Timestamp.now()

            try:
                # 获取已有数据
                existing_data = pd.read_sql(
                    "SELECT * FROM stock_basic_info",
                    self.engine
                )

                if not existing_data.empty:
                    # 使用 DataMergeStrategy 合并数据
                    from ..storage.merge_strategy import DataMergeStrategy
                    df = DataMergeStrategy.merge_dataframes(
                        df,
                        existing_data,
                        merge_keys=['symbol'],
                        exclude_merge_keys=None
                    )

                # 保存到数据库
                df.to_sql(
                    'stock_basic_info',
                    self.engine,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                self.logger.info(f"成功保存{len(df)}条股票基本信息到数据库")

            except Exception as e:
                self.logger.error(f"保存股票列表数据到数据库失败: {str(e)}")

            # 返回包含symbol字段的DataFrame
            return df
            
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {str(e)}")
            return None
    
    @cache_result(expire_seconds=300)
    def get_index_list(self) -> Optional[List[Dict[str, Any]]]:
        """获取指数列表
        Returns:
            Optional[List[Dict[str, Any]]]: 指数列表
        """
        try:
            return self.collector.get_index_list()
        except Exception as e:
            self.logger.error(f"获取指数列表失败: {str(e)}")
            return None
    
    def get_realtime_quotes(self, symbols: List[str]) -> Optional[pd.DataFrame]:
        """获取实时行情数据
        Args:
            symbols: 股票代码列表
        Returns:
            Optional[pd.DataFrame]: 实时行情数据
        """
        try:
            return self.collector.get_realtime_quotes(symbols)
        except Exception as e:
            self.logger.error(f"获取实时行情数据失败: {str(e)}")
            return None
    
    def get_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """获取市场数据
        Args:
            **kwargs: 查询参数
        Returns:
            Optional[pd.DataFrame]: 市场数据
        """
        try:
            symbols = kwargs.get('symbols')
            if not symbols:
                self.logger.error("缺少必要的symbols参数")
                return None
            return self.get_realtime_quotes(symbols)
        except Exception as e:
            self.logger.error(f"获取市场数据失败: {str(e)}")
            return None

    def save_data(self, data: pd.DataFrame, **kwargs) -> bool:
        """保存市场数据
        Args:
            data: 待保存的数据
            **kwargs: 保存参数
        Returns:
            bool: 是否保存成功
        """
        try:
            # 这里可以根据实际需求实现数据保存逻辑
            # 由于市场数据通常是实时数据，可能不需要保存
            self.logger.info("市场数据保存成功")
            return True
        except Exception as e:
            self.logger.error(f"保存市场数据失败: {str(e)}")
            return False