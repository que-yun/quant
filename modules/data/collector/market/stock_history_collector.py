from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger

from ..base.collector_base import CollectorBase
from ..api.akshare_api import AKShareAPI
from ...service.market_data_service import MarketDataService

class StockHistoryCollector(CollectorBase, AKShareAPI):
    """A股历史数据采集器，支持增量式采集和数据完整性校验"""

    def __init__(self):
        CollectorBase.__init__(self)
        AKShareAPI.__init__(self)
        self.market_service = MarketDataService()
        self.table_name = 'daily_bars'
        self.retry_times = 3
        self.required_fields = ['open', 'high', 'low', 'close', 'volume']

    def collect(self, **kwargs) -> Optional[Dict[str, Any]]:
        """增量式采集历史数据
        Args:
            **kwargs: 采集参数，支持以下参数：
                - start_date: 开始日期，格式YYYY-MM-DD
                - end_date: 结束日期，格式YYYY-MM-DD
        Returns:
            Optional[Dict[str, Any]]: 采集结果统计
        """
        try:
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date', datetime.now().strftime('%Y-%m-%d'))

            if not start_date:
                self.logger.error("未提供开始日期")
                return None

            # 获取股票列表（使用缓存）
            stock_list_df = self.market_service.get_stock_list()
            if stock_list_df is None or stock_list_df.empty:
                self.logger.error("获取股票列表失败")
                return None

            # 过滤掉退市股票
            active_stocks = self._filter_active_stocks(stock_list_df)
            if not active_stocks:
                self.logger.error("没有找到活跃的股票")
                return None

            # 统计信息
            stats = {
                'total_stocks': len(active_stocks),
                'processed_stocks': 0,
                'success_stocks': 0,
                'failed_stocks': 0,
                'skipped_stocks': 0
            }

            # 遍历处理每个股票
            for symbol in active_stocks:
                try:
                    # 检查数据是否已存在
                    missing_dates = self._get_missing_dates(symbol, start_date, end_date)
                    if not missing_dates:
                        stats['skipped_stocks'] += 1
                        continue

                    # 获取缺失数据
                    success = self._collect_and_save_stock_data(symbol, missing_dates)
                    if success:
                        stats['success_stocks'] += 1
                    else:
                        stats['failed_stocks'] += 1

                except Exception as e:
                    self.logger.error(f"处理股票{symbol}时发生错误: {str(e)}")
                    stats['failed_stocks'] += 1
                finally:
                    stats['processed_stocks'] += 1

            return stats

        except Exception as e:
            self.logger.error(f"数据采集过程发生错误: {str(e)}")
            return None

    def _filter_active_stocks(self, stock_list_df: pd.DataFrame) -> List[str]:
        """过滤获取活跃的股票列表
        Args:
            stock_list_df: 股票列表数据
        Returns:
            List[str]: 活跃股票代码列表
        """
        try:
            # 过滤ST股票和状态异常的股票
            active_df = stock_list_df[
                (~stock_list_df['name'].str.contains('ST|\*ST', na=False)) &
                (stock_list_df['volume'] > 0)
            ]
            return active_df['symbol'].tolist()
        except Exception as e:
            self.logger.error(f"过滤活跃股票失败: {str(e)}")
            return []

    def _get_missing_dates(self, symbol: str, start_date: str, end_date: str) -> List[str]:
        """获取缺失的日期列表
        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        Returns:
            List[str]: 缺失的日期列表
        """
        try:
            # 查询数据库中已有的数据
            query = f"SELECT DISTINCT date FROM {self.table_name} WHERE symbol = '{symbol}' AND date BETWEEN '{start_date}' AND '{end_date}'"
            existing_df = pd.read_sql(query, self.engine)
            existing_dates = set(existing_df['date'].astype(str))

            # 生成完整的日期范围
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            all_dates = set(date_range.strftime('%Y-%m-%d'))

            # 计算缺失的日期
            missing_dates = list(all_dates - existing_dates)
            missing_dates.sort()
            return missing_dates

        except Exception as e:
            self.logger.error(f"获取缺失日期失败: {str(e)}")
            return []

    def _collect_and_save_stock_data(self, symbol: str, dates: List[str]) -> bool:
        """采集并保存股票数据
        Args:
            symbol: 股票代码
            dates: 需要采集的日期列表
        Returns:
            bool: 是否成功
        """
        retry_count = 0
        while retry_count < self.retry_times:
            try:
                # 调用AKShare API获取数据
                df = self.get_stock_daily_data(symbol, min(dates), max(dates))
                if df is None or df.empty:
                    retry_count += 1
                    continue

                # 验证关键字段
                if not all(field in df.columns for field in self.required_fields):
                    self.logger.error(f"股票{symbol}数据缺少必要字段")
                    retry_count += 1
                    continue

                # 过滤出需要的日期的数据
                df = df[df['date'].astype(str).isin(dates)]
                if df.empty:
                    return True  # 没有需要保存的数据，视为成功

                # 保存数据
                df['symbol'] = symbol
                df = df.reset_index(drop=True)  # 添加这行，确保删除index列
                df.to_sql(
                    self.table_name,
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                return True

            except Exception as e:
                self.logger.error(f"采集股票{symbol}数据失败: {str(e)}")
                retry_count += 1
                if retry_count < self.retry_times:
                    self.logger.info(f"正在进行第{retry_count + 1}次重试...")

        return False