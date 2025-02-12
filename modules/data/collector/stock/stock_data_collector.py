from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import pandas as pd
import time
from ..base.collector_base import CollectorBase
from ..api.akshare_api import AKShareAPI
from ...storage.stock_storage import StockStorage


class StockDataCollector(CollectorBase, AKShareAPI):
    """增强版股票数据采集器，负责采集股票的日线、分钟线等数据"""

    def __init__(self):
        CollectorBase.__init__(self)
        AKShareAPI.__init__(self)
        self._trading_days_cache = None
        self._cache_update_time = None
        self._batch_size = 100  # 批量处理的大小
        self._retry_times = 3  # 重试次数
        self._retry_delay = 1  # 重试延迟（秒）
        self.storage = None  # 使用延迟加载

    def _get_storage(self):
        """延迟加载StockStorage实例"""
        if self.storage is None:
            from ...storage.stock_storage import StockStorage
            self.storage = StockStorage()
            # 初始化存储引擎
            self.storage._get_storage()
        return self.storage

    def batch_collect_daily_data(self, symbols: List[str], start_date: str = None, end_date: str = None) -> Optional[
        pd.DataFrame]:
        try:
            # 确保存储对象已初始化
            storage = self._get_storage()
            if storage is None:
                self.logger.error("初始化存储对象失败")
                return None
            total_symbols = len(symbols)
            processed_count = 0
            success_count = 0
            failed_count = 0

            # 批量处理股票数据
            for i in range(0, total_symbols, self._batch_size):
                batch_symbols = symbols[i:i + self._batch_size]
                self.logger.info(f"正在处理第 {i + 1} 到 {min(i + self._batch_size, total_symbols)} 只股票的数据")

                # 批量获取数据
                batch_data = []
                for symbol in batch_symbols:
                    try:
                        # 获取数据并进行重试
                        for retry in range(self._retry_times):
                            try:
                                result = self.collect(data_type='daily', symbol=symbol,
                                                      start_date=start_date, end_date=end_date)
                                if result and 'data' in result:
                                    df = result['data']
                                    if not df.empty:
                                        # 验证数据完整性
                                        required_fields = ['open', 'high', 'low', 'close', 'volume']
                                        # 确保date列存在
                                        if 'date' not in df.columns:
                                            self.logger.error(f"股票{symbol}数据缺少date字段")
                                            continue
                                        # 添加必要的字段
                                        df.loc[:, 'symbol'] = symbol
                                        df.loc[:, 'update_time'] = pd.Timestamp.now()
                                        if all(field in df.columns for field in required_fields):
                                            batch_data.append(df)
                                            success_count += 1
                                        else:
                                            self.logger.error(f"股票{symbol}数据缺少必要字段")
                                            failed_count += 1
                                    break
                            except Exception as e:
                                if retry < self._retry_times - 1:
                                    self.logger.warning(f"采集{symbol}数据失败，{retry + 1}次重试: {str(e)}")
                                    time.sleep(self._retry_delay)
                                else:
                                    self.logger.error(f"采集{symbol}数据失败: {str(e)}")
                                    failed_count += 1
                    except Exception as e:
                        self.logger.error(f"处理股票{symbol}时发生错误: {str(e)}")
                        failed_count += 1
                    finally:
                        processed_count += 1

                # 批量保存数据
                if batch_data:
                    try:
                        batch_df = pd.concat(batch_data, ignore_index=True)

                        # 确保所有必要字段都存在
                        required_fields = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount',
                                           'amplitude', 'pct_change', 'price_change', 'turnover_rate', 'update_time']
                        for field in required_fields:
                            if field not in batch_df.columns:
                                if field == 'update_time':
                                    batch_df[field] = pd.Timestamp.now()
                                elif field == 'amount':
                                    batch_df[field] = 0.0
                                elif field in ['amplitude', 'pct_change', 'price_change', 'turnover_rate']:
                                    batch_df[field] = 0.0
                                else:
                                    self.logger.error(f"缺少必要字段：{field}")
                                    raise ValueError(f"数据缺少必要字段：{field}")

                        # 使用StockStorage保存数据
                        if not self.storage.save_stock_data(batch_df, 'daily_bars'):
                            raise Exception("保存数据失败")

                        self.logger.info(f"成功保存{len(batch_data)}只股票的数据")
                    except Exception as e:
                        self.logger.error(f"保存批次数据失败: {str(e)}")
                        failed_count += len(batch_data)
                        success_count -= len(batch_data)
                    finally:
                        batch_data.clear()  # 释放内存

            # 输出统计信息
            self.logger.info(f"数据采集完成：总计{total_symbols}只股票，")
            self.logger.info(f"处理{processed_count}只，成功{success_count}只，")
            self.logger.info(f"失败{failed_count}只")

            return None  # 由于数据已经分批保存，不需要返回DataFrame


        except Exception as e:
            self.logger.error(f"批量采集日线数据失败: {str(e)}")
            return None

    def _get_trading_days(self):
        """获取交易日历，使用缓存优化性能"""
        now = datetime.now()
        if self._trading_days_cache is None or \
                self._cache_update_time is None or \
                (now - self._cache_update_time).days >= 1:
            start_date = (now - timedelta(days=365)).strftime("%Y%m%d")
            end_date = now.strftime("%Y%m%d")
            self._trading_days_cache = set(self.get_trade_dates(start_date, end_date))
            self._cache_update_time = now
        return self._trading_days_cache

    def _is_trading_day(self, date_str):
        """判断是否为交易日"""
        try:
            date = datetime.strptime(date_str, "%Y%m%d")
            if date.weekday() >= 5:
                return False
            return date_str in self._get_trading_days()
        except Exception:
            return True

    def collect(self, **kwargs) -> Optional[Dict[str, Any]]:
        """数据采集方法
        Args:
            **kwargs: 采集参数，支持以下参数：
                - data_type: 数据类型，支持 'daily'、'minute'、'index'
                - symbol: 股票代码
                - start_date: 开始日期，格式YYYYMMDD
                - end_date: 结束日期，格式YYYYMMDD
                - freq: 分钟数据的频率，如'5'
                - days: 分钟数据的天数
        Returns:
            Optional[Dict[str, Any]]: 采集到的数据
        """
        data_type = kwargs.get('data_type')
        symbol = kwargs.get('symbol')

        if not symbol:
            self.logger.error("未提供股票代码")
            return None

        # 从kwargs中移除symbol参数，避免重复传递
        kwargs_without_symbol = kwargs.copy()
        kwargs_without_symbol.pop('symbol', None)

        if data_type == 'daily':
            return self._collect_daily_data(symbol, **kwargs_without_symbol)
        elif data_type == 'minute':
            return self._collect_minute_data(symbol, **kwargs_without_symbol)
        elif data_type == 'index':
            return self._collect_index_data(symbol, **kwargs_without_symbol)
        else:
            self.logger.error(f"不支持的数据类型：{data_type}")
            return None

    def _collect_daily_data(self, symbol: str, **kwargs) -> Optional[Dict[str, Any]]:
        """采集日线数据"""
        try:
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')

            # 获取数据
            df = self.get_stock_history(symbol, 'daily', start_date, end_date)
            if df is None:
                return None

            return {
                'data_type': 'daily',
                'symbol': symbol,
                'data': df
            }

        except Exception as e:
            self.logger.error(f"日线数据采集失败：{str(e)}")
            return None

    def _collect_minute_data(self, symbol: str, **kwargs) -> Optional[Dict[str, Any]]:
        """采集分钟数据，支持分段获取"""
        try:
            freq = kwargs.get('freq', '5')
            days = kwargs.get('days', 30)

            # 分段获取数据
            segment_days = 60
            total_segments = (days + segment_days - 1) // segment_days

            all_data = []
            for segment in range(total_segments):
                end_date = (datetime.now() - timedelta(days=segment * segment_days))
                start_date = end_date - timedelta(days=min(segment_days, days - segment * segment_days))

                # 格式化日期
                end_time = end_date.strftime("%Y%m%d")
                start_time = start_date.strftime("%Y%m%d")

                # 检查是否为交易日
                if not self._is_trading_day(end_time):
                    continue

                # 获取数据
                stock_code = symbol[2:] if symbol.startswith(('sh', 'sz')) else symbol
                df = self.get_minute_data(stock_code, freq, start_time, end_time)

                if df is not None and not df.empty:
                    all_data.append(df)

                time.sleep(1)  # 控制请求频率

            if not all_data:
                return None

            final_df = pd.concat(all_data, ignore_index=True)
            return {
                'data_type': 'minute',
                'symbol': symbol,
                'freq': freq,
                'data': final_df
            }

        except Exception as e:
            self.logger.error(f"分钟数据采集失败：{str(e)}")
            return None

    def get_index_history(self, symbol: str, period: str, start_date: str = None, end_date: str = None) -> Optional[
        pd.DataFrame]:
        """获取指数历史数据
        Args:
            symbol: 指数代码
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
        Returns:
            Optional[pd.DataFrame]: 指数历史数据
        """
        try:
            # 使用 AKShareAPI 获取指数数据
            result = self.get_index_data(symbol, period, start_date, end_date)
            if result is None or 'data' not in result:
                self.logger.error(f"获取指数{symbol}的历史数据失败")
                return None

            # 将字典数据转换为DataFrame
            df = pd.DataFrame(result['data'])

            # 确保日期格式正确
            df['date'] = pd.to_datetime(df['date'])

            return df

        except Exception as e:
            self.logger.error(f"获取指数历史数据失败：{str(e)}")
            return None

    def _collect_index_data(self, symbol: str, **kwargs) -> Optional[Dict[str, Any]]:
        """采集指数数据"""
        try:
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')
            period = kwargs.get('period', 'daily')

            # 获取指数数据
            df = self.get_index_history(symbol, period, start_date, end_date)
            if df is None or df.empty:
                return None

            # 添加必要字段
            df['symbol'] = symbol
            df['update_time'] = pd.Timestamp.now()

            # 确保必要字段存在
            required_fields = ['open', 'high', 'low', 'close', 'volume']
            if not all(field in df.columns for field in required_fields):
                self.logger.error(f"指数{symbol}数据缺少必要字段")
                return None

            return {
                'data_type': 'index',
                'symbol': symbol,
                'period': period,
                'data': df
            }

        except Exception as e:
            self.logger.error(f"指数数据采集失败：{str(e)}")
            return None

    def batch_collect_weekly_data(self, symbols: List[str], start_date: str = None, end_date: str = None) -> Optional[
        pd.DataFrame]:
        """批量采集股票周线数据
        Args:
            symbols: 股票代码列表
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
        Returns:
            Optional[pd.DataFrame]: 合并后的周线数据
        """
        try:
            # 确保存储对象已初始化
            storage = self._get_storage()
            if storage is None:
                self.logger.error("初始化存储对象失败")
                return None

            total_symbols = len(symbols)
            processed_count = 0
            success_count = 0
            failed_count = 0

            # 批量处理股票数据
            for i in range(0, total_symbols, self._batch_size):
                batch_symbols = symbols[i:i + self._batch_size]
                self.logger.info(f"正在处理第 {i + 1} 到 {min(i + self._batch_size, total_symbols)} 只股票的周线数据")

                # 批量获取数据
                batch_data = []
                for symbol in batch_symbols:
                    try:
                        # 获取数据并进行重试
                        for retry in range(self._retry_times):
                            try:
                                # 直接获取周线数据
                                df = self.get_stock_history(symbol, 'weekly', start_date, end_date)
                                if df is not None and not df.empty:
                                    # 验证数据完整性
                                    required_fields = ['open', 'high', 'low', 'close', 'volume']
                                    # 确保date列存在
                                    if 'date' not in df.columns:
                                        self.logger.error(f"股票{symbol}数据缺少date字段")
                                        continue
                                    # 添加必要的字段
                                    df.loc[:, 'symbol'] = symbol
                                    df.loc[:, 'update_time'] = pd.Timestamp.now()
                                    if all(field in df.columns for field in required_fields):
                                        batch_data.append(df)
                                        success_count += 1
                                    else:
                                        self.logger.error(f"股票{symbol}数据缺少必要字段")
                                        failed_count += 1
                                break
                            except Exception as e:
                                if retry < self._retry_times - 1:
                                    self.logger.warning(f"采集{symbol}周线数据失败，{retry + 1}次重试: {str(e)}")
                                    time.sleep(self._retry_delay)
                                else:
                                    self.logger.error(f"采集{symbol}周线数据失败: {str(e)}")
                                    failed_count += 1
                    except Exception as e:
                        self.logger.error(f"处理股票{symbol}时发生错误: {str(e)}")
                        failed_count += 1
                    finally:
                        processed_count += 1

                # 批量保存数据
                if batch_data:
                    try:
                        batch_df = pd.concat(batch_data, ignore_index=True)

                        # 使用 DataMergeStrategy 确保所有必要字段都存在
                        from ...storage.merge_strategy import DataMergeStrategy
                        required_fields = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount',
                                           'amplitude', 'pct_change', 'price_change', 'turnover_rate', 'update_time']
                        batch_df = DataMergeStrategy.ensure_required_fields(batch_df, required_fields)

                        # 使用StockStorage保存数据
                        if not self.storage.save_stock_data(batch_df, 'weekly_bars'):
                            raise Exception("保存数据失败")

                        self.logger.info(f"成功保存{len(batch_data)}只股票的周线数据")
                    except Exception as e:
                        self.logger.error(f"保存批次数据失败: {str(e)}")
                        failed_count += len(batch_data)
                        success_count -= len(batch_data)
                    finally:
                        batch_data.clear()  # 释放内存

            # 输出统计信息
            self.logger.info(f"数据采集完成：总计{total_symbols}只股票，")
            self.logger.info(f"处理{processed_count}只，成功{success_count}只，")
            self.logger.info(f"失败{failed_count}只")

            return None  # 由于数据已经分批保存，不需要返回DataFrame

        except Exception as e:
            self.logger.error(f"批量采集周线数据失败: {str(e)}")
            return None

    def batch_collect_monthly_data(self, symbols: List[str], start_date: str = None, end_date: str = None) -> Optional[
        pd.DataFrame]:
        """批量采集股票月线数据
        Args:
            symbols: 股票代码列表
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
        Returns:
            Optional[pd.DataFrame]: 合并后的月线数据
        """
        try:
            # 确保存储对象已初始化
            storage = self._get_storage()
            if storage is None:
                self.logger.error("初始化存储对象失败")
                return None

            total_symbols = len(symbols)
            processed_count = 0
            success_count = 0
            failed_count = 0

            # 批量处理股票数据
            for i in range(0, total_symbols, self._batch_size):
                batch_symbols = symbols[i:i + self._batch_size]
                self.logger.info(f"正在处理第 {i + 1} 到 {min(i + self._batch_size, total_symbols)} 只股票的月线数据")

                # 批量获取数据
                batch_data = []
                for symbol in batch_symbols:
                    try:
                        # 获取数据并进行重试
                        for retry in range(self._retry_times):
                            try:
                                # 直接获取月线数据
                                df = self.get_stock_history(symbol, 'monthly', start_date, end_date)
                                if df is not None and not df.empty:
                                    # 验证数据完整性
                                    required_fields = ['open', 'high', 'low', 'close', 'volume']
                                    # 确保date列存在
                                    if 'date' not in df.columns:
                                        self.logger.error(f"股票{symbol}数据缺少date字段")
                                        continue
                                    # 添加必要的字段
                                    df.loc[:, 'symbol'] = symbol
                                    df.loc[:, 'update_time'] = pd.Timestamp.now()
                                    if all(field in df.columns for field in required_fields):
                                        batch_data.append(df)
                                        success_count += 1
                                    else:
                                        self.logger.error(f"股票{symbol}数据缺少必要字段")
                                        failed_count += 1
                                break
                            except Exception as e:
                                if retry < self._retry_times - 1:
                                    self.logger.warning(f"采集{symbol}月线数据失败，{retry + 1}次重试: {str(e)}")
                                    time.sleep(self._retry_delay)
                                else:
                                    self.logger.error(f"采集{symbol}月线数据失败: {str(e)}")
                                    failed_count += 1
                    except Exception as e:
                        self.logger.error(f"处理股票{symbol}时发生错误: {str(e)}")
                        failed_count += 1
                    finally:
                        processed_count += 1

                # 批量保存数据
                if batch_data:
                    try:
                        batch_df = pd.concat(batch_data, ignore_index=True)

                        # 确保所有必要字段都存在
                        required_fields = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount',
                                           'amplitude', 'pct_change', 'price_change', 'turnover_rate', 'update_time']
                        for field in required_fields:
                            if field not in batch_df.columns:
                                if field == 'update_time':
                                    batch_df[field] = pd.Timestamp.now()
                                elif field == 'amount':
                                    batch_df[field] = 0.0
                                elif field in ['amplitude', 'pct_change', 'price_change', 'turnover_rate']:
                                    batch_df[field] = 0.0
                                else:
                                    self.logger.error(f"缺少必要字段：{field}")
                                    raise ValueError(f"数据缺少必要字段：{field}")

                        # 使用StockStorage保存数据
                        if not self.storage.save_stock_data(batch_df, 'monthly_bars'):
                            raise Exception("保存数据失败")

                        self.logger.info(f"成功保存{len(batch_data)}只股票的月线数据")
                    except Exception as e:
                        self.logger.error(f"保存批次数据失败: {str(e)}")
                        failed_count += len(batch_data)
                        success_count -= len(batch_data)
                    finally:
                        batch_data.clear()  # 释放内存

            # 输出统计信息
            self.logger.info(f"数据采集完成：总计{total_symbols}只股票，")
            self.logger.info(f"处理{processed_count}只，成功{success_count}只，")
            self.logger.info(f"失败{failed_count}只")

            return None  # 由于数据已经分批保存，不需要返回DataFrame

        except Exception as e:
            self.logger.error(f"批量采集月线数据失败: {str(e)}")
            return None

    def batch_collect_index_data(self, symbols: List[str], start_date: str = None, end_date: str = None) -> Optional[
        pd.DataFrame]:
        """批量采集指数数据
        Args:
            symbols: 指数代码列表
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
        Returns:
            Optional[pd.DataFrame]: 合并后的指数数据
        """
        try:
            all_data = []
            total_symbols = len(symbols)

            for i in range(0, total_symbols, self._batch_size):
                batch_symbols = symbols[i:i + self._batch_size]
                self.logger.info(f"正在处理第 {i + 1} 到 {min(i + self._batch_size, total_symbols)} 个指数的数据")

                for symbol in batch_symbols:
                    for retry in range(self._retry_times):
                        try:
                            result = self.collect(data_type='index', symbol=symbol,
                                                  start_date=start_date, end_date=end_date)
                            if result and 'data' in result:
                                df = result['data']
                                if not df.empty:
                                    # 添加指数代码列
                                    df['symbol'] = symbol
                                    all_data.append(df)
                                break
                        except Exception as e:
                            if retry < self._retry_times - 1:
                                self.logger.warning(f"采集{symbol}指数数据失败，{retry + 1}次重试: {str(e)}")
                                time.sleep(self._retry_delay)
                            else:
                                self.logger.error(f"采集{symbol}指数数据失败: {str(e)}")

            if not all_data:
                return None

            # 合并所有数据
            final_df = pd.concat(all_data, axis=0)
            # 重置索引，确保数据格式一致
            final_df = final_df.reset_index(drop=True)

            # 确保所有必要字段都存在
            required_fields = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount', 'amplitude',
                               'pct_change', 'price_change', 'turnover_rate', 'update_time']
            for field in required_fields:
                if field not in final_df.columns:
                    if field == 'update_time':
                        final_df[field] = pd.Timestamp.now()
                    elif field == 'amount':
                        final_df[field] = 0.0
                    elif field in ['amplitude', 'pct_change', 'price_change', 'turnover_rate']:
                        final_df[field] = 0.0
                    else:
                        self.logger.error(f"缺少必要字段：{field}")
                        raise ValueError(f"数据缺少必要字段：{field}")

            # 使用StockStorage保存数据到index_daily_data表
            storage = self._get_storage()
            if storage is None:
                self.logger.error("初始化存储对象失败")
                return None

            if not self.storage.save_stock_data(final_df, 'index_daily_data'):
                raise Exception("保存数据失败")

            self.logger.info(f"成功采集并保存{len(set(final_df['symbol']))}个指数的数据")
            return None  # 由于数据已经保存，不需要返回DataFrame

        except Exception as e:
            self.logger.error(f"批量采集指数数据失败: {str(e)}")
            return None
