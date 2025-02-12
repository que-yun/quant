from typing import Optional, Dict, Any
import pandas as pd

from .base_service import BaseService, cache_result


class StockDataService(BaseService):
    """股票数据服务，提供股票数据的存储和查询接口"""

    def __init__(self):
        super().__init__()
        self.table_name = 'daily_bars'

    def get_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """获取数据的实现方法
        Args:
            **kwargs: 查询参数，支持symbol、start_date、end_date
        Returns:
            Optional[pd.DataFrame]: 查询结果
        """
        try:
            symbol = kwargs.get('symbol')
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')

            if not symbol:
                self.logger.error("缺少必要的symbol参数")
                return None

            return self.get_stock_data(symbol, start_date, end_date)
        except Exception as e:
            self.logger.error(f"获取数据失败: {str(e)}")
            return None

    def save_data(self, data: pd.DataFrame, **kwargs) -> bool:
        """保存数据的实现方法
        Args:
            data: 待保存的数据
            **kwargs: 保存参数
        Returns:
            bool: 是否保存成功
        """
        try:
            # 如果数据是多级索引，需要重置索引
            if isinstance(data.index, pd.MultiIndex):
                data = data.reset_index()

            # 优化：直接使用DataFrame进行批量存储
            df = data.copy()
            # 确保不包含多余的index列
            if 'index' in df.columns:
                df = df.drop('index', axis=1)
            
            # 统一将日期列重命名为date
            if '日期' in df.columns:
                df = df.rename(columns={'日期': 'date'})

            # 检查并过滤已存在的数据
            if 'date' in df.columns and 'symbol' in df.columns:
                existing_dates = set()
                symbols = df['symbol'].unique()
                for symbol in symbols:
                    symbol_data = df[df['symbol'] == symbol]
                    min_date = symbol_data['date'].min()
                    max_date = symbol_data['date'].max()
                    query = f"SELECT DISTINCT date FROM {self.table_name} WHERE symbol='{symbol}' AND date BETWEEN '{min_date}' AND '{max_date}'"
                    try:
                        existing_df = pd.read_sql(query, self.engine)
                        existing_dates.update(existing_df['date'].astype(str))
                    except Exception as e:
                        self.logger.warning(f"查询已存在数据失败：{str(e)}")
                
                if existing_dates:
                    df = df[~df['date'].astype(str).isin(existing_dates)]
                    if df.empty:
                        self.logger.info("所有数据已存在，跳过保存")
                        return True
            
            # 使用更大的chunksize进行批量存储
            df.to_sql(
                self.table_name,
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=5000  # 增加批量大小以提升性能
            )
            self.logger.info(f"批量保存数据成功，共{len(df)}条记录")
            return True
        except Exception as e:
            self.logger.error(f"保存数据失败: {str(e)}")
            return False

    def save_stock_data(self, data: Dict[str, Any]) -> bool:
        """保存股票数据
        Args:
            data: 股票数据
        Returns:
            bool: 保存是否成功
        """
        try:
            df = pd.DataFrame(data)
            # 确保不包含多余的index列
            if 'index' in df.columns:
                df = df.drop('index', axis=1)
            
            # 统一将日期列重命名为date
            if '日期' in df.columns:
                df = df.rename(columns={'日期': 'date'})
            df.to_sql(
                self.table_name,
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            self.logger.info(f"保存股票数据成功，共{len(df)}条记录")
            return True
        except Exception as e:
            self.logger.error(f"保存股票数据失败: {str(e)}")
            return False

    @cache_result(expire_seconds=300)
    def get_stock_data(self, symbol: str, start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """获取股票数据
        Args:
            symbol: 股票代码
            start_date: 开始日期，格式YYYY-MM-DD
            end_date: 结束日期，格式YYYY-MM-DD
        Returns:
            Optional[pd.DataFrame]: 股票数据
        """
        try:
            # 构建优化的查询语句
            query = f"""SELECT date, open, high, low, close, volume, amount, 
                amplitude, pct_change, price_change, turnover_rate 
                FROM {self.table_name} 
                WHERE symbol = ? 
                {' AND date >= ?' if start_date else ''}
                {' AND date <= ?' if end_date else ''}
                ORDER BY date"""

            # 准备查询参数
            params = [symbol]
            if start_date:
                params.append(start_date)
            if end_date:
                params.append(end_date)

            # 使用参数化查询防止SQL注入并提高性能
            df = pd.read_sql_query(query, self.engine, params=params)
            
            if not df.empty:
                # 优化DataFrame结构
                df.set_index('date', inplace=True)
                df.index = pd.to_datetime(df.index)
                
                # 使用类型转换优化内存使用
                numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount',
                                'amplitude', 'pct_change', 'price_change', 'turnover_rate']
                df[numeric_columns] = df[numeric_columns].astype('float32')
                
                return df
            return None

        except Exception as e:
            self.logger.error(f"获取股票数据失败: {str(e)}")
            return None

    def get_latest_trade_date(self, symbol: str) -> Optional[str]:
        """获取最新交易日期
        Args:
            symbol: 股票代码
        Returns:
            Optional[str]: 最新交易日期，格式YYYY-MM-DD
        """
        try:
            query = f"SELECT MAX(date) as latest_date FROM {self.table_name} WHERE symbol = '{symbol}'"
            df = pd.read_sql(query, self.engine)
            return df['latest_date'].iloc[0] if not df.empty else None
        except Exception as e:
            self.logger.error(f"获取最新交易日期失败: {str(e)}")
            return None

    def delete_stock_data(self, symbol: str, start_date: str = None, end_date: str = None) -> bool:
        """删除股票数据
        Args:
            symbol: 股票代码
            start_date: 开始日期，格式YYYY-MM-DD
            end_date: 结束日期，格式YYYY-MM-DD
        Returns:
            bool: 删除是否成功
        """
        try:
            query = f"DELETE FROM {self.table_name} WHERE symbol = '{symbol}'"

            if start_date:
                query += f" AND date >= '{start_date}'"
            if end_date:
                query += f" AND date <= '{end_date}'"

            with self.engine.connect() as conn:
                conn.execute(query)
                conn.commit()

            self.logger.info(f"删除股票数据成功: {symbol}")
            return True
        except Exception as e:
            self.logger.error(f"删除股票数据失败: {str(e)}")
            return False
