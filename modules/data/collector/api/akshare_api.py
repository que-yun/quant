from typing import Optional, Dict, Any
import pandas as pd
import akshare as ak
from datetime import datetime
from ..base.data_api_base import DataAPIBase


class AKShareAPI(DataAPIBase):
    """AKShare API实现类，提供标准化的数据获取接口"""

    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """获取股票列表
        Returns:
            Optional[pd.DataFrame]: 股票列表数据
        """
        try:
            # 使用akshare获取A股股票列表
            df = ak.stock_zh_a_spot_em()
            
            if df is None or df.empty:
                self.logger.error("获取股票列表数据为空")
                return None
            
            # 标准化数据格式
            df = df.rename(columns={
                '代码': 'symbol',
                '名称': 'name',
                '成交量': 'volume',
                '最新价': 'close',
                '涨跌幅': 'pct_change',
                '涨跌额': 'price_change',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '最高': 'high',
                '最低': 'low',
                '今开': 'open',
                '昨收': 'prev_close',
                '量比': 'volume_ratio',
                '换手率': 'turnover_rate',
                '市盈率-动态': 'pe_ratio',
                '市净率': 'pb_ratio',
                '总市值': 'market_cap',
                '流通市值': 'circulating_market_cap',
                '涨速': 'price_speed',
                '5分钟涨跌': '5min_change',
                '60日涨跌幅': '60day_pct_change',
                '年初至今涨跌幅': 'ytd_pct_change'
            })
            
            # 添加市场信息前缀
            df['symbol'] = df['symbol'].apply(lambda x: f"{'sh' if x.startswith('6') else 'sz'}{x}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {str(e)}")
            return None

    def _preprocess_symbol(self, symbol: str) -> str:
        """预处理股票代码，移除前缀
        Args:
            symbol: 股票代码
        Returns:
            str: 处理后的股票代码
        """
        if symbol.startswith('sh') or symbol.startswith('sz'):
            return symbol[2:]
        return symbol

    def get_stock_history(self, symbol: str, period: str = 'daily', start_date: str = None, end_date: str = None) -> \
    Optional[pd.DataFrame]:
        try:
            # 验证股票代码
            if not self._validate_symbol(symbol):
                self.logger.error(f"无效的股票代码格式：{symbol}")
                return None

            # 预处理股票代码
            processed_symbol = self._preprocess_symbol(symbol)
            # 获取数据
            df = ak.stock_zh_a_hist(
                symbol=processed_symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust='qfq'
            )

            if df is None or df.empty:
                return None

            # 标准化数据
            df = self._standardize_data(df, 'stock')
            df = self._add_market_info(df, 'symbol')

            return df

        except Exception as e:
            self.logger.error(f"获取历史行情数据失败: {str(e)}")
            return None

    def get_minute_data(self, symbol: str, freq: str, start_date: str = None, end_date: str = None) -> Optional[
        pd.DataFrame]:
        try:
            # 验证股票代码
            if not self._validate_symbol(symbol):
                self.logger.error(f"无效的股票代码格式：{symbol}")
                return None

            # 预处理股票代码
            processed_symbol = self._preprocess_symbol(symbol)

            # 获取数据
            df = ak.stock_zh_a_hist_min_em(
                symbol=processed_symbol,
                period=freq,
                start_date=start_date,
                end_date=end_date,
                adjust='qfq'
            )

            if df is None or df.empty:
                return None

            # 标准化数据
            df = self._standardize_data(df, 'stock')
            df = self._add_market_info(df, 'symbol')
            df['freq'] = freq

            return df

        except Exception as e:
            self.logger.error(f"获取分钟数据失败: {str(e)}")
            return None

    def get_index_data(self, symbol: str, period: str, start_date: str = None, end_date: str = None) -> Optional[Dict[str, Any]]:
        try:
            # 预处理指数代码
            processed_symbol = self._preprocess_symbol(symbol)

            df = ak.index_zh_a_hist(symbol=processed_symbol,period=period, start_date=start_date,end_date=end_date)

            if df is None or df.empty:
                return None

            # 标准化数据格式
            df = df.rename(columns={
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
                '换手率': 'turnover_rate'
            })

            # 确保日期格式正确
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df['symbol'] = symbol
            df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            return {
                'symbol': symbol,
                'data': df.to_dict('records')
            }

        except Exception as e:
            self.logger.error(f"获取指数数据失败: {str(e)}")
            return None