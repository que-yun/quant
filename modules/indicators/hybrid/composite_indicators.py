import pandas as pd
import talib
from ..basic.base_indicators import BaseIndicators

class CompositeIndicators(BaseIndicators):
    """混合指标类
    包含由多个基础指标组合而成的复合指标，如KDJ、BOLL等
    """
    def __init__(self):
        super().__init__()

    def calculate_kdj(self, high_prices, low_prices, close_prices, 
                      fastk_period=9, slowk_period=3, slowd_period=3):
        """计算KDJ指标
        Args:
            high_prices (pd.Series): 最高价序列
            low_prices (pd.Series): 最低价序列
            close_prices (pd.Series): 收盘价序列
            fastk_period (int): 快速K线周期
            slowk_period (int): 慢速K线周期
            slowd_period (int): 慢速D线周期
        Returns:
            tuple: (K值, D值, J值)
        """
        cache_key = self._get_cache_key('kdj', high_prices.values.tobytes(), 
                                      low_prices.values.tobytes(),
                                      close_prices.values.tobytes(),
                                      fastk_period, slowk_period, slowd_period)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            k, d = talib.STOCH(high_prices, low_prices, close_prices,
                              fastk_period=fastk_period,
                              slowk_period=slowk_period,
                              slowk_matype=0,
                              slowd_period=slowd_period,
                              slowd_matype=0)
            j = 3 * k - 2 * d
            result = (k, d, j)
            self._cache[cache_key] = result
            return result
        except Exception as e:
            self.logger.error(f"计算KDJ指标失败: {str(e)}")
            return None, None, None

    def calculate_bollinger_bands(self, close_prices, period=20, num_std=2):
        """计算布林带
        Args:
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
            num_std (int): 标准差倍数
        Returns:
            tuple: (上轨, 中轨, 下轨)
        """
        cache_key = self._get_cache_key('boll', close_prices.values.tobytes(), period, num_std)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            upper, middle, lower = talib.BBANDS(close_prices, 
                                               timeperiod=period,
                                               nbdevup=num_std,
                                               nbdevdn=num_std,
                                               matype=0)
            result = (upper, middle, lower)
            self._cache[cache_key] = result
            return result
        except Exception as e:
            self.logger.error(f"计算布林带失败: {str(e)}")
            return None, None, None

    def calculate_stochrsi(self, close_prices, timeperiod=14, fastk_period=5, fastd_period=3):
        """计算StochRSI指标
        Args:
            close_prices (pd.Series): 收盘价序列
            timeperiod (int): RSI周期
            fastk_period (int): 快速K线周期
            fastd_period (int): 快速D线周期
        Returns:
            tuple: (K值, D值)
        """
        cache_key = self._get_cache_key('stochrsi', close_prices.values.tobytes(), 
                                      timeperiod, fastk_period, fastd_period)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            fastk, fastd = talib.STOCHRSI(close_prices, 
                                          timeperiod=timeperiod,
                                          fastk_period=fastk_period,
                                          fastd_period=fastd_period,
                                          fastd_matype=0)
            result = (fastk, fastd)
            self._cache[cache_key] = result
            return result
        except Exception as e:
            self.logger.error(f"计算StochRSI指标失败: {str(e)}")
            return None, None