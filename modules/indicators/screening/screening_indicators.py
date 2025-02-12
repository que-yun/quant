import pandas as pd
import talib
from ..basic.base_indicators import BaseIndicators

class ScreeningIndicators(BaseIndicators):
    """选股指标类
    包含用于股票筛选的技术指标，如量价关系、趋势强度等
    """
    def __init__(self):
        super().__init__()

    def calculate_price_volume_ratio(self, close_prices, volume, period=20):
        """计算价量关系指标
        Args:
            close_prices (pd.Series): 收盘价序列
            volume (pd.Series): 成交量序列
            period (int): 计算周期
        Returns:
            pd.Series: 价量比值
        """
        cache_key = self._get_cache_key('pvr', close_prices.values.tobytes(), 
                                      volume.values.tobytes(), period)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            price_ma = talib.SMA(close_prices, timeperiod=period)
            volume_ma = talib.SMA(volume, timeperiod=period)
            pvr = price_ma / volume_ma
            self._cache[cache_key] = pvr
            return pvr
        except Exception as e:
            self.logger.error(f"计算价量关系指标失败: {str(e)}")
            return None

    def calculate_trend_strength(self, close_prices, high_prices, low_prices, period=20):
        """计算趋势强度指标
        Args:
            close_prices (pd.Series): 收盘价序列
            high_prices (pd.Series): 最高价序列
            low_prices (pd.Series): 最低价序列
            period (int): 计算周期
        Returns:
            pd.Series: 趋势强度值
        """
        cache_key = self._get_cache_key('trend_strength', close_prices.values.tobytes(),
                                      high_prices.values.tobytes(),
                                      low_prices.values.tobytes(), period)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            # 计算真实波幅
            tr = talib.TRANGE(high_prices, low_prices, close_prices)
            # 计算方向变动值
            plus_dm = talib.PLUS_DM(high_prices, low_prices, timeperiod=period)
            minus_dm = talib.MINUS_DM(high_prices, low_prices, timeperiod=period)
            # 计算趋势强度
            strength = (plus_dm - minus_dm) / tr
            self._cache[cache_key] = strength
            return strength
        except Exception as e:
            self.logger.error(f"计算趋势强度指标失败: {str(e)}")
            return None

    def calculate_volume_breakout(self, volume, period=20, threshold=2.0):
        """计算成交量突破指标
        Args:
            volume (pd.Series): 成交量序列
            period (int): 计算周期
            threshold (float): 突破阈值倍数
        Returns:
            pd.Series: 成交量突破信号
        """
        cache_key = self._get_cache_key('volume_breakout', volume.values.tobytes(),
                                      period, threshold)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            volume_ma = talib.SMA(volume, timeperiod=period)
            breakout = volume / volume_ma > threshold
            self._cache[cache_key] = breakout
            return breakout
        except Exception as e:
            self.logger.error(f"计算成交量突破指标失败: {str(e)}")
            return None

    def calculate_momentum_rank(self, close_prices, period=20):
        """计算动量排名指标
        Args:
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
        Returns:
            pd.Series: 动量排名值
        """
        cache_key = self._get_cache_key('momentum_rank', close_prices.values.tobytes(), period)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            # 计算价格变化率
            roc = talib.ROC(close_prices, timeperiod=period)
            # 计算相对强弱指标
            rsi = talib.RSI(close_prices, timeperiod=period)
            # 综合评分
            rank = (roc + rsi) / 2
            self._cache[cache_key] = rank
            return rank
        except Exception as e:
            self.logger.error(f"计算动量排名指标失败: {str(e)}")
            return None