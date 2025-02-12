import pandas as pd
import talib
from loguru import logger

class BaseIndicators:
    """基础技术指标类
    包含最基本的技术分析指标，如移动平均线、MACD等
    """
    def __init__(self):
        self.logger = logger
        self._cache = {}

    def _get_cache_key(self, func_name, *args, **kwargs):
        """生成缓存键"""
        return f"{func_name}_{hash(str(args) + str(kwargs))}"

    def calculate_ma(self, close_prices, periods=[5, 10, 20, 30, 60]):
        """计算移动平均线
        Args:
            close_prices (pd.Series): 收盘价序列
            periods (list): MA的周期列表
        Returns:
            dict: 不同周期的MA值
        """
        cache_key = self._get_cache_key('ma', close_prices.values.tobytes(), periods)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            ma_dict = {}
            for period in periods:
                ma = talib.MA(close_prices, timeperiod=period)
                ma_dict[f'MA{period}'] = ma
            self._cache[cache_key] = ma_dict
            return ma_dict
        except Exception as e:
            self.logger.error(f"计算MA指标失败: {str(e)}")
            return None

    def calculate_ema(self, close_prices, periods=[5, 10, 20, 30, 60]):
        """计算指数移动平均线
        Args:
            close_prices (pd.Series): 收盘价序列
            periods (list): EMA的周期列表
        Returns:
            dict: 不同周期的EMA值
        """
        cache_key = self._get_cache_key('ema', close_prices.values.tobytes(), periods)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            ema_dict = {}
            for period in periods:
                ema = talib.EMA(close_prices, timeperiod=period)
                ema_dict[f'EMA{period}'] = ema
            self._cache[cache_key] = ema_dict
            return ema_dict
        except Exception as e:
            self.logger.error(f"计算EMA指标失败: {str(e)}")
            return None

    def calculate_macd(self, close_prices, fast_period=12, slow_period=26, signal_period=9):
        """计算MACD指标
        Args:
            close_prices (pd.Series): 收盘价序列
            fast_period (int): 快线周期
            slow_period (int): 慢线周期
            signal_period (int): 信号线周期
        Returns:
            tuple: (MACD线, 信号线, MACD柱状图)
        """
        cache_key = self._get_cache_key('macd', close_prices.values.tobytes(), fast_period, slow_period, signal_period)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            macd, signal, hist = talib.MACD(close_prices, 
                                           fastperiod=fast_period,
                                           slowperiod=slow_period,
                                           signalperiod=signal_period)
            result = (macd, signal, hist)
            self._cache[cache_key] = result
            return result
        except Exception as e:
            self.logger.error(f"计算MACD指标失败: {str(e)}")
            return None, None, None

    def calculate_rsi(self, close_prices, period=14):
        """计算RSI指标
        Args:
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
        Returns:
            pd.Series: RSI值
        """
        cache_key = self._get_cache_key('rsi', close_prices.values.tobytes(), period)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            rsi = talib.RSI(close_prices, timeperiod=period)
            self._cache[cache_key] = rsi
            return rsi
        except Exception as e:
            self.logger.error(f"计算RSI指标失败: {str(e)}")
            return None