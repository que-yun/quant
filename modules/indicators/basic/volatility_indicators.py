import pandas as pd
import talib
from loguru import logger

class VolatilityIndicators:
    def __init__(self):
        self.logger = logger

    def calculate_bollinger_bands(self, close_prices, period=20, num_std=2):
        """计算布林带
        Args:
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
            num_std (int): 标准差倍数
        Returns:
            tuple: (上轨, 中轨, 下轨)
        """
        try:
            upper, middle, lower = talib.BBANDS(close_prices, 
                                               timeperiod=period,
                                               nbdevup=num_std,
                                               nbdevdn=num_std,
                                               matype=0)
            return upper, middle, lower
        except Exception as e:
            self.logger.error(f"计算布林带失败: {str(e)}")
            return None, None, None

    def calculate_atr(self, high_prices, low_prices, close_prices, period=14):
        """计算ATR(平均真实波幅)
        Args:
            high_prices (pd.Series): 最高价序列
            low_prices (pd.Series): 最低价序列
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
        Returns:
            pd.Series: ATR值
        """
        try:
            atr = talib.ATR(high_prices, low_prices, close_prices, timeperiod=period)
            return atr
        except Exception as e:
            self.logger.error(f"计算ATR指标失败: {str(e)}")
            return None

    def calculate_natr(self, high_prices, low_prices, close_prices, period=14):
        """计算NATR(归一化平均真实波幅)
        Args:
            high_prices (pd.Series): 最高价序列
            low_prices (pd.Series): 最低价序列
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
        Returns:
            pd.Series: NATR值
        """
        try:
            natr = talib.NATR(high_prices, low_prices, close_prices, timeperiod=period)
            return natr
        except Exception as e:
            self.logger.error(f"计算NATR指标失败: {str(e)}")
            return None

    def calculate_standard_deviation(self, close_prices, period=20):
        """计算标准差
        Args:
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
        Returns:
            pd.Series: 标准差值
        """
        try:
            stddev = talib.STDDEV(close_prices, timeperiod=period)
            return stddev
        except Exception as e:
            self.logger.error(f"计算标准差失败: {str(e)}")
            return None

    def calculate_trange(self, high_prices, low_prices, close_prices):
        """计算真实波幅
        Args:
            high_prices (pd.Series): 最高价序列
            low_prices (pd.Series): 最低价序列
            close_prices (pd.Series): 收盘价序列
        Returns:
            pd.Series: 真实波幅值
        """
        try:
            trange = talib.TRANGE(high_prices, low_prices, close_prices)
            return trange
        except Exception as e:
            self.logger.error(f"计算真实波幅失败: {str(e)}")
            return None