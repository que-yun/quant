import pandas as pd
import talib
from loguru import logger

class TrendIndicators:
    def __init__(self):
        self.logger = logger

    def calculate_ma(self, close_prices, periods=[5, 10, 20, 30, 60]):
        """计算移动平均线
        Args:
            close_prices (pd.Series): 收盘价序列
            periods (list): MA的周期列表
        Returns:
            dict: 不同周期的MA值
        """
        try:
            ma_dict = {}
            for period in periods:
                ma = talib.MA(close_prices, timeperiod=period)
                ma_dict[f'MA{period}'] = ma
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
        try:
            ema_dict = {}
            for period in periods:
                ema = talib.EMA(close_prices, timeperiod=period)
                ema_dict[f'EMA{period}'] = ema
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
        try:
            macd, signal, hist = talib.MACD(close_prices, 
                                           fastperiod=fast_period,
                                           slowperiod=slow_period,
                                           signalperiod=signal_period)
            return macd, signal, hist
        except Exception as e:
            self.logger.error(f"计算MACD指标失败: {str(e)}")
            return None, None, None

    def calculate_dema(self, close_prices, period=30):
        """计算双重指数移动平均线
        Args:
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
        Returns:
            pd.Series: DEMA值
        """
        try:
            dema = talib.DEMA(close_prices, timeperiod=period)
            return dema
        except Exception as e:
            self.logger.error(f"计算DEMA指标失败: {str(e)}")
            return None

    def calculate_tema(self, close_prices, period=30):
        """计算三重指数移动平均线
        Args:
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
        Returns:
            pd.Series: TEMA值
        """
        try:
            tema = talib.TEMA(close_prices, timeperiod=period)
            return tema
        except Exception as e:
            self.logger.error(f"计算TEMA指标失败: {str(e)}")
            return None

    def calculate_wma(self, close_prices, period=30):
        """计算加权移动平均线
        Args:
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
        Returns:
            pd.Series: WMA值
        """
        try:
            wma = talib.WMA(close_prices, timeperiod=period)
            return wma
        except Exception as e:
            self.logger.error(f"计算WMA指标失败: {str(e)}")
            return None