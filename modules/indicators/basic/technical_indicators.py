import pandas as pd
import talib
from loguru import logger

class TechnicalIndicators:
    def __init__(self):
        self.logger = logger

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

    def calculate_rsi(self, close_prices, period=14):
        """计算RSI指标
        Args:
            close_prices (pd.Series): 收盘价序列
            period (int): 计算周期
        Returns:
            pd.Series: RSI值
        """
        try:
            rsi = talib.RSI(close_prices, timeperiod=period)
            return rsi
        except Exception as e:
            self.logger.error(f"计算RSI指标失败: {str(e)}")
            return None

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
        try:
            k, d = talib.STOCH(high_prices, low_prices, close_prices,
                              fastk_period=fastk_period,
                              slowk_period=slowk_period,
                              slowk_matype=0,
                              slowd_period=slowd_period,
                              slowd_matype=0)
            j = 3 * k - 2 * d
            return k, d, j
        except Exception as e:
            self.logger.error(f"计算KDJ指标失败: {str(e)}")
            return None, None, None

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

    def calculate_all(self, data):
        """计算所有技术指标
        Args:
            data (dict): 股票数据字典，key为股票代码，value为DataFrame
        Returns:
            dict: 包含技术指标的股票数据字典
        """
        try:
            for symbol, df in data.items():
                # 计算MACD
                macd, signal, hist = self.calculate_macd(df['close'])
                df['MACD'] = macd
                df['MACD_signal'] = signal
                df['MACD_hist'] = hist

                # 计算RSI
                df['RSI'] = self.calculate_rsi(df['close'])

                # 计算KDJ
                k, d, j = self.calculate_kdj(df['high'], df['low'], df['close'])
                df['K'] = k
                df['D'] = d
                df['J'] = j

                # 计算MA
                ma_dict = self.calculate_ma(df['close'])
                for ma_name, ma_values in ma_dict.items():
                    df[ma_name] = ma_values

                # 计算布林带
                upper, middle, lower = self.calculate_bollinger_bands(df['close'])
                df['BB_upper'] = upper
                df['BB_middle'] = middle
                df['BB_lower'] = lower

                self.logger.info(f"成功计算{symbol}的技术指标")

            return data

        except Exception as e:
            self.logger.error(f"计算技术指标失败: {str(e)}")
            return None