import pandas as pd
import talib
from loguru import logger

class VolumeIndicators:
    def __init__(self):
        self.logger = logger

    def calculate_obv(self, close_prices, volume):
        """计算OBV(能量潮)
        Args:
            close_prices (pd.Series): 收盘价序列
            volume (pd.Series): 成交量序列
        Returns:
            pd.Series: OBV值
        """
        try:
            obv = talib.OBV(close_prices, volume)
            return obv
        except Exception as e:
            self.logger.error(f"计算OBV指标失败: {str(e)}")
            return None

    def calculate_ad(self, high_prices, low_prices, close_prices, volume):
        """计算A/D(累积/派发线)
        Args:
            high_prices (pd.Series): 最高价序列
            low_prices (pd.Series): 最低价序列
            close_prices (pd.Series): 收盘价序列
            volume (pd.Series): 成交量序列
        Returns:
            pd.Series: A/D值
        """
        try:
            ad = talib.AD(high_prices, low_prices, close_prices, volume)
            return ad
        except Exception as e:
            self.logger.error(f"计算A/D指标失败: {str(e)}")
            return None

    def calculate_adosc(self, high_prices, low_prices, close_prices, volume, 
                       fastperiod=3, slowperiod=10):
        """计算ADOSC(震荡指标)
        Args:
            high_prices (pd.Series): 最高价序列
            low_prices (pd.Series): 最低价序列
            close_prices (pd.Series): 收盘价序列
            volume (pd.Series): 成交量序列
            fastperiod (int): 快周期
            slowperiod (int): 慢周期
        Returns:
            pd.Series: ADOSC值
        """
        try:
            adosc = talib.ADOSC(high_prices, low_prices, close_prices, volume,
                               fastperiod=fastperiod, slowperiod=slowperiod)
            return adosc
        except Exception as e:
            self.logger.error(f"计算ADOSC指标失败: {str(e)}")
            return None

    def calculate_cmf(self, high_prices, low_prices, close_prices, volume, period=20):
        """计算CMF(钱德动量指标)
        Args:
            high_prices (pd.Series): 最高价序列
            low_prices (pd.Series): 最低价序列
            close_prices (pd.Series): 收盘价序列
            volume (pd.Series): 成交量序列
            period (int): 计算周期
        Returns:
            pd.Series: CMF值
        """
        try:
            # 计算资金流量乘数
            mf_multiplier = ((close_prices - low_prices) - (high_prices - close_prices)) / (high_prices - low_prices)
            mf_volume = mf_multiplier * volume
            
            # 计算CMF
            cmf = mf_volume.rolling(window=period).sum() / volume.rolling(window=period).sum()
            return cmf
        except Exception as e:
            self.logger.error(f"计算CMF指标失败: {str(e)}")
            return None

    def calculate_vwap(self, high_prices, low_prices, close_prices, volume, period=14):
        """计算VWAP(成交量加权平均价格)
        Args:
            high_prices (pd.Series): 最高价序列
            low_prices (pd.Series): 最低价序列
            close_prices (pd.Series): 收盘价序列
            volume (pd.Series): 成交量序列
            period (int): 计算周期
        Returns:
            pd.Series: VWAP值
        """
        try:
            typical_price = (high_prices + low_prices + close_prices) / 3
            vwap = (typical_price * volume).rolling(window=period).sum() / volume.rolling(window=period).sum()
            return vwap
        except Exception as e:
            self.logger.error(f"计算VWAP指标失败: {str(e)}")
            return None