import pandas as pd
import numpy as np
import talib
from sklearn.ensemble import IsolationForest

class TrendDetector:
    """趋势检测器
    用于检测市场趋势变化和异常波动
    """
    def __init__(self, data: pd.DataFrame):
        """
        data需包含以下列: ['close', 'high', 'low', 'volume']
        """
        self.data = data.copy()
        self._calc_indicators()
        self._status_history = []  # 用于记录历史状态
        
    def _calc_indicators(self):
        """核心指标计算"""
        # 数据验证
        required_columns = ['close', 'high', 'low', 'volume']
        if not all(col in self.data.columns for col in required_columns):
            raise ValueError(f"数据缺少必要的列: {required_columns}")
        
        # 使用可用的历史数据计算指标
        data_length = len(self.data)
        ma200_period = min(200, data_length)
        ma60_period = min(60, data_length)
        ma20_period = min(20, data_length)
        
        # 1. 趋势强度指标 - 优化MA计算，确保数据一致性
        close_series = self.data['close'].copy()
        self.data['ma200'] = talib.SMA(close_series, ma200_period).ffill()
        self.data['ma60'] = talib.SMA(close_series, ma60_period).ffill()
        self.data['adx'] = talib.ADX(self.data['high'], self.data['low'], self.data['close'], min(14, data_length)).ffill()
        
        # 2. 波动率指标
        self.data['atr'] = talib.ATR(self.data['high'], self.data['low'], self.data['close'], min(14, data_length)).ffill()
        self.data['natr'] = talib.NATR(self.data['high'], self.data['low'], self.data['close'], min(14, data_length)).ffill()
        
        # 3. 量能指标
        volume_series = self.data['volume'].copy()
        self.data['obv'] = talib.OBV(close_series, volume_series).ffill()
        self.data['volume_ma20'] = talib.SMA(volume_series, ma20_period).ffill()
        
        # 4. 市场情绪指标
        self.data['rsi'] = talib.RSI(close_series, min(14, data_length)).ffill()
        self.data['bull_bear_power'] = (talib.EMA(close_series, min(13, data_length)) - talib.EMA(close_series, min(26, data_length))).ffill()
        
        # 5. 异常检测模型
        self._add_anomaly_detection()
        
    def _add_anomaly_detection(self):
        """使用孤立森林检测异常波动"""
        model = IsolationForest(contamination=0.05, random_state=42)
        # 先填充缺失值，避免dropna()导致的索引不匹配问题
        features_data = self.data[['close', 'volume', 'atr', 'rsi']].ffill().bfill()
        self.data['anomaly'] = model.fit_predict(features_data)
        
    def get_market_status(self) -> str:
        """判断当前市场状态"""
        current = self.data.iloc[-1].copy()  # 创建当前数据的副本
        # 确保在数据不足时也能正常工作
        if 'ma200' not in self.data.columns:
            return 'range'  # 数据不足时返回震荡市场状态
            
        # 计算价格比值并缓存
        price_ratio = current['close'] / current['ma200']
        
        # 牛市判断条件
        bull_conditions = [
            price_ratio > 1.08,
            current['ma60'] > current['ma200'],
            current['adx'] > 25,
            current['obv'] > self.data['obv'].rolling(50).mean().iloc[-1]
        ]
        
        # 熊市判断条件
        bear_conditions = [
            price_ratio < 0.92,
            current['ma60'] < current['ma200'],
            current['adx'] > 30,
            current['volume_ma20'] > self.data['volume_ma20'].quantile(0.8)
        ]
        
        # 根据当前条件判断市场状态
        if sum(bull_conditions) >= 3:
            new_status = 'bull'
        elif sum(bear_conditions) >= 3:
            new_status = 'bear'
        else:
            new_status = 'range'
            
        # 更新状态历史
        self._status_history.append(new_status)
        return new_status

    def trend_change_alert(self) -> dict:
        """生成趋势转换预警信号"""
        recent = self.data.iloc[-30:]
        signals = {}
        
        # 信号1：ADX与波动率背离
        adx_trend = np.polyfit(range(5), recent['adx'].iloc[-5:], 1)[0]
        atr_trend = np.polyfit(range(5), recent['atr'].iloc[-5:], 1)[0]
        signals['divergence'] = (adx_trend * atr_trend) < 0
        
        # 信号2：量价异常组合
        vol_spike = recent['volume'].iloc[-1] > 2 * recent['volume_ma20'].iloc[-1]
        price_drop = recent['close'].iloc[-1] < 0.98 * recent['close'].iloc[-2]
        signals['volume_anomaly'] = vol_spike and price_drop
        
        # 信号3：牛熊力量逆转
        bull_power = recent['bull_bear_power'].iloc[-3:].mean()
        bull_trend = bull_power > 0
        last_power = recent['bull_bear_power'].iloc[-1]
        signals['power_reversal'] = (last_power * bull_power) < 0
        
        # 信号4：孤立森林异常点检测
        signals['anomaly_detected'] = recent['anomaly'][-3:].sum() <= -1
        
        return signals
    
    def composite_signal(self) -> bool:
        """综合预警信号"""
        signals = self.trend_change_alert()
        # 当满足任意两个关键信号时触发
        critical_signals = [
            signals['divergence'],
            signals['volume_anomaly'],
            signals['power_reversal']
        ]
        return sum(critical_signals) >= 2 and signals['anomaly_detected']