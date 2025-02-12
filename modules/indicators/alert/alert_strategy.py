import pandas as pd
from loguru import logger
from .alert_indicators import TrendDetector

class MarketAlert:
    """市场告警策略类
    用于实现市场状态监控和趋势变化告警
    """
    def __init__(self):
        self.logger = logger
        self.last_status_change_date = None
        self.status_history = []  # 添加状态历史记录
        self.current_status = 'range'  # 初始状态设为震荡

    def run_alert(self, data: pd.DataFrame) -> dict:
        """运行市场告警检测
        Args:
            data: DataFrame, 上证指数日线数据，包含['close', 'high', 'low', 'volume']列
        Returns:
            dict: 告警信息字典
        """
        try:
            detector = TrendDetector(data)
            market_status = detector.get_market_status()
            signals = detector.trend_change_alert()
            is_alert = detector.composite_signal()
            
            # 检查当前日期是否已经发生过状态变化
            current_date = data.index[-1].strftime('%Y-%m-%d')
            
            # 计算并缓存关键指标
            current_price = data['close'].iloc[-1]
            current_ma200 = detector.data['ma200'].iloc[-1]
            price_ratio = current_price / current_ma200 if current_ma200 != 0 else 0
            ma_trend = 'up' if detector.data['ma60'].iloc[-1] > detector.data['ma200'].iloc[-1] else 'down'
            volume_ratio = detector.data['volume'].iloc[-1] / detector.data['volume_ma20'].iloc[-1] if detector.data['volume_ma20'].iloc[-1] != 0 else 0
            
            # 检查状态是否发生变化
            status_changed = market_status != self.current_status
            status_change_info = None
            
            if status_changed and (self.last_status_change_date is None or current_date != self.last_status_change_date):
                self.last_status_change_date = current_date
                self.current_status = market_status  # 更新当前状态
                
                # 记录状态变化信息
                status_change_info = {
                    'from': self.status_history[-1] if self.status_history else 'range',
                    'to': market_status,
                    'date': current_date,
                    'key_indicators': {
                        'price_ratio': price_ratio,
                        'ma_trend': ma_trend,
                        'adx': detector.data['adx'].iloc[-1],
                        'volume_level': f"{volume_ratio:.2f}x"
                    }
                }
                
                # 更新状态历史
                self.status_history.append(market_status)
                
                # 记录状态变化日志
                self.logger.warning(f"市场状态发生变化! 日期: {current_date}")
                self.logger.warning(f"状态转换: {status_change_info['from']} -> {market_status}")
                self.logger.warning("关键指标变化:")
                self.logger.warning(f"- 价格/MA200比值: {price_ratio:.2f}")
                self.logger.warning(f"- MA趋势: {ma_trend}")
                self.logger.warning(f"- ADX指标: {status_change_info['key_indicators']['adx']:.2f}")
                self.logger.warning(f"- 成交量水平: {status_change_info['key_indicators']['volume_level']}")
            
            alert_info = {
                'timestamp': data.index[-1],
                'market_status': market_status,
                'signals': signals,
                'alert_triggered': is_alert,
                'current_price': current_price,
                'price_change': (current_price / data['close'].iloc[-2] - 1) * 100,
                'status_change': status_change_info
            }
            
            # 趋势信号告警
            if is_alert:
                self.logger.warning(f"市场趋势告警触发! 日期: {current_date}")
                self.logger.warning(f"当前市场状态: {market_status}")
                self.logger.warning(f"触发信号: {signals}")
            
            return alert_info
            
        except Exception as e:
            self.logger.error(f"运行市场告警检测失败: {str(e)}")
            return None