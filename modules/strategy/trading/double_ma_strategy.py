import pandas as pd
import talib
from ..base.strategy_base import StrategyBase


class DoubleMAStrategy(StrategyBase):
    """双均线交易策略
    使用快速和慢速移动平均线的交叉来生成交易信号
    """

    def __init__(self, fast_period=5, slow_period=20):
        super().__init__()
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.fast_ma = None
        self.slow_ma = None

    def initialize(self):
        """策略初始化"""
        self.logger.info(f"初始化双均线策略: 快线周期={self.fast_period}, 慢线周期={self.slow_period}")

    def process_data(self, data: pd.DataFrame):
        """计算技术指标"""
        try:
            close_prices = data['close']
            self.fast_ma = talib.SMA(close_prices, timeperiod=self.fast_period)
            self.slow_ma = talib.SMA(close_prices, timeperiod=self.slow_period)
        except Exception as e:
            self.logger.error(f"计算移动平均线失败: {str(e)}")

    def generate_signals(self) -> dict:
        """生成交易信号
        Returns:
            dict: 交易信号字典
            {
                'symbol': {
                    'action': 'buy'/'sell',
                    'amount': float,
                    'price': float
                }
            }
        """
        signals = {}
        try:
            # 确保有足够的数据
            if self.fast_ma is None or self.slow_ma is None:
                return signals

            # 获取最新的均线值
            current_fast = self.fast_ma.iloc[-1]
            current_slow = self.slow_ma.iloc[-1]
            prev_fast = self.fast_ma.iloc[-2]
            prev_slow = self.slow_ma.iloc[-2]

            # 判断均线交叉
            golden_cross = prev_fast <= prev_slow and current_fast > current_slow
            death_cross = prev_fast >= prev_slow and current_fast < current_slow

            # 生成交易信号
            if golden_cross:
                signals['signal'] = {
                    'action': 'buy',
                    'amount': 100,  # 默认交易数量
                    'price': current_fast
                }
            elif death_cross:
                signals['signal'] = {
                    'action': 'sell',
                    'amount': 100,  # 默认交易数量
                    'price': current_fast
                }

        except Exception as e:
            self.logger.error(f"生成交易信号失败: {str(e)}")

        return signals
