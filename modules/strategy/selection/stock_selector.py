import pandas as pd
import numpy as np
from ..base.strategy_base import StrategyBase
from ...indicators.screening.screening_indicators import ScreeningIndicators


class StockSelector(StrategyBase):
    """股票选择器
    基于技术指标和基本面指标的股票筛选策略
    """

    def __init__(self):
        super().__init__()
        self.screening = ScreeningIndicators()
        self.selected_stocks = []

    def initialize(self):
        """策略初始化"""
        self.logger.info("初始化股票选择器")

    def process_data(self, data: pd.DataFrame):
        """处理股票数据
        Args:
            data: DataFrame, 包含['close', 'high', 'low', 'volume']等列的股票数据
        """
        try:
            # 计算技术指标
            close_prices = data['close']
            volume = data['volume']
            high_prices = data['high']
            low_prices = data['low']

            # 计算价量关系指标
            self.price_volume_ratio = self.screening.calculate_price_volume_ratio(
                close_prices, volume)

            # 计算趋势强度指标
            self.trend_strength = self.screening.calculate_trend_strength(
                close_prices, high_prices, low_prices)

            # 计算成交量突破指标
            self.volume_breakout = self.screening.calculate_volume_breakout(volume)

            # 计算动量排名
            self.momentum_rank = self.screening.calculate_momentum_rank(close_prices)

        except Exception as e:
            self.logger.error(f"处理股票数据失败: {str(e)}")

    def generate_signals(self) -> dict:
        """生成选股信号
        Returns:
            dict: 选股信号字典
            {
                'symbol': {
                    'score': float,  # 综合评分
                    'indicators': {  # 指标详情
                        'price_volume': float,
                        'trend_strength': float,
                        'volume_breakout': bool,
                        'momentum_rank': float
                    }
                }
            }
        """
        signals = {}
        try:
            # 确保所有指标都已计算
            if any(x is None for x in [self.price_volume_ratio, self.trend_strength,
                                       self.volume_breakout, self.momentum_rank]):
                return signals

            # 获取最新的指标值
            latest_pv = self.price_volume_ratio.iloc[-1]
            latest_ts = self.trend_strength.iloc[-1]
            latest_vb = self.volume_breakout.iloc[-1]
            latest_mr = self.momentum_rank.iloc[-1]

            # 生成综合评分
            score = 0
            if not np.isnan(latest_pv):
                score += latest_pv * 0.3
            if not np.isnan(latest_ts):
                score += latest_ts * 0.3
            if latest_vb:
                score += 0.2
            if not np.isnan(latest_mr):
                score += latest_mr * 0.2

            signals['stock'] = {
                'score': score,
                'indicators': {
                    'price_volume': latest_pv,
                    'trend_strength': latest_ts,
                    'volume_breakout': latest_vb,
                    'momentum_rank': latest_mr
                }
            }

        except Exception as e:
            self.logger.error(f"生成选股信号失败: {str(e)}")

        return signals
