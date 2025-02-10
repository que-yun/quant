from loguru import logger
from modules.strategy.strategy_base import StrategyBase
import pandas as pd

class DoubleMAStrategy(StrategyBase):
    """分钟级双均线策略示例"""
    def __init__(self, data=None, fast_period=5, slow_period=10, initial_capital=1000000):
        super().__init__(data=data, initial_capital=initial_capital)
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.max_position_ratio = 0.4  # 单只股票最大仓位比例
        logger.info(f"初始化双均线策略，快线周期：{fast_period}，慢线周期：{slow_period}，初始资金：{initial_capital}")
        
    def handle_data(self, symbol, current_data):
        try:
            # 计算快速和慢速移动平均线
            close_prices = current_data['close']
            ma_fast = close_prices.rolling(window=self.fast_period).mean()
            ma_slow = close_prices.rolling(window=self.slow_period).mean()
            
            if len(ma_fast) < self.slow_period:
                return 0, 0
            
            current_price = close_prices.iloc[-1]
            current_date = current_data.index[-1]
            position = self.get_position(symbol)
            
            # 金叉：快线在慢线之上
            if ma_fast.iloc[-1] > ma_slow.iloc[-1]:
                if position['volume'] == 0:
                    # 计算可买入数量，提高资金利用率
                    available_capital = self.get_current_capital() * self.max_position_ratio
                    shares = int(available_capital / current_price)
                    if shares > 0:
                        return 1, shares
            
            # 死叉：快线在慢线之下
            elif ma_fast.iloc[-1] < ma_slow.iloc[-1]:
                current_volume = position['volume']
                if current_volume > 0:
                    # 确保卖出数量不超过当前持仓量
                    return -1, current_volume
            
            # 止损：当前价格低于买入价格的5%
            elif position['volume'] > 0 and position['cost'] > 0:
                avg_cost = position['cost'] / position['volume']
                if current_price < avg_cost * 0.95:
                    return -1, position['volume']
            
            return 0, 0
            
        except Exception as e:
            logger.error(f"策略执行出错：{str(e)}")
            return 0, 0
