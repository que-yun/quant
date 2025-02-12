import pandas as pd
from abc import ABC, abstractmethod
from loguru import logger


class StrategyBase(ABC):
    """策略基类
    所有交易策略和选股策略都应继承自此基类
    """

    def __init__(self):
        self.logger = logger
        self.positions = {}
        self.cash = 0
        self.portfolio_value = 0

    @abstractmethod
    def initialize(self):
        """策略初始化"""
        pass

    @abstractmethod
    def process_data(self, data: pd.DataFrame):
        """数据处理"""
        pass

    @abstractmethod
    def generate_signals(self) -> dict:
        """生成交易信号"""
        pass

    def update_portfolio(self, signals: dict):
        """更新投资组合"""
        try:
            # 根据信号更新持仓
            for symbol, signal in signals.items():
                if signal['action'] == 'buy' and signal['amount'] > 0:
                    self._execute_buy(symbol, signal['amount'], signal['price'])
                elif signal['action'] == 'sell' and symbol in self.positions:
                    self._execute_sell(symbol, signal['amount'], signal['price'])

            # 更新组合价值
            self._update_portfolio_value()

        except Exception as e:
            self.logger.error(f"更新投资组合失败: {str(e)}")

    def _execute_buy(self, symbol: str, amount: float, price: float):
        """执行买入操作"""
        cost = amount * price
        if cost <= self.cash:
            if symbol in self.positions:
                self.positions[symbol]['amount'] += amount
                self.positions[symbol]['cost'] = (self.positions[symbol]['cost'] *
                                                  self.positions[symbol]['amount'] + cost) / (
                                                             self.positions[symbol]['amount'] + amount)
            else:
                self.positions[symbol] = {
                    'amount': amount,
                    'cost': price
                }
            self.cash -= cost
            self.logger.info(f"买入 {symbol}: {amount}股, 价格: {price}")

    def _execute_sell(self, symbol: str, amount: float, price: float):
        """执行卖出操作"""
        if amount >= self.positions[symbol]['amount']:
            self.cash += self.positions[symbol]['amount'] * price
            del self.positions[symbol]
        else:
            self.positions[symbol]['amount'] -= amount
            self.cash += amount * price
        self.logger.info(f"卖出 {symbol}: {amount}股, 价格: {price}")

    def _update_portfolio_value(self):
        """更新组合总价值"""
        total_value = self.cash
        for symbol, position in self.positions.items():
            total_value += position['amount'] * position['cost']
        self.portfolio_value = total_value
