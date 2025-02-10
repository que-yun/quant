import pandas as pd
import numpy as np
from loguru import logger

class StrategyBase:
    def __init__(self, data, initial_capital=100000):
        """策略基类
        Args:
            data (dict): 股票数据字典，key为股票代码，value为DataFrame
            initial_capital (float): 初始资金
        """
        self.data = data
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.positions = {}
        self.trades = []
        self.records = self.trades  # 统一使用trades作为交易记录
        self.logger = logger

    def initialize(self):
        """初始化策略，在回测开始前调用"""
        pass

    def handle_data(self, symbol, current_data):
        """处理每个交易日的数据，由子类实现具体的交易逻辑
        Args:
            symbol (str): 股票代码
            current_data (pd.DataFrame): 当前交易日的数据
        Returns:
            tuple: (交易信号, 交易数量)
            交易信号: 1表示买入，-1表示卖出，0表示持有
        """
        raise NotImplementedError("子类必须实现handle_data方法")

    def buy(self, symbol, price, volume, date):
        """买入操作
        Args:
            symbol (str): 股票代码
            price (float): 买入价格
            volume (int): 买入数量
            date (str): 交易日期
        Returns:
            bool: 交易是否成功
        """
        try:
            amount = price * volume
            commission = self.calculate_commission(amount)
            total_cost = amount + commission

            if total_cost > self.current_capital:
                self.logger.warning(f"资金不足，无法买入{symbol}")
                return False

            # 更新持仓和资金
            if symbol not in self.positions:
                self.positions[symbol] = {'volume': 0, 'cost': 0}
            
            self.positions[symbol]['volume'] += volume
            self.positions[symbol]['cost'] += amount
            self.current_capital -= total_cost

            # 记录交易到trades和records
            # 使用传入的date参数作为交易时间
            trade_time = pd.Timestamp(date).strftime('%Y-%m-%d %H:%M:%S')
            trade_record = {
                'date': date,
                'time': trade_time,  # 使用格式化后的时间戳
                'symbol': symbol,
                'type': 'buy',
                'price': price,
                'volume': volume,
                'amount': amount,
                'commission': commission
            }
            self.trades.append(trade_record)
            self.records.append(trade_record)

            self.logger.info(f"买入{symbol} {volume}股，价格{price}，总成本{total_cost}")
            return True

        except Exception as e:
            self.logger.error(f"买入操作失败: {str(e)}")
            return False

    def sell(self, symbol, price, volume, date):
        """卖出操作
        Args:
            symbol (str): 股票代码
            price (float): 卖出价格
            volume (int): 卖出数量
            date (str): 交易日期
        Returns:
            bool: 交易是否成功
        """
        try:
            # 检查持仓是否存在
            if symbol not in self.positions:
                self.logger.warning(f"没有{symbol}的持仓，无法卖出")
                return False
                
            # 检查卖出数量是否超过持仓量
            current_volume = self.positions[symbol]['volume']
            if current_volume < volume:
                self.logger.warning(f"持仓不足，当前持仓{current_volume}股，无法卖出{volume}股")
                return False

            amount = price * volume
            commission = self.calculate_commission(amount)
            net_income = amount - commission

            # 计算收益
            avg_cost = self.positions[symbol]['cost'] / self.positions[symbol]['volume']
            profit = (price - avg_cost) * volume - commission

            # 更新持仓和资金
            self.positions[symbol]['volume'] -= volume
            if self.positions[symbol]['volume'] == 0:
                self.positions[symbol]['cost'] = 0
            else:
                self.positions[symbol]['cost'] *= (1 - volume/self.positions[symbol]['volume'])
            self.current_capital += net_income

            # 记录交易到trades和records
            # 记录精确的交易时间
            # 使用回测时间作为交易时间
            trade_time = pd.Timestamp(date).strftime('%Y-%m-%d %H:%M:%S')
            trade_record = {
                'date': date,
                'time': trade_time,  # 使用格式化后的时间戳
                'symbol': symbol,
                'type': 'sell',
                'price': price,
                'volume': volume,
                'amount': amount,
                'commission': commission,
                'profit': profit
            }
            self.trades.append(trade_record)
            self.records.append(trade_record)

            self.logger.info(f"卖出{symbol} {volume}股，价格{price}，净收入{net_income}，收益{profit}")
            return True

        except Exception as e:
            self.logger.error(f"卖出操作失败: {str(e)}")
            return False

    def calculate_commission(self, amount):
        """计算交易佣金
        Args:
            amount (float): 交易金额
        Returns:
            float: 佣金
        """
        # 默认佣金率为0.0003，最低5元
        commission = max(amount * 0.0003, 5)
        return commission

    def get_position(self, symbol):
        """获取持仓信息
        Args:
            symbol (str): 股票代码
        Returns:
            dict: 持仓信息
        """
        return self.positions.get(symbol, {'volume': 0, 'cost': 0})

    def get_current_capital(self):
        """获取当前资金"""
        return self.current_capital

    def get_total_value(self, current_prices):
        """计算当前总资产
        Args:
            current_prices (dict): 当前价格字典，key为股票代码，value为当前价格
        Returns:
            float: 总资产
        """
        total_value = self.current_capital
        for symbol, position in self.positions.items():
            if symbol in current_prices:
                total_value += position['volume'] * current_prices[symbol]
        return total_value

    def get_trade_history(self):
        """获取交易历史"""
        return pd.DataFrame(self.trades)

    def calculate_returns(self):
        """计算策略收益率
        Returns:
            dict: 策略表现指标
        """
        if not self.trades:
            return None

        trade_df = pd.DataFrame(self.trades)
        trade_df['profit'] = 0

        # 计算每笔交易的盈亏
        for symbol in trade_df['symbol'].unique():
            symbol_trades = trade_df[trade_df['symbol'] == symbol].copy()
            symbol_trades['profit'] = np.where(
                symbol_trades['type'] == 'sell',  # 修改action为type
                symbol_trades['amount'] - symbol_trades['commission'],
                -(symbol_trades['amount'] + symbol_trades['commission'])
            )
            trade_df.loc[symbol_trades.index, 'profit'] = symbol_trades['profit']

        # 计算策略表现指标
        total_profit = trade_df['profit'].sum()
        win_trades = trade_df[trade_df['profit'] > 0]
        lose_trades = trade_df[trade_df['profit'] < 0]

        results = {
            'total_trades': len(trade_df),
            'win_trades': len(win_trades),
            'lose_trades': len(lose_trades),
            'win_rate': len(win_trades) / len(trade_df) if len(trade_df) > 0 else 0,
            'total_profit': total_profit,
            'final_capital': self.current_capital,
            'return_rate': (self.current_capital - self.initial_capital) / self.initial_capital
        }

        return results