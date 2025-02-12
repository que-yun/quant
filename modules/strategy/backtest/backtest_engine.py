import pandas as pd
from loguru import logger
from typing import Dict
from ..base.strategy_base import StrategyBase

class BacktestEngine:
    """回测引擎
    用于执行策略回测和性能评估
    """
    def __init__(self, strategy: StrategyBase, initial_capital: float = 1000000):
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.logger = logger
        self.performance_metrics = {}
        self.trade_history = []
        
    def run(self, data: pd.DataFrame) -> Dict:
        """运行回测
        Args:
            data: DataFrame, 回测数据，包含OHLCV等必要列
        Returns:
            Dict: 回测结果，包含收益率、夏普比率等指标
        """
        try:
            self.strategy.cash = self.initial_capital
            self.strategy.initialize()
            
            for timestamp, row in data.iterrows():
                # 处理当前数据
                current_data = pd.DataFrame([row])
                self.strategy.process_data(current_data)
                
                # 生成交易信号
                signals = self.strategy.generate_signals()
                
                # 更新投资组合
                if signals:
                    self.strategy.update_portfolio(signals)
                    self._record_trade(timestamp, signals)
            
            # 计算回测指标
            self._calculate_metrics()
            
            return self.performance_metrics
            
        except Exception as e:
            self.logger.error(f"回测执行失败: {str(e)}")
            return {}
    
    def _record_trade(self, timestamp: pd.Timestamp, signals: Dict):
        """记录交易历史"""
        for symbol, signal in signals.items():
            trade_record = {
                'timestamp': timestamp,
                'symbol': symbol,
                'action': signal['action'],
                'amount': signal['amount'],
                'price': signal['price'],
                'value': signal['amount'] * signal['price']
            }
            self.trade_history.append(trade_record)
    
    def _calculate_metrics(self):
        """计算回测性能指标"""
        try:
            if not self.trade_history:
                return
            
            # 转换交易历史为DataFrame
            trades_df = pd.DataFrame(self.trade_history)
            
            # 计算收益率
            final_value = self.strategy.portfolio_value
            total_return = (final_value - self.initial_capital) / self.initial_capital
            
            # 计算其他指标
            self.performance_metrics = {
                'total_return': total_return,
                'total_trades': len(self.trade_history),
                'final_value': final_value,
                'win_rate': self._calculate_win_rate(trades_df)
            }
            
        except Exception as e:
            self.logger.error(f"计算回测指标失败: {str(e)}")
    
    def _calculate_win_rate(self, trades_df: pd.DataFrame) -> float:
        """计算胜率"""
        if trades_df.empty:
            return 0.0
            
        profitable_trades = trades_df[trades_df['value'] > 0]
        return len(profitable_trades) / len(trades_df) if len(trades_df) > 0 else 0.0