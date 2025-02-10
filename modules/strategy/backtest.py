import pandas as pd
import matplotlib.pyplot as plt
import sqlalchemy as sa
import mplfinance as mpf
import numpy as np
from loguru import logger

# 设置matplotlib支持中文显示
plt.rcParams['font.sans-serif'] = ['Heiti TC']  # 使用macOS系统的黑体-繁
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示为方块的问题

# 创建自定义的 mplfinance 样式，基于 'charles' 样式，并覆盖 rc 参数中的字体设置
my_style = mpf.make_mpf_style(base_mpf_style='charles',
                              rc={'font.sans-serif': ['Heiti TC'],
                                  'axes.unicode_minus': False})

class BacktestEngine:
    def __init__(self, symbol, start_date, end_date, freq='D'):
        self.engine = sa.create_engine("sqlite:////Users//admin//work//quant//trading.db")
        self.symbol = symbol
        self.freq = freq
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        
    def load_data(self):
        """加载回测所需数据"""
        try:
            # 加载日线数据
            query = f"""
            SELECT date, symbol, open, high, low, close, volume, amount, amplitude, pct_change, price_change, turnover_rate 
            FROM daily_bars 
            WHERE symbol='{self.symbol}' 
              AND date BETWEEN '{self.start_date}' AND '{self.end_date}'
            ORDER BY date
            """
            self.daily_data = pd.read_sql(query, self.engine)
            
            if self.daily_data.empty:
                logger.error(f"错误：没有可用的{self.symbol}的日线数据进行回测")
                return False
                
            # 优化数据处理
            self.daily_data['date'] = pd.to_datetime(self.daily_data['date'])
            self.daily_data.set_index('date', inplace=True)
            
            # 预计算一些常用的技术指标
            self.daily_data['ma5'] = self.daily_data['close'].rolling(window=5).mean()
            self.daily_data['ma10'] = self.daily_data['close'].rolling(window=10).mean()
            self.daily_data['ma20'] = self.daily_data['close'].rolling(window=20).mean()
            self.daily_data['ma60'] = self.daily_data['close'].rolling(window=60).mean()
            
            # 计算波动率
            self.daily_data['volatility'] = self.daily_data['close'].pct_change().rolling(window=20).std()
            
            logger.info(f"成功加载{self.symbol}的回测数据，时间范围：{self.start_date} 至 {self.end_date}")
            return True
            
        except Exception as e:
            logger.error(f"加载回测数据失败: {str(e)}")
            return False
            
    def run(self, strategy):
        """执行回测"""
        try:
            strategy.records = []  # 记录交易历史
            strategy.snapshot = []  # 记录每日净值
            
            # 验证数据是否可用
            if self.daily_data.empty:
                logger.error("错误：没有可用的日线数据进行回测")
                return
                
            # 记录初始净值
            strategy.snapshot.append({
                'date': self.daily_data.index[0].date(),
                'value': strategy.get_current_capital(),
                'position': 0,
                'cash': strategy.get_current_capital(),
                'holding_value': 0,  # 持仓市值
                'total_pnl': 0,  # 总盈亏
                'daily_pnl': 0,  # 日盈亏
                'commission': 0  # 累计手续费
            })
            
            # 遍历每个交易日进行回测
            for idx, row in self.daily_data.iterrows():
                # 获取当前数据切片
                current_data = self.daily_data[:idx]
                
                # 执行策略逻辑
                signal, volume = strategy.handle_data(self.symbol, current_data)
                
                # 根据信号执行交易
                # 设置交易时间为当天的收盘时间（假设为15:00:00）
                trade_time = pd.Timestamp(idx).replace(hour=15, minute=0, second=0)
                
                if signal == 1 and volume > 0:  # 买入信号
                    strategy.buy(self.symbol, row['close'], volume, trade_time)
                    logger.info(f"买入信号触发: {self.symbol}, 价格: {row['close']}, 数量: {volume}, 时间: {trade_time}")
                elif signal == -1 and volume > 0:  # 卖出信号
                    position = strategy.get_position(self.symbol)
                    if position['volume'] > 0:
                        strategy.sell(self.symbol, row['close'], volume, trade_time)
                        logger.info(f"卖出信号触发: {self.symbol}, 价格: {row['close']}, 数量: {volume}, 时间: {trade_time}")
                
                # 记录当日净值和持仓信息
                try:
                    position = strategy.get_position(self.symbol)
                    holding_value = position['volume'] * row['close']
                    total_value = strategy.get_current_capital() + holding_value
                    
                    strategy.snapshot.append({
                        'date': trade_time,  # 直接使用Timestamp对象
                        'value': total_value,
                        'position': position,
                        'cash': strategy.get_current_capital(),
                        'holding_value': holding_value
                    })
                except Exception as e:
                    logger.error(f"记录净值时发生错误：{str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"回测执行失败: {str(e)}")
            
    def analyze(self, strategy):
        """生成回测报告"""
        try:
            if not strategy.snapshot:
                logger.error("没有回测数据可供分析")
                return
                
            df = pd.DataFrame(strategy.snapshot)
            df.set_index('date', inplace=True)
            df['return'] = df['value'].pct_change()
            df['cumulative_return'] = (1 + df['return']).cumprod() - 1
            
            # 计算关键指标
            total_return = (df['value'].iloc[-1] - df['value'].iloc[0])/df['value'].iloc[0]
            annual_return = total_return * (252 / len(df))  # 年化收益率
            max_drawdown = (df['value'].cummax() - df['value']).max()/df['value'].cummax().max()
            volatility = df['return'].std() * np.sqrt(252)  # 年化波动率
            sharpe_ratio = annual_return / volatility if volatility != 0 else 0  # 夏普比率
            
            # 计算交易胜率和交易次数
            buy_trades = len([record for record in strategy.records if record['type'] == 'buy'])
            sell_trades = len([record for record in strategy.records if record['type'] == 'sell'])
            profitable_trades = sum(1 for record in strategy.records if record['type'] == 'sell' and record.get('profit', 0) > 0)
            win_rate = profitable_trades / sell_trades if sell_trades > 0 else 0
            
            # 绘制回测结果图表
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), gridspec_kw={'height_ratios': [2, 1]})
            
            # 净值曲线
            ax1.plot(df.index, df['cumulative_return'] + 1, label='策略收益曲线')
            ax1.set_title("策略整体收益表现")
            ax1.set_xlabel("日期")
            ax1.set_ylabel("策略收益倍数")
            ax1.grid(True)
            ax1.legend()
            
            # 回撤曲线
            drawdown = (df['value'].cummax() - df['value']) / df['value'].cummax()
            ax2.fill_between(df.index, drawdown, 0, color='red', alpha=0.3, label='回撤')
            ax2.set_xlabel("日期")
            ax2.set_ylabel("回撤幅度")
            ax2.grid(True)
            ax2.legend()
            
            plt.tight_layout()
            plt.savefig('backtest_result.png')
            
            # 输出回测报告
            print("\n策略整体回测分析:")
            print(f"总收益率: {total_return:.2%}")
            print(f"年化收益率: {annual_return:.2%}")
            print(f"最大回撤: {max_drawdown:.2%}")
            print(f"年化波动率: {volatility:.2%}")
            print(f"夏普比率: {sharpe_ratio:.2f}")
            print(f"胜率: {win_rate:.2%}")
            print(f"买入次数: {buy_trades}")
            print(f"卖出次数: {sell_trades}")
            print(f"交易次数: {len(strategy.records)}")
            
            return {
                'total_return': total_return,
                'annual_return': annual_return,
                'max_drawdown': max_drawdown,
                'volatility': volatility,
                'sharpe_ratio': sharpe_ratio,
                'win_rate': win_rate,
                'buy_trades': buy_trades,
                'sell_trades': sell_trades
            }
            
        except Exception as e:
            logger.error(f"分析回测结果失败: {str(e)}")
            return None
        
        # 绘制净值曲线
        plt.figure(figsize=(12,6))
        plt.plot(df['date'], df['value'], label='Strategy')
        plt.title(f"回测结果 ({self.symbol})")
        plt.xlabel("日期")
        plt.ylabel("组合价值")
        plt.legend()
        plt.grid(True)
        plt.style = my_style  # 使用自定义样式
        plt.savefig('backtest_result.png')
        
        # 计算关键指标
        total_return = (df['value'].iloc[-1] - df['value'].iloc[0])/df['value'].iloc[0]
        max_drawdown = (df['value'].cummax() - df['value']).max()/df['value'].cummax().max()
        
        print(f"\n回测结果分析:")
        print(f"累计收益率: {total_return:.2%}")
        print(f"最大回撤: {max_drawdown:.2%}")
        print(f"交易次数: {len(strategy.records)}")