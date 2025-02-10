import os
import sys
import random
from loguru import logger
from PyQt5.QtWidgets import QApplication
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# 配置matplotlib中文字体
plt.rcParams['font.sans-serif'] = ['Heiti TC']  # 使用macOS系统的黑体-繁
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示为方块的问题

# 设置项目根目录
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(ROOT_DIR)

# 导入自定义模块
from modules.data.data_fetcher import AStockData
from modules.data.database import Database
from modules.data.market_data import MarketData
from modules.indicators.technical_indicators import TechnicalIndicators
from modules.strategy.strategy_base import StrategyBase
from modules.strategy.backtest import BacktestEngine
from modules.strategy.DoubleMAStrategy import DoubleMAStrategy
from modules.visualization.market_viewer import MarketViewer

# 设置日志
logger.add(os.path.join(ROOT_DIR, "trading.log"))

class TradingSystem:
    def __init__(self):
        self.data_fetcher = AStockData()
        self.database = Database()
        self.market_data = MarketData()
        self.indicators = TechnicalIndicators()
        self.backtest_engine = None
        self.config = {
            'default_period_days': 360,  # 延长回测周期到一年
            'default_initial_capital': 1000000,
            'default_num_stocks': 5,
            'ma_fast_period': 5,  # 更新为优化后的快线周期
            'ma_slow_period': 10   # 更新为优化后的慢线周期
        }

    def initialize(self):
        """初始化交易系统"""
        try:
            # 确保数据库和必要的表已创建
            if not self.database.initialize():
                logger.error("数据库初始化失败")
                return False
                
            # 尝试获取股票列表以验证数据访问
            if not self.market_data.get_stock_list():
                logger.error("无法获取股票列表")
                return False
                
            logger.info("交易系统初始化成功")
            return True
        except Exception as e:
            logger.error(f"交易系统初始化失败: {str(e)}")
            return False
    
    def run_backtest(self, strategy_class, symbol, start_date, end_date, freq="D", initial_capital=None):
        """运行单个股票的回测"""
        try:
            if initial_capital is None:
                initial_capital = self.config['default_initial_capital']
                
            # 获取回测数据
            if not self.data_fetcher.get_daily_data(symbol=symbol, start_date=start_date):
                logger.error(f"获取{symbol}的日线数据失败")
                return None
            
            # 初始化回测引擎
            self.backtest_engine = BacktestEngine(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                freq=freq
            )
            
            if not self.backtest_engine.load_data():
                logger.error(f"加载{symbol}的回测数据失败")
                return None
            
            # 创建策略实例
            strategy = strategy_class(
                fast_period=self.config['ma_fast_period'],
                slow_period=self.config['ma_slow_period'],
                initial_capital=initial_capital
            )
            
            # 运行回测并分析
            self.backtest_engine.run(strategy)
            self.backtest_engine.analyze(strategy)
            
            return {
                'symbol': symbol,
                'records': strategy.records,
                'snapshot': strategy.snapshot
            }
        except Exception as e:
            logger.error(f"回测运行失败: {str(e)}")
            return None
    
    def run_multi_stock_backtest(self, strategy_class, num_stocks=5, period_days=180, initial_capital=1000000):
        """运行多股票回测"""
        try:
            # 获取所有股票列表
            all_stocks = self.market_data.get_stock_list()
            if not all_stocks:
                logger.error("获取股票列表失败")
                return None
            
            # 随机选择指定数量的股票
            selected_stocks = random.sample(all_stocks, min(num_stocks, len(all_stocks)))
            
            # 设置回测时间范围
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=period_days)).strftime('%Y-%m-%d')
            
            results = []
            success_count = 0
            
            # 对每只股票进行回测
            for symbol in selected_stocks:
                logger.info(f"开始回测股票 {symbol}")
                result = self.run_backtest(
                    strategy_class=strategy_class,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    freq="D",
                    initial_capital=initial_capital
                )
                
                if result and result['snapshot']:
                    # 计算收益率
                    initial_value = result['snapshot'][0]['value']
                    final_value = result['snapshot'][-1]['value']
                    return_rate = (final_value - initial_value) / initial_value
                    
                    # 判断是否盈利
                    if return_rate > 0:
                        success_count += 1
                    
                    results.append({
                        'symbol': symbol,
                        'return_rate': return_rate,
                        'trades': len(result['records'])
                    })
            
            # 计算成功率
            success_rate = success_count / len(selected_stocks) if selected_stocks else 0
            
            # 打印回测结果
            print("\n多股票回测结果汇总:")
            print(f"回测周期: {period_days}天 ({start_date} 至 {end_date})")
            print(f"回测股票数: {len(selected_stocks)}")
            print(f"策略成功率: {success_rate:.2%}\n")
            
            print("各股票详细结果:")
            for result in results:
                print(f"股票 {result['symbol']}: 收益率 {result['return_rate']:.2%}, 交易次数 {result['trades']}")
            
            return results
            
        except Exception as e:
            logger.error(f"多股票回测失败: {str(e)}")
            return None

def main():
    # 创建Qt应用
    app = QApplication.instance()
    if not app:
        app = QApplication(sys.argv)
    
    # 初始化交易系统
    trading_system = TradingSystem()
    if not trading_system.initialize():
        sys.exit(1)
    
    # 运行多股票回测
    trading_system.run_multi_stock_backtest(
        strategy_class=DoubleMAStrategy,
        num_stocks=5,  # 随机选择5只股票
        period_days=180,  # 回测180天
        initial_capital=1000000
    )
    
    # 创建并显示市场行情查看器
    viewer = MarketViewer(trading_system)
    viewer.show()
    
    # 运行应用
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()