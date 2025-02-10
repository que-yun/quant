import matplotlib
matplotlib.use('Qt5Agg')  # 使用 Qt5Agg 后端获得更好的交互性能

import matplotlib.pyplot as plt

# --- 全局设置字体（推荐使用系统中存在的中文字体，比如 PingFang SC） ---
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['PingFang SC']  # macOS 推荐
plt.rcParams['axes.unicode_minus'] = False  # 解决负号 '-' 显示为方块的问题

# --- 创建 mplfinance 自定义样式 ---
import mplfinance as mpf
my_style = mpf.make_mpf_style(base_mpf_style='charles',
                              rc={'font.family': 'sans-serif',
                                  'font.sans-serif': ['PingFang SC'],
                                  'axes.unicode_minus': False})
# 使用自定义样式（推荐用上下文管理器或 plt.style.use）
plt.style = my_style

# 以下为你回测及可视化的代码
import pandas as pd
import random
from datetime import datetime, timedelta
from loguru import logger

# 其它模块导入（请确保这些模块中的绘图部分没有再次覆盖字体设置）
from modules.strategy.DoubleMAStrategy import DoubleMAStrategy
from modules.strategy.backtest import BacktestEngine
from modules.data.data_fetcher import AStockData
from modules.data.market_data import MarketData

def run_backtest_with_visualization(num_stocks=5, period_days=180, initial_capital=1000000):
    """运行多股票回测并生成带有交易标注的可视化图表"""
    try:
        # 初始化数据获取器和市场数据
        data_fetcher = AStockData()
        market_data = MarketData()
        
        # 获取所有股票列表
        all_stocks = market_data.get_stock_list()
        if not all_stocks:
            logger.error("获取股票列表失败")
            return None
        
        # 随机选择指定数量的股票
        selected_stocks = random.sample(all_stocks, min(num_stocks, len(all_stocks)))
        
        # 设置回测时间范围
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=period_days)).strftime('%Y-%m-%d')
        
        # 创建子图布局
        fig, axes = plt.subplots(num_stocks, 1, figsize=(15, 6*num_stocks))
        if num_stocks == 1:
            axes = [axes]
        
        results = []
        success_count = 0
        
        # 对每只股票进行回测
        for idx, symbol in enumerate(selected_stocks):
            logger.info(f"开始回测股票 {symbol}")
            
            # 获取回测数据
            if not data_fetcher.get_daily_data(symbol=symbol, start_date=start_date):
                logger.error(f"获取{symbol}的日线数据失败")
                continue
            
            # 初始化回测引擎
            backtest_engine = BacktestEngine(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                freq='D'
            )
            
            if not backtest_engine.load_data():
                logger.error(f"加载{symbol}的回测数据失败")
                continue
            
            # 创建策略实例
            strategy = DoubleMAStrategy(
                fast_period=5,
                slow_period=10,
                initial_capital=initial_capital
            )
            
            # 运行回测
            backtest_engine.run(strategy)
            
            # 处理回测数据
            df = pd.DataFrame(strategy.snapshot)
            df.set_index('date', inplace=True)
            df.index = pd.to_datetime(df.index)
            df['return'] = df['value'].pct_change()
            df['cumulative_return'] = (1 + df['return']).cumprod() - 1
            
            # 计算收益率
            initial_value = df['value'].iloc[0]
            final_value = df['value'].iloc[-1]
            return_rate = (final_value - initial_value) / initial_value
            
            # 判断是否盈利
            if return_rate > 0:
                success_count += 1
            
            results.append({
                'symbol': symbol,
                'return_rate': return_rate,
                'trades': len(strategy.records),
                'data': df,
                'records': strategy.records
            })
            
            # 绘制净值曲线
            ax = axes[idx]
            ax.plot(df.index, df['value'], label='策略净值', color='blue')
            
            # 标注交易点
            buy_dates = []
            buy_values = []
            buy_trade_infos = []
            sell_dates = []
            sell_values = []
            sell_trade_infos = []
            
            for record in strategy.records:
                try:
                    # 将交易时间转换为 Timestamp
                    trade_datetime = pd.to_datetime(record['time'])
                    
                    # 利用 get_indexer 找到与 trade_datetime 最接近的日期索引
                    idx = df.index.get_indexer([trade_datetime], method='nearest')[0]
                    nearest_date = df.index[idx]
                    
                    # 获取该日期对应的净值（可以认为是收盘后的净值）
                    value = df.loc[nearest_date, 'value']
                    
                    # 根据交易类型分别记录数据
                    if record['type'] == 'buy':
                        buy_dates.append(nearest_date)
                        buy_values.append(value)
                        total_amount = record["volume"] * record["price"]
                        info = f'买入\n单价: ¥{record["price"]:.2f}\n数量: {record["volume"]}\n总额: ¥{total_amount:.2f}'
                        buy_trade_infos.append(info)
                    else:  # 'sell'
                        sell_dates.append(nearest_date)
                        sell_values.append(value)
                        profit = record.get('profit', 0)
                        total_amount = record["volume"] * record["price"]
                        info = f'卖出\n单价: ¥{record["price"]:.2f}\n数量: {record["volume"]}\n总额: ¥{total_amount:.2f}\n收益: ¥{profit:.2f}'
                        sell_trade_infos.append(info)
                except Exception as e:
                    logger.warning(f"处理交易记录时出错: {str(e)}, 记录: {record}")
                    continue
            
            # 在图上绘制买入和卖出的散点，并添加标注
            # 标注买入点
            if buy_dates:
                ax.scatter(buy_dates, buy_values, color='red', marker='^', s=100)
                for date, value, info in zip(buy_dates, buy_values, buy_trade_infos):
                    ax.annotate(info, (date, value), xytext=(10, 10),
                                textcoords='offset points', ha='left', va='bottom',
                                bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
            
            # 标注卖出点
            if sell_dates:
                ax.scatter(sell_dates, sell_values, color='green', marker='v', s=100)
                for date, value, info in zip(sell_dates, sell_values, sell_trade_infos):
                    ax.annotate(info, (date, value), xytext=(10, -10),
                                textcoords='offset points', ha='left', va='top',
                                bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
            
            ax.set_title(f'{symbol}策略回测结果 (收益率: {return_rate:.2%})')
            ax.set_xlabel('日期')
            ax.set_ylabel('策略净值')
            ax.grid(True)
            ax.legend()
        
        # 计算成功率
        success_rate = success_count / len(selected_stocks) if selected_stocks else 0
        
        # 添加总体回测结果标题
        fig.suptitle(
            f'多股票回测结果汇总\n回测周期: {period_days}天 ({start_date} 至 {end_date})\n策略成功率: {success_rate:.2%}',
            fontsize=16
        )
        
        # 自动调整布局，防止标题与子图重叠
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        
        # 保存图表（建议使用 dpi 和 bbox_inches 参数保证字体不失真）
        plt.savefig('backtest_visualization.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 输出回测结果到控制台
        print("\n多股票回测结果汇总:")
        print(f"回测周期: {period_days}天 ({start_date} 至 {end_date})")
        print(f"回测股票数: {len(selected_stocks)}")
        print(f"策略成功率: {success_rate:.2%}\n")
        
        print("各股票详细结果:")
        for result in results:
            buy_trades = len([r for r in result['records'] if r['type'] == 'buy'])
            sell_trades = len([r for r in result['records'] if r['type'] == 'sell'])
            print(f"股票 {result['symbol']}: 收益率 {result['return_rate']:.2%}, 买入次数 {buy_trades}, 卖出次数 {sell_trades}")
        
        # 计算汇总指标
        avg_return_rate = sum(r['return_rate'] for r in results) / len(results)
        total_trades = sum(len(r['records']) for r in results)
        total_buy_trades = sum(len([t for t in r['records'] if t['type'] == 'buy']) for r in results)
        total_sell_trades = sum(len([t for t in r['records'] if t['type'] == 'sell']) for r in results)
        profitable_trades = sum(len([t for t in r['records'] if t['type'] == 'sell' and t.get('profit', 0) > 0]) for r in results)
        overall_win_rate = profitable_trades / total_sell_trades if total_sell_trades > 0 else 0
        
        # 创建汇总数据子图
        summary_fig = plt.figure(figsize=(12, 6))
        ax = summary_fig.add_subplot(111)
        
        # 准备汇总数据
        metrics = ['平均收益率', '策略成功率', '整体胜率']
        values = [avg_return_rate, success_rate, overall_win_rate]
        colors = ['blue', 'green', 'orange']
        
        # 绘制柱状图
        bars = ax.bar(metrics, [v * 100 for v in values], color=colors)
        
        # 添加数值标签
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}%',
                    ha='center', va='bottom')
        
        # 设置图表样式
        ax.set_title('策略整体表现汇总')
        ax.set_ylabel('百分比 (%)')
        ax.grid(True, axis='y', linestyle='--', alpha=0.7)
        
        # 添加交易统计信息
        info_text = f'总交易次数: {total_trades}\n'
        info_text += f'买入次数: {total_buy_trades}\n'
        info_text += f'卖出次数: {total_sell_trades}\n'
        info_text += f'回测股票数: {len(results)}'
        
        # 在图表右上角添加文本框
        ax.text(0.95, 0.95, info_text,
                transform=ax.transAxes,
                verticalalignment='top',
                horizontalalignment='right',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        # 保存汇总图表
        plt.tight_layout()
        summary_fig.savefig('backtest_summary.png', dpi=300, bbox_inches='tight')
        plt.close(summary_fig)
        
        return {
            'success': True,
            'results': results,
            'success_rate': success_rate,
            'summary_metrics': {
                'avg_return_rate': avg_return_rate,
                'success_rate': success_rate,
                'win_rate': overall_win_rate,
                'total_trades': total_trades
            }
        }
        
    except Exception as e:
        logger.error(f"回测可视化失败: {str(e)}")
        return None

def main():
    # 运行多股票回测并生成可视化
    result = run_backtest_with_visualization(
        num_stocks=5,  # 随机选择5只股票
        period_days=180,  # 回测180天
        initial_capital=1000000
    )
    
    if result:
        logger.info("回测完成，可视化结果已保存到 backtest_visualization.png")

if __name__ == '__main__':
    main()
