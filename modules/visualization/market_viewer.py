import matplotlib
matplotlib.use('Qt5Agg')

import matplotlib.pyplot as plt
import mplfinance as mpf
from matplotlib.widgets import Cursor
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT, FigureCanvasQTAgg
from PyQt5.QtWidgets import QMainWindow, QWidget, QVBoxLayout
from PyQt5.QtCore import Qt
import pandas as pd

# 创建自定义的 mplfinance 样式
my_style = mpf.make_mpf_style(base_mpf_style='charles',
                              rc={'font.sans-serif': ['Heiti TC'],
                                  'axes.unicode_minus': False})

class MarketViewer(QMainWindow):
    def __init__(self, trading_system):
        super().__init__()
        self.trading_system = trading_system
        self.init_ui()
        
    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle('市场行情查看器')
        self.resize(1200, 800)
        
        # 创建中心部件和布局
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        # 创建图表
        self.create_chart()
        
        # 创建画布
        canvas = FigureCanvasQTAgg(self.fig)
        canvas.figure.set_dpi(100)
        
        # 添加工具栏
        toolbar = NavigationToolbar2QT(canvas, self)
        self.addToolBar(toolbar)
        
        # 将画布添加到布局中
        layout.addWidget(canvas)
        
        # 绑定事件
        canvas.mpl_connect('motion_notify_event', self.mouse_move)
        canvas.mpl_connect('scroll_event', self.on_scroll)
        canvas.mpl_connect('key_press_event', self.on_key)
        canvas.setFocusPolicy(Qt.StrongFocus)
        canvas.setFocus()
    
    def create_chart(self):
        """创建图表"""
        try:
            # 检查回测引擎和数据的有效性
            if not hasattr(self.trading_system, 'backtest_engine') or \
               not self.trading_system.backtest_engine or \
               not hasattr(self.trading_system.backtest_engine, 'strategy') or \
               not hasattr(self.trading_system.backtest_engine.strategy, 'snapshot') or \
               not self.trading_system.backtest_engine.strategy.snapshot:
                # 创建空的DataFrame
                df = pd.DataFrame({
                    'value': [1.0],
                    'return': [0.0],
                    'cumulative_return': [0.0]
                })
                df.index = pd.to_datetime(['2000-01-01'])  # 设置一个默认日期
                df.index.name = 'date'
            else:
                try:
                    # 获取策略回测数据
                    df = pd.DataFrame(self.trading_system.backtest_engine.strategy.snapshot)
                    if df.empty:
                        raise ValueError("回测数据为空")
                    df.set_index('date', inplace=True)
                    df.index = pd.to_datetime(df.index)
                    df['return'] = df['value'].pct_change()
                    df['cumulative_return'] = (1 + df['return']).cumprod() - 1
                except Exception as e:
                    print(f"处理回测数据时出错: {str(e)}")
                    # 创建默认数据
                    df = pd.DataFrame({
                        'value': [1.0],
                        'return': [0.0],
                        'cumulative_return': [0.0]
                    })
                    df.index = pd.to_datetime(['2000-01-01'])
                    df.index.name = 'date'
            
            # 创建图表和子图
            self.fig, (self.ax_main, self.ax_drawdown) = plt.subplots(2, 1, figsize=(12, 8), 
                                                                      gridspec_kw={'height_ratios': [2, 1]})
            
            # 绘制收益曲线
            self.ax_main.plot(df.index, df['cumulative_return'] + 1, label='策略收益曲线', color='blue')
            self.ax_main.set_title('策略回测结果')
            self.ax_main.set_xlabel('日期')
            self.ax_main.set_ylabel('策略收益倍数')
            self.ax_main.grid(True)
            self.ax_main.legend()
            
            # 计算并绘制回撤曲线
            drawdown = (df['value'].cummax() - df['value']) / df['value'].cummax()
            self.ax_drawdown.fill_between(df.index, drawdown, 0, color='red', alpha=0.3, label='回撤')
            self.ax_drawdown.set_xlabel('日期')
            self.ax_drawdown.set_ylabel('回撤幅度')
            self.ax_drawdown.grid(True)
            self.ax_drawdown.legend()
            
            # 添加十字光标
            self.cursor_main = Cursor(self.ax_main, useblit=True, color='gray', linewidth=0.8)
            self.cursor_drawdown = Cursor(self.ax_drawdown, useblit=True, color='gray', linewidth=0.8)
            
            # 创建文本注释
            self.text = self.ax_main.text(0.02, 0.95, '', transform=self.ax_main.transAxes,
                                        bbox=dict(facecolor='white', alpha=0.8))
            
            # 调整布局
            plt.tight_layout()
            self.axlist = [self.ax_main, self.ax_drawdown]
            
        except Exception as e:
            print(f"创建图表失败: {str(e)}")
            # 确保即使创建失败也初始化fig属性
            self.fig = plt.figure()
    
    def mouse_move(self, event):
        """鼠标移动事件处理"""
        if event.inaxes:
            if hasattr(self.trading_system, 'backtest_engine') and \
               hasattr(self.trading_system.backtest_engine, 'strategy') and \
               self.trading_system.backtest_engine.strategy.snapshot:
                df = pd.DataFrame(self.trading_system.backtest_engine.strategy.snapshot)
                df.set_index('date', inplace=True)
                df.index = pd.to_datetime(df.index)
                df['return'] = df['value'].pct_change()
                df['cumulative_return'] = (1 + df['return']).cumprod() - 1
                
                index = int(event.xdata)
                if 0 <= index < len(df):
                    date = df.index[index].strftime('%Y-%m-%d')
                    value = df['value'].iloc[index]
                    cumulative_return = df['cumulative_return'].iloc[index]
                    daily_return = df['return'].iloc[index]
                    drawdown = (df['value'].cummax().iloc[index] - value) / df['value'].cummax().iloc[index]
                    
                    info = f'日期: {date}\n'
                    info += f'净值: {value:,.2f}\n'
                    info += f'累计收益: {cumulative_return:.2%}\n'
                    info += f'日收益率: {daily_return:.2%}\n'
                    info += f'回撤: {drawdown:.2%}'
                    
                    self.text.set_text(info)
                    self.fig.canvas.draw_idle()
    
    def on_scroll(self, event):
        """滚轮缩放事件处理"""
        if event.inaxes:
            # 获取当前视图范围
            cur_xlim = event.inaxes.get_xlim()
            cur_ylim = event.inaxes.get_ylim()
            
            # 设置缩放因子
            base_scale = 1.1
            scale_factor = 1/base_scale if event.button == 'up' else base_scale
            
            # 计算新的范围
            new_width = (cur_xlim[1] - cur_xlim[0]) * scale_factor
            new_height = (cur_ylim[1] - cur_ylim[0]) * scale_factor
            
            # 计算鼠标位置的相对位置
            rel_x = (cur_xlim[1] - event.xdata)/(cur_xlim[1] - cur_xlim[0])
            rel_y = (cur_ylim[1] - event.ydata)/(cur_ylim[1] - cur_ylim[0])
            
            # 设置新的范围
            event.inaxes.set_xlim([event.xdata - new_width * (1-rel_x),
                                  event.xdata + new_width * rel_x])
            event.inaxes.set_ylim([event.ydata - new_height * (1-rel_y),
                                  event.ydata + new_height * rel_y])
            
            # 重绘图表
            self.fig.canvas.draw_idle()
    
    def on_key(self, event):
        """键盘事件处理"""
        if event.key in ['left', 'right']:
            # 获取当前视图范围
            for ax in self.axlist:
                cur_xlim = ax.get_xlim()
                # 设置移动步长为1（一个交易日）
                move_size = 1
                
                # 根据按键方向移动
                if event.key == 'left':
                    ax.set_xlim([cur_xlim[0] - move_size, cur_xlim[1] - move_size])
                else:  # right
                    ax.set_xlim([cur_xlim[0] + move_size, cur_xlim[1] + move_size])
            
            # 重绘图表
            self.fig.canvas.draw()