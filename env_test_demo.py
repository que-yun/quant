import matplotlib
matplotlib.use('Qt5Agg')  # 使用Qt5Agg后端以获得更好的交互性能

import matplotlib.pyplot as plt

# 设置matplotlib支持中文显示（注意这里的字体列表必须用逗号分隔）
plt.rcParams['font.sans-serif'] = ['Heiti TC']  # 或者使用 'PingFang SC'
plt.rcParams['axes.unicode_minus'] = False  # 解决负号 '-' 显示为方块的问题

import mplfinance as mpf

# 创建自定义的 mplfinance 样式，基于 'charles' 样式，并覆盖 rc 参数中的字体设置
my_style = mpf.make_mpf_style(base_mpf_style='charles',
                              rc={'font.sans-serif': ['Heiti TC'],
                                  'axes.unicode_minus': False})

import pandas as pd
import numpy as np
import akshare as ak
from loguru import logger

import talib
from matplotlib.widgets import Cursor
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT, FigureCanvasQTAgg
from PyQt5.QtWidgets import QMainWindow, QApplication
from PyQt5.QtCore import Qt
import sys

# 设置日志
logger.add("demo.log")

def test_environment():
    try:
        # 测试 pandas 和 numpy
        logger.info("创建测试数据...")
        df = pd.DataFrame({
            'A': np.random.randn(5),
            'B': np.random.randn(5)
        })
        logger.info(f"测试数据创建成功:\n{df}")

        # 获取股票数据
        logger.info("获取股票数据...")
        # 获取上证指数最近60个交易日数据
        df_stock = ak.index_zh_a_hist(symbol="000001", period="daily", start_date="20230101", end_date="20240101")
        logger.info(f"成功获取股市数据，共{len(df_stock)}条记录")
        
        # 准备绘制K线图的数据
        column_mapping = {
            '日期': 'Date',
            '开盘': 'Open',
            '收盘': 'Close',
            '最高': 'High',
            '最低': 'Low',
            '成交量': 'Volume',
        }
        df_stock.rename(columns=column_mapping, inplace=True)
        df_stock.set_index('Date', inplace=True)
        df_stock.index = pd.to_datetime(df_stock.index)
        
        # 计算MACD指标
        close = df_stock['Close']
        macd, signal, hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        
        # 创建MACD指标的三条线
        apds = [
            mpf.make_addplot(macd, panel=2, color='blue', ylabel='MACD', 
                            title='MACD(12,26,9)', width=0.7),
            mpf.make_addplot(signal, panel=2, color='orange', width=0.7),
            mpf.make_addplot(hist, type='bar', panel=2, color='dimgray', alpha=1)
        ]
        
        # 创建Qt应用
        app = QApplication.instance()
        if not app:
            app = QApplication(sys.argv)
        
        # 创建主窗口
        main_window = QMainWindow()
        
        # 绘制K线图和MACD
        # 创建图表和子图
        fig, axlist = mpf.plot(df_stock, 
                type='candle',
                title='上证指数K线图 - MACD',
                ylabel='价格',
                volume=True,
                addplot=apds,
                style=my_style,  # 使用自定义样式
                figsize=(12, 8),  # 增加图表整体大小
                panel_ratios=(4,1,2),  # 调整各个子图的比例
                volume_panel=1,
                ylabel_lower='成交量',
                returnfig=True,
                tight_layout=False,  # 关闭自动布局以便手动控制边距
                scale_padding={'left': 0.8, 'right': 1.2, 'top': 2.0, 'bottom': 0.8})  # 调整边距确保完整显示
        
        # 获取主图和成交量图的坐标轴
        ax_main = axlist[0]
        ax_volume = axlist[1]
        ax_macd = axlist[2]
        
        # 添加十字光标
        cursor_main = Cursor(ax_main, useblit=True, color='gray', linewidth=0.8)
        cursor_volume = Cursor(ax_volume, useblit=True, color='gray', linewidth=0.8)
        cursor_macd = Cursor(ax_macd, useblit=True, color='gray', linewidth=0.8)
        
        # 创建文本注释
        text = ax_main.text(0.02, 0.95, '', transform=ax_main.transAxes, 
                           bbox=dict(facecolor='white', alpha=0.8))
        
        def format_coord(x, y):
            # 找到最近的数据点
            index = int(x)
            if 0 <= index < len(df_stock):
                date = df_stock.index[index].strftime('%Y-%m-%d')
                price = f'价格: {y:.2f}'
                volume = f'成交量: {df_stock["Volume"].iloc[index]:,.0f}'
                macd_val = f'MACD: {macd[index]:.3f}'
                signal_val = f'Signal: {signal[index]:.3f}'
                hist_val = f'Hist: {hist[index]:.3f}'
                return f'日期: {date}\n{price}\n{volume}\n{macd_val}\n{signal_val}\n{hist_val}'
            return ''
        
        def mouse_move(event):
            if event.inaxes:
                text.set_text(format_coord(event.xdata, event.ydata))
                fig.canvas.draw_idle()
        
        # 创建画布并设置DPI以提高显示质量
        canvas = FigureCanvasQTAgg(fig)
        canvas.figure.set_dpi(100)
        main_window.setCentralWidget(canvas)
        
        # 添加工具栏并设置图表交互
        toolbar = NavigationToolbar2QT(canvas, main_window)
        main_window.addToolBar(toolbar)
        
        # 启用图表的平移和缩放功能
        for ax in axlist:
            ax.set_zorder(1)
            ax.patch.set_visible(False)
        
        # 调整图表布局和边距
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
        fig.tight_layout(pad=1.5)
        
        # 定义滚轮缩放事件处理函数
        def on_scroll(event):
            if event.inaxes:
                # 获取当前视图范围
                cur_xlim = event.inaxes.get_xlim()
                cur_ylim = event.inaxes.get_ylim()
                
                # 设置缩放因子
                base_scale = 1.1
                # 根据滚轮方向确定是放大还是缩小
                if event.button == 'up':
                    scale_factor = 1/base_scale
                else:
                    scale_factor = base_scale
                
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
                fig.canvas.draw_idle()
        
        # 定义键盘事件处理函数
        def on_key(event):
            if event.key in ['left', 'right']:
                # 获取当前视图范围
                for ax in axlist:
                    cur_xlim = ax.get_xlim()
                    # 设置移动步长为1（一个交易日）
                    move_size = 1
                    
                    # 根据按键方向移动
                    if event.key == 'left':
                        ax.set_xlim([cur_xlim[0] - move_size, cur_xlim[1] - move_size])
                    else:  # right
                        ax.set_xlim([cur_xlim[0] + move_size, cur_xlim[1] + move_size])
                
                # 重绘图表
                fig.canvas.draw()
        
        # 绑定事件
        canvas.mpl_connect('motion_notify_event', mouse_move)
        canvas.mpl_connect('scroll_event', on_scroll)
        canvas.mpl_connect('key_press_event', on_key)
        canvas.setFocusPolicy(Qt.StrongFocus)
        canvas.setFocus()
        
        # 保存图表
        plt.savefig('kline_macd.png')
        
        # 设置窗口大小和标题
        main_window.setWindowTitle('上证指数K线图分析工具')
        main_window.resize(1200, 800)
        
        # 显示窗口
        main_window.show()
        app.exec_()
        
        logger.success("K线图和MACD指标已生成并保存为kline_macd.png")
        logger.success("环境测试完成！所有依赖包工作正常。")
        return True

    except Exception as e:
        logger.error(f"测试过程中发生错误: {str(e)}")
        return False

if __name__ == "__main__":
    test_environment()