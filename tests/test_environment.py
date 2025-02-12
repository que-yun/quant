import sys
import numpy as np
import pandas as pd
import akshare as ak
import talib
from loguru import logger
from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QtCore import Qt
import matplotlib.pyplot as plt
import mplfinance as mpf
from matplotlib.widgets import Cursor
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT, FigureCanvasQTAgg

# 创建自定义的 mplfinance 样式
my_style = mpf.make_mpf_style(base_mpf_style='charles',
                             rc={'font.sans-serif': ['Heiti TC'],
                                 'axes.unicode_minus': False})

def prepare_test_data():
    """准备测试数据"""
    logger.info("创建测试数据...")
    df = pd.DataFrame({
        'A': np.random.randn(5),
        'B': np.random.randn(5)
    })
    logger.info(f"测试数据创建成功:\n{df}")
    return True

def fetch_stock_data():
    """获取股票数据"""
    try:
        logger.info("获取股票数据...")
        df_stock = ak.index_zh_a_hist(symbol="000001", period="daily", 
                                    start_date="20230101", end_date="20240101")
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
        return df_stock
    except Exception as e:
        logger.error(f"获取股票数据失败: {str(e)}")
        return None

def calculate_indicators(df_stock):
    """计算技术指标"""
    try:
        close = df_stock['Close']
        macd, signal, hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        return macd, signal, hist
    except Exception as e:
        logger.error(f"计算技术指标失败: {str(e)}")
        return None, None, None

def create_chart(df_stock, macd, signal, hist):
    """创建图表"""
    try:
        # 创建MACD指标的三条线
        apds = [
            mpf.make_addplot(macd, panel=2, color='blue', ylabel='MACD', 
                            title='MACD(12,26,9)', width=0.7),
            mpf.make_addplot(signal, panel=2, color='orange', width=0.7),
            mpf.make_addplot(hist, type='bar', panel=2, color='dimgray', alpha=1)
        ]
        
        # 绘制K线图和MACD
        fig, axlist = mpf.plot(df_stock, 
                type='candle',
                title='上证指数K线图 - MACD',
                ylabel='价格',
                volume=True,
                addplot=apds,
                style=my_style,
                figsize=(12, 8),
                panel_ratios=(4,1,2),
                volume_panel=1,
                ylabel_lower='成交量',
                returnfig=True,
                tight_layout=False,
                scale_padding={'left': 0.8, 'right': 1.2, 'top': 2.0, 'bottom': 0.8})
        
        return fig, axlist
    except Exception as e:
        logger.error(f"创建图表失败: {str(e)}")
        return None, None

def setup_chart_interaction(fig, axlist, df_stock, macd, signal, hist):
    """设置图表交互功能"""
    try:
        ax_main, ax_volume, ax_macd = axlist[0], axlist[1], axlist[2]
        
        # 添加十字光标
        cursor_main = Cursor(ax_main, useblit=True, color='gray', linewidth=0.8)
        cursor_volume = Cursor(ax_volume, useblit=True, color='gray', linewidth=0.8)
        cursor_macd = Cursor(ax_macd, useblit=True, color='gray', linewidth=0.8)
        
        # 创建文本注释
        text = ax_main.text(0.02, 0.95, '', transform=ax_main.transAxes, 
                           bbox=dict(facecolor='white', alpha=0.8))
        
        def format_coord(x, y):
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
        
        def on_scroll(event):
            if event.inaxes:
                cur_xlim = event.inaxes.get_xlim()
                cur_ylim = event.inaxes.get_ylim()
                base_scale = 1.1
                scale_factor = 1/base_scale if event.button == 'up' else base_scale
                
                new_width = (cur_xlim[1] - cur_xlim[0]) * scale_factor
                new_height = (cur_ylim[1] - cur_ylim[0]) * scale_factor
                
                rel_x = (cur_xlim[1] - event.xdata)/(cur_xlim[1] - cur_xlim[0])
                rel_y = (cur_ylim[1] - event.ydata)/(cur_ylim[1] - cur_ylim[0])
                
                event.inaxes.set_xlim([event.xdata - new_width * (1-rel_x), 
                                      event.xdata + new_width * rel_x])
                event.inaxes.set_ylim([event.ydata - new_height * (1-rel_y), 
                                      event.ydata + new_height * rel_y])
                
                fig.canvas.draw_idle()
        
        def on_key(event):
            if event.key in ['left', 'right']:
                for ax in axlist:
                    cur_xlim = ax.get_xlim()
                    move_size = 1
                    if event.key == 'left':
                        ax.set_xlim([cur_xlim[0] - move_size, cur_xlim[1] - move_size])
                    else:
                        ax.set_xlim([cur_xlim[0] + move_size, cur_xlim[1] + move_size])
                fig.canvas.draw()
        
        return mouse_move, on_scroll, on_key
    except Exception as e:
        logger.error(f"设置图表交互功能失败: {str(e)}")
        return None, None, None

def create_qt_window(fig):
    """创建Qt窗口"""
    try:
        app = QApplication.instance()
        if not app:
            app = QApplication(sys.argv)
        
        main_window = QMainWindow()
        canvas = FigureCanvasQTAgg(fig)
        canvas.figure.set_dpi(100)
        main_window.setCentralWidget(canvas)
        
        toolbar = NavigationToolbar2QT(canvas, main_window)
        main_window.addToolBar(toolbar)
        
        main_window.setWindowTitle('上证指数K线图分析工具')
        main_window.resize(1200, 800)
        
        return app, main_window, canvas
    except Exception as e:
        logger.error(f"创建Qt窗口失败: {str(e)}")
        return None, None, None

def test_environment():
    """环境测试主函数"""
    try:
        # 准备测试数据
        if not prepare_test_data():
            return False
        
        # 获取股票数据
        df_stock = fetch_stock_data()
        if df_stock is None:
            return False
        
        # 计算技术指标
        macd, signal, hist = calculate_indicators(df_stock)
        if macd is None:
            return False
        
        # 创建图表
        fig, axlist = create_chart(df_stock, macd, signal, hist)
        if fig is None or axlist is None:
            return False
        
        # 设置图表交互
        mouse_move, on_scroll, on_key = setup_chart_interaction(fig, axlist, df_stock, macd, signal, hist)
        if mouse_move is None:
            return False
        
        # 创建Qt窗口
        app, main_window, canvas = create_qt_window(fig)
        if app is None:
            return False
        
        # 启用图表的平移和缩放功能
        for ax in axlist:
            ax.set_zorder(1)
            ax.patch.set_visible(False)
        
        # 调整图表布局
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
        fig.tight_layout(pad=1.5)
        
        # 绑定事件
        canvas.mpl_connect('motion_notify_event', mouse_move)
        canvas.mpl_connect('scroll_event', on_scroll)
        canvas.mpl_connect('key_press_event', on_key)
        canvas.setFocusPolicy(Qt.StrongFocus)
        canvas.setFocus()
        
        # 保存图表
        plt.savefig('kline_macd.png')
        
        # 显示窗口
        main_window.show()
        app.exec_()
        
        logger.success("K线图和MACD指标已生成并保存为kline_macd.png")
        logger.success("环境测试完成！所有依赖包工作正常。")
        return True

    except Exception as e:
        logger.error(f"测试过程中发生错误: {str(e)}")
        return False