import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from modules.indicators.alert.alert_strategy import MarketAlert

class TestMarketAlert(unittest.TestCase):
    def setUp(self):
        """初始化测试环境"""
        # 创建数据库连接
        self.engine = create_engine('mysql+pymysql://root:12345678@localhost:3306/quant')
        self.alert = MarketAlert()
        
        # 获取上证指数一年的日线数据
        query = """
        SELECT trade_date, close, high, low, volume 
        FROM index_daily_data 
        WHERE ts_code = '000001.SH'
        AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
        ORDER BY trade_date
        """
        
        self.data = pd.read_sql(query, self.engine)
        # 将trade_date设置为索引
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])
        self.data.set_index('trade_date', inplace=True)
        
    def test_market_alert(self):
        """测试市场告警功能"""
        # 运行告警检测
        alert_info = self.alert.run_alert(self.data)
        
        # 基本断言
        self.assertIsNotNone(alert_info, "告警信息不应为空")
        self.assertIn('market_status', alert_info, "应包含市场状态信息")
        self.assertIn('signals', alert_info, "应包含信号信息")
        self.assertIn('alert_triggered', alert_info, "应包含告警触发标志")
        
        # 打印详细的告警信息
        print("\n市场告警测试结果:")
        print(f"当前市场状态: {alert_info['market_status']}")
        print(f"最新交易日期: {alert_info['timestamp'].strftime('%Y-%m-%d')}")
        print(f"当前价格: {alert_info['current_price']:.2f}")
        print(f"价格变化: {alert_info['price_change']:.2f}%")
        
        # 如果有状态变化，打印变化信息
        if alert_info['status_change']:
            print("\n状态变化信息:")
            print(f"从 {alert_info['status_change']['from']} 变为 {alert_info['status_change']['to']}")
            print(f"变化日期: {alert_info['status_change']['date']}")
            print("关键指标:")
            for key, value in alert_info['status_change']['key_indicators'].items():
                print(f"- {key}: {value}")
        
        # 如果触发告警，打印告警信号
        if alert_info['alert_triggered']:
            print("\n触发告警信号:")
            for signal_name, is_triggered in alert_info['signals'].items():
                if is_triggered:
                    print(f"- {signal_name}")
    
    def test_historical_alerts(self):
        """测试历史数据中的告警点"""
        window_size = 100  # 使用100天的滑动窗口
        alert_dates = []
        status_changes = []
        
        # 使用滑动窗口模拟历史数据
        for i in range(window_size, len(self.data)):
            window_data = self.data.iloc[i-window_size:i+1]
            alert_info = self.alert.run_alert(window_data)
            
            if alert_info['alert_triggered']:
                alert_dates.append({
                    'date': alert_info['timestamp'],
                    'status': alert_info['market_status'],
                    'signals': alert_info['signals']
                })
            
            if alert_info['status_change']:
                status_changes.append(alert_info['status_change'])
        
        # 打印历史告警统计
        print("\n历史告警统计:")
        print(f"分析周期: {self.data.index[0].strftime('%Y-%m-%d')} 至 {self.data.index[-1].strftime('%Y-%m-%d')}")
        print(f"告警触发次数: {len(alert_dates)}")
        print(f"状态变化次数: {len(status_changes)}")
        
        # 打印详细的告警记录
        if alert_dates:
            print("\n告警触发记录:")
            for alert in alert_dates:
                print(f"\n日期: {alert['date'].strftime('%Y-%m-%d')}")
                print(f"市场状态: {alert['status']}")
                print("触发信号:")
                for signal_name, is_triggered in alert['signals'].items():
                    if is_triggered:
                        print(f"- {signal_name}")
        
        # 打印状态变化记录
        if status_changes:
            print("\n状态变化记录:")
            for change in status_changes:
                print(f"\n日期: {change['date']}")
                print(f"从 {change['from']} 变为 {change['to']}")
                print("关键指标:")
                for key, value in change['key_indicators'].items():
                    print(f"- {key}: {value}")

if __name__ == '__main__':
    unittest.main()