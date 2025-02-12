import unittest
from datetime import datetime, timedelta
from modules.data.collector.stock_data_collector import StockDataCollector
from modules.data.collector.market_data_collector import MarketDataCollector
from modules.data.collector.index_data_collector import IndexDataCollector

class TestCollector(unittest.TestCase):
    def setUp(self):
        """测试前的准备工作"""
        self.stock_collector = StockDataCollector()
        self.market_collector = MarketDataCollector()
        self.index_collector = IndexDataCollector()
        self.test_symbol = 'sh600000'  # 浦发银行
        self.test_index = '000001'     # 上证指数
    
    def test_stock_daily_data_collection(self):
        """测试股票日线数据采集"""
        # 测试正常场景
        result = self.stock_collector.collect(
            data_type='daily',
            symbol=self.test_symbol,
            start_date=(datetime.now() - timedelta(days=30)).strftime('%Y%m%d'),
            end_date=datetime.now().strftime('%Y%m%d')
        )
        
        # 验证返回结果
        self.assertIsNotNone(result)
        self.assertTrue(self.stock_collector.validate(result))
        self.assertEqual(result['data_type'], 'daily')
        self.assertEqual(result['symbol'], self.test_symbol)
        
        # 测试异常场景 - 无效的股票代码
        invalid_result = self.stock_collector.collect(
            data_type='daily',
            symbol='invalid_symbol'
        )
        self.assertIsNone(invalid_result)
    
    def test_stock_minute_data_collection(self):
        """测试股票分钟线数据采集"""
        result = self.stock_collector.collect(
            data_type='minute',
            symbol=self.test_symbol,
            freq='5',
            days=1
        )
        
        # 验证返回结果
        if result is not None:  # 由于分钟数据可能因为非交易时间而返回None
            self.assertTrue(self.stock_collector.validate(result))
            self.assertEqual(result['data_type'], 'minute')
            self.assertEqual(result['symbol'], self.test_symbol)
            self.assertEqual(result['freq'], '5')
    
    def test_market_data_collection(self):
        """测试市场数据采集"""
        # 测试股票列表采集
        result = self.market_collector.collect(data_type='stock_list')
        
        self.assertIsNotNone(result)
        self.assertTrue(self.market_collector.validate(result))
        self.assertEqual(result['data_type'], 'stock_list')
        
        # 测试股票信息采集
        stock_info = self.market_collector.collect(
            data_type='stock_info',
            symbol=self.test_symbol
        )
        
        if stock_info is not None:  # 股票信息可能不存在
            self.assertTrue(self.market_collector.validate(stock_info))
            self.assertEqual(stock_info['data_type'], 'stock_info')
            self.assertEqual(stock_info['symbol'], self.test_symbol)
    
    def test_index_data_collection(self):
        """测试指数数据采集"""
        result = self.index_collector.collect(
            symbol=self.test_index,
            start_date=(datetime.now() - timedelta(days=30)).strftime('%Y%m%d'),
            end_date=datetime.now().strftime('%Y%m%d'),
            period='daily'
        )
        
        self.assertIsNotNone(result)
        self.assertTrue(self.index_collector.validate(result))
        self.assertEqual(result['symbol'], self.test_index)
        self.assertEqual(result['period'], 'daily')
    
    def test_data_validation(self):
        """测试数据验证功能"""
        # 测试无效数据
        self.assertFalse(self.stock_collector.validate(None))
        self.assertFalse(self.stock_collector.validate({}))
        self.assertFalse(self.stock_collector.validate({'data_type': 'daily'}))
        
        # 测试缺少必要字段的数据
        invalid_data = {
            'data_type': 'daily',
            'symbol': self.test_symbol
        }
        self.assertFalse(self.stock_collector.validate(invalid_data))

if __name__ == '__main__':
    unittest.main()