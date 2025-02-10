import akshare as ak

def test_stock_data():
    try:
        print('正在获取股票数据...')
        df = ak.stock_zh_a_spot_em()
        target = df[df['代码'] == '600000']
        print('数据获取成功：')
        print(target)
        
        # 测试历史数据获取
        print('\n正在获取历史数据...')
        hist_data = ak.stock_zh_a_hist(
            symbol='600000',
            period='daily',
            start_date='20240101',
            end_date='20240131',
            adjust=''
        )
        print('历史数据获取成功：')
        print(hist_data.head())
    except Exception as e:
        print(f'错误：{str(e)}')

if __name__ == '__main__':
    test_stock_data()