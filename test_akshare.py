import akshare as ak
import pandas as pd

def test_minute_data():
    try:
        # 获取分钟级数据
        df = ak.stock_zh_a_hist_min_em(
            symbol='600000',
            period='5',
            start_date='20240101',
            end_date='20240208',
            adjust=''
        )
        
        print('数据列名:', df.columns.tolist())
        print('\n数据类型:', df.dtypes)
        print('\n前5行数据:\n', df.head())
        
    except Exception as e:
        print(f'获取数据失败: {str(e)}')

if __name__ == '__main__':
    test_minute_data()