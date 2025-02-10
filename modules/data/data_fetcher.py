import akshare as ak
import pandas as pd
import sqlalchemy as sa
from loguru import logger
from datetime import datetime, timedelta
import time
from .database import Database

class AStockData:
    def __init__(self):
        self.engine = sa.create_engine("sqlite:////Users//admin//work//quant//trading.db")
        
    def _convert_ak_date(self, date_str):
        """转换AKShare的特殊日期格式（1120 -> 11:20, 09:30）"""
        return f"{date_str[:2]}:{date_str[2:]}"
    
    def _check_data_exists(self, symbol, table_name, start_date=None, end_date=None, freq=None):
        """统一的数据存在性检查
        :param symbol: 股票代码
        :param table_name: 表名
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param freq: 频率（分钟数据专用）
        :return: bool 是否已存在数据
        """
        try:
            query = f"SELECT MAX(update_time) as latest_update FROM {table_name} WHERE symbol='{symbol}'"
            if freq:
                query += f" AND freq='{freq}'"
            if start_date and end_date:
                query += f" AND date BETWEEN '{start_date}' AND '{end_date}'"

            result = pd.read_sql(query, self.engine)
            if not result.empty and result['latest_update'].iloc[0] is not None:
                latest_update = pd.to_datetime(result['latest_update'].iloc[0])
                # 如果最后更新时间在今天，说明数据是最新的
                if latest_update.date() == datetime.now().date():
                    return True
            return False
        except Exception as e:
            logger.error(f"检查数据存在性失败: {str(e)}")
            return False

    def get_daily_data(self, symbol="600000", start_date=None, end_date=None, period="daily", timeout=30):
        try:
            # 处理日期参数
            if not end_date:
                end_date = datetime.now().strftime("%Y%m%d")
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
                
            # 确保不会获取未来数据
            current_date = datetime.now().strftime("%Y%m%d")
            if end_date > current_date:
                end_date = current_date
            if start_date > current_date:
                start_date = (datetime.strptime(current_date, "%Y%m%d") - timedelta(days=365)).strftime("%Y%m%d")
            
            # 确保股票代码格式正确
            original_symbol = symbol
            if symbol.startswith('sh') or symbol.startswith('sz'):
                symbol = symbol[2:]
            
            # 根据周期选择表名
            table_map = {
                'daily': 'daily_bars',
                'weekly': 'weekly_bars',
                'monthly': 'monthly_bars'
            }
            table_name = table_map.get(period)
            if not table_name:
                logger.error(f"不支持的数据周期：{period}")
                return False

            # 检查数据是否需要更新
            if self._check_data_exists(original_symbol, table_name, start_date, end_date):
                logger.info(f"{original_symbol}的{period}数据已是最新，无需更新")
                return True
            
            # 获取数据
            max_retries = 3
            retry_count = 0
            retry_delay = 3
            
            while retry_count < max_retries:
                try:
                    df = ak.stock_zh_a_hist(
                        symbol=symbol, 
                        period=period,
                        start_date=start_date,
                        end_date=end_date,
                        adjust="",
                        timeout=timeout
                    )
                    
                    if df is None or df.empty:
                        retry_count += 1
                        if retry_count < max_retries:
                            logger.warning(f"第{retry_count}次获取数据为空，将进行重试...")
                            time.sleep(retry_delay * retry_count)
                            continue
                        else:
                            logger.error(f"获取{original_symbol}的{period}数据为空")
                            return False
                    
                    # 字段标准化
                    df = df.rename(columns={
                        "日期": "date",
                        "开盘": "open",
                        "收盘": "close",
                        "最高": "high",
                        "最低": "low",
                        "成交量": "volume",
                        "成交额": "amount",
                        "振幅": "amplitude",
                        "涨跌幅": "pct_change",
                        "涨跌额": "price_change",
                        "换手率": "turnover_rate"
                    })
                    
                    # 数据验证和处理
                    if 'date' not in df.columns:
                        logger.error(f"数据字段映射失败，当前列名：{df.columns.tolist()}")
                        return False
                    
                    df["date"] = pd.to_datetime(df["date"], errors='coerce')
                    if df["date"].isnull().any():
                        logger.error(f"{original_symbol}存在无效日期数据")
                        return False
                    
                    key_columns = ["open", "high", "low", "close", "volume", "amount"]
                    if df[key_columns].isnull().any().any():
                        logger.error(f"{original_symbol}存在无效数据")
                        return False
                    
                    # 添加必要字段
                    df["symbol"] = original_symbol
                    df["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # 保存数据
                    df.to_sql(
                        table_name,
                        self.engine,
                        if_exists="append",
                        index=False
                    )
                    
                    logger.success(f"{period}数据更新完成：{original_symbol}")
                    return True
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"第{retry_count}次获取数据失败：{str(e)}，将进行重试...")
                        time.sleep(retry_delay * retry_count)
                    else:
                        logger.error(f"日线数据获取失败：{str(e)}")
                        return False
            
            return False
            
        except Exception as e:
            logger.error(f"日线数据获取失败：{str(e)}")
            return False

    def get_minute_data(self, symbol="600000", freq="5", days=30):
        """
        获取分钟级数据（自动处理时间格式）
        :param freq: 分钟频率（1/5/15/30/60）
        :param days: 最近N天的数据
        """
        try:
            # 分段获取数据，每次获取60天
            segment_days = 60
            total_segments = (days + segment_days - 1) // segment_days
            
            for segment in range(total_segments):
                # 计算当前段的时间范围
                end_date = (datetime.now() - timedelta(days=segment*segment_days))
                start_date = end_date - timedelta(days=min(segment_days, days-segment*segment_days))
                
                # 格式化日期
                end_time = end_date.strftime("%Y%m%d")
                start_time = start_date.strftime("%Y%m%d")
                
                # 判断是否为交易日
                if not self._is_trading_day(end_time):
                    logger.info(f"{end_time}为非交易日，跳过数据获取")
                    continue
                
                # 确保股票代码格式正确
                stock_code = symbol
                if stock_code.startswith('sh') or stock_code.startswith('sz'):
                    stock_code = stock_code[2:]
                
                # 检查数据是否需要更新
                if self._check_data_exists(symbol, 'minute_bars', start_time, end_time, f"{freq}min"):
                    logger.info(f"{symbol}的{freq}分钟数据已是最新，无需更新")
                    continue
                
                # 添加重试机制
                max_retries = 3
                retry_count = 0
                df = None
                
                while retry_count < max_retries:
                    try:
                        df = ak.stock_zh_a_hist_min_em(
                            symbol=stock_code,
                            period=freq,
                            start_date=start_time,
                            end_date=end_time,
                            adjust=""  # 不复权
                        )
                        
                        if df is None or df.empty:
                            retry_count += 1
                            if retry_count < max_retries:
                                logger.warning(f"第{retry_count}次获取分钟数据为空，将进行重试...")
                                time.sleep(2)
                                continue
                            else:
                                logger.warning(f"时间段{start_time}-{end_time}的分钟数据为空，跳过处理")
                                break
                        
                        # 字段标准化
                        df = df.rename(columns={
                            "时间": "date",
                            "开盘": "open",
                            "收盘": "close",
                            "最高": "high",
                            "最低": "low",
                            "成交量": "volume",
                            "成交额": "amount",
                            "振幅": "amplitude",
                            "涨跌幅": "pct_change",
                            "涨跌额": "price_change",
                            "换手率": "turnover_rate"
                        })
                        
                        # 数据验证和处理
                        if 'date' not in df.columns:
                            logger.error(f"数据字段映射失败，当前列名：{df.columns.tolist()}")
                            break
                        
                        df["date"] = pd.to_datetime(df["date"], errors='coerce')
                        if df["date"].isnull().any():
                            logger.error(f"{symbol}存在无效日期数据")
                            break
                        
                        key_columns = ["open", "high", "low", "close", "volume", "amount"]
                        if df[key_columns].isnull().any().any():
                            logger.error(f"{symbol}存在无效数据")
                            break
                        
                        # 添加必要字段
                        df["symbol"] = symbol
                        df["freq"] = f"{freq}min"
                        df["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        # 保存数据
                        df.to_sql(
                            "minute_bars",
                            self.engine,
                            if_exists="append",
                            index=False
                        )
                        
                        logger.success(f"分钟数据更新完成：{symbol} {freq}分钟 {start_time}-{end_time}")
                        break  # 数据处理成功，跳出重试循环
                        
                    except Exception as e:
                        retry_count += 1
                        if retry_count < max_retries:
                            logger.warning(f"第{retry_count}次获取分钟数据失败：{str(e)}，将进行重试...")
                            time.sleep(2)
                        else:
                            logger.error(f"分钟数据获取失败：{str(e)}")
                            break
                
                # 添加延时避免频繁请求
                time.sleep(1)
            
            return True
        except Exception as e:
            logger.error(f"分钟数据获取失败：{str(e)}")
            return False

    def get_index_data(self, symbol, start_date=None, end_date=None, period="daily"):
        """
        获取指数数据
        :param symbol: 指数代码，如：000001（上证指数）
        :param start_date: 开始日期，格式：YYYYMMDD
        :param end_date: 结束日期，格式：YYYYMMDD
        :param period: 周期，可选值：daily, weekly, monthly
        :return: pd.DataFrame 指数数据
        """
        try:
            # 处理日期参数
            if not end_date:
                end_date = datetime.now().strftime("%Y%m%d")
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

            # 获取指数数据
            df = ak.index_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=""
            )

            # 字段标准化
            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "涨跌幅": "pct_change",
                "涨跌额": "price_change"
            })
            df["symbol"] = symbol
            df["date"] = pd.to_datetime(df["date"], format='mixed')
            
            # 存储到数据库
            df.to_sql(
                "index_data",
                self.engine,
                if_exists="append",
                index=False
            )
            
            logger.success(f"成功获取{symbol}的指数数据，共{len(df)}条记录")
            return df
        except Exception as e:
            logger.error(f"获取指数数据失败: {str(e)}")
            return None

    def get_historical_data(self, start_date, end_date, symbols=None):
        """
        获取多个股票的历史数据
        :param start_date: 开始日期，格式：YYYYMMDD
        :param end_date: 结束日期，格式：YYYYMMDD
        :param symbols: 股票代码列表，默认为None，表示获取上证指数
        :return: dict 股票数据字典，key为股票代码，value为DataFrame
        """
        if symbols is None:
            symbols = ['000001']  # 默认获取上证指数

        data_dict = {}
        for symbol in symbols:
            if symbol.startswith('0') or symbol.startswith('3') or symbol.startswith('6'):
                self.get_daily_data(symbol, start_date, end_date)
            else:
                self.get_index_data(symbol, start_date, end_date)

        return data_dict

    def _is_trading_day(self, date_str):
        """判断是否为交易日
        :param date_str: 日期字符串，格式为YYYYMMDD
        :return: bool
        """
        try:
            # 转换日期字符串为datetime对象
            date = datetime.strptime(date_str, "%Y%m%d")
            
            # 判断是否为周末
            if date.weekday() >= 5:  # 5是周六，6是周日
                return False
            
            # 尝试获取当天的日线数据，如果能获取到数据说明是交易日
            df = ak.stock_zh_a_hist(
                symbol="000001",  # 使用上证指数作为判断依据
                period="daily",
                start_date=date_str,
                end_date=date_str,
                adjust=""
            )
            
            return not (df is None or df.empty)
            
        except Exception as e:
            logger.warning(f"交易日判断失败：{str(e)}")
            return True  # 如果判断失败，默认为交易日

if __name__ == "__main__":
    astock = AStockData()
    # 测试获取浦发银行日线数据
    astock.get_daily_data(symbol="sh600000", start_date="20240101")
    # 测试获取最近5天的5分钟数据
    astock.get_minute_data(symbol="sh600000", freq="5", days=30)
    # 测试获取上证指数数据
    astock.get_index_data(symbol="000001", start_date="20240101")