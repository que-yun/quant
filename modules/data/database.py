import os
import sqlite3
import pandas as pd
from datetime import datetime
from loguru import logger

class Database:
    def __init__(self, db_path=None):
        if db_path is None:
            # 默认在项目根目录创建数据库文件
            root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            db_path = os.path.join(root_dir, 'trading.db')
        
        self.db_path = db_path
        self.logger = logger

    def initialize(self):
        """初始化数据库，创建必要的表"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 创建个股基本面信息表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_basic_info (
                symbol TEXT PRIMARY KEY,         -- 股票代码，格式：sh600000/sz000001
                name TEXT,                      -- 股票名称
                industry TEXT,                  -- 所属行业
                market TEXT,                    -- 市场类型：sh/sz
                total_share REAL,               -- 总股本（万股）
                circulating_share REAL,         -- 流通股本（万股）
                market_cap REAL,                -- 总市值（万元）
                circulating_market_cap REAL,    -- 流通市值（万元）
                pe_ratio REAL,                  -- 市盈率
                pb_ratio REAL,                  -- 市净率
                ps_ratio REAL,                  -- 市销率
                pcf_ratio REAL,                 -- 市现率
                update_time TEXT                -- 数据更新时间
            )
            """)

            # 创建日线数据表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_bars (
                date TEXT,                      -- 交易日期，格式：YYYY-MM-DD
                symbol TEXT,                    -- 股票代码，格式：sh600000/sz000001
                open REAL,                      -- 开盘价
                high REAL,                      -- 最高价
                low REAL,                       -- 最低价
                close REAL,                     -- 收盘价
                volume REAL,                    -- 成交量（手）
                amount REAL,                    -- 成交额（元）
                amplitude REAL,                 -- 振幅（%）
                pct_change REAL,                -- 涨跌幅（%）
                price_change REAL,              -- 涨跌额（元）
                turnover_rate REAL,             -- 换手率（%）
                update_time TEXT,               -- 数据更新时间
                PRIMARY KEY (date, symbol)
            )
            """)

            # 创建周线数据表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS weekly_bars (
                date TEXT,                      -- 交易日期，格式：YYYY-MM-DD
                symbol TEXT,                    -- 股票代码，格式：sh600000/sz000001
                open REAL,                      -- 开盘价
                high REAL,                      -- 最高价
                low REAL,                       -- 最低价
                close REAL,                     -- 收盘价
                volume REAL,                    -- 成交量（手）
                amount REAL,                    -- 成交额（元）
                amplitude REAL,                 -- 振幅（%）
                pct_change REAL,                -- 涨跌幅（%）
                price_change REAL,              -- 涨跌额（元）
                turnover_rate REAL,             -- 换手率（%）
                update_time TEXT,               -- 数据更新时间
                PRIMARY KEY (date, symbol)
            )
            """)

            # 创建月线数据表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS monthly_bars (
                date TEXT,                      -- 交易日期，格式：YYYY-MM-DD
                symbol TEXT,                    -- 股票代码，格式：sh600000/sz000001
                open REAL,                      -- 开盘价
                high REAL,                      -- 最高价
                low REAL,                       -- 最低价
                close REAL,                     -- 收盘价
                volume REAL,                    -- 成交量（手）
                amount REAL,                    -- 成交额（元）
                amplitude REAL,                 -- 振幅（%）
                pct_change REAL,                -- 涨跌幅（%）
                price_change REAL,              -- 涨跌额（元）
                turnover_rate REAL,             -- 换手率（%）
                update_time TEXT,               -- 数据更新时间
                PRIMARY KEY (date, symbol)
            )
            """)

            # 创建分钟数据表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS minute_bars (
                date TEXT,                      -- 交易时间，格式：YYYY-MM-DD HH:MM:SS
                symbol TEXT,                    -- 股票代码，格式：sh600000/sz000001
                freq TEXT,                      -- 分钟频率，如：1min/5min/15min/30min/60min
                open REAL,                      -- 开盘价
                high REAL,                      -- 最高价
                low REAL,                       -- 最低价
                close REAL,                     -- 收盘价
                volume REAL,                    -- 成交量（手）
                amount REAL,                    -- 成交额（元）
                amplitude REAL,                 -- 振幅（%）
                pct_change REAL,                -- 涨跌幅（%）
                price_change REAL,              -- 涨跌额（元）
                turnover_rate REAL,             -- 换手率（%）
                update_time TEXT,               -- 数据更新时间
                PRIMARY KEY (date, symbol, freq)
            )
            """)


            # 创建策略表现表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS strategy_performance (
                strategy_name TEXT,             -- 策略名称
                start_date TEXT,                -- 回测开始日期，格式：YYYY-MM-DD
                end_date TEXT,                  -- 回测结束日期，格式：YYYY-MM-DD
                initial_capital REAL,           -- 初始资金（元）
                final_capital REAL,             -- 最终资金（元）
                total_return REAL,              -- 总收益率（%）
                annual_return REAL,             -- 年化收益率（%）
                max_drawdown REAL,              -- 最大回撤（%）
                win_rate REAL,                  -- 胜率（%）
                sharpe_ratio REAL,              -- 夏普比率
                update_time TEXT,               -- 数据更新时间
                PRIMARY KEY (strategy_name, start_date, end_date)
            )
            """)

            # 创建资金流向表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fund_flow (
                date TEXT,                      -- 交易日期，格式：YYYY-MM-DD
                symbol TEXT,                    -- 股票代码，格式：sh600000/sz000001
                market TEXT,                    -- 市场类型：sh（上海）/sz（深圳）
                close REAL,                     -- 收盘价（元）
                pct_change REAL,                -- 涨跌幅（%）
                main_net_flow REAL,             -- 主力净流入金额（元）
                main_net_flow_rate REAL,        -- 主力净流入占比（%）
                super_big_net_flow REAL,        -- 超大单净流入金额（元）
                super_big_net_flow_rate REAL,   -- 超大单净流入占比（%）
                big_net_flow REAL,              -- 大单净流入金额（元）
                big_net_flow_rate REAL,         -- 大单净流入占比（%）
                medium_net_flow REAL,           -- 中单净流入金额（元）
                medium_net_flow_rate REAL,      -- 中单净流入占比（%）
                small_net_flow REAL,            -- 小单净流入金额（元）
                small_net_flow_rate REAL,       -- 小单净流入占比（%）
                update_time TEXT,               -- 数据更新时间
                PRIMARY KEY (date, symbol)
            )
            """)


            conn.commit()
            self.logger.info("数据库初始化成功")
            return True

        except Exception as e:
            self.logger.error(f"数据库初始化失败: {str(e)}")
            return False

        finally:
            if conn:
                conn.close()

    def save_stock_data(self, symbol, df, freq='D'):
        """保存股票数据到数据库
        Args:
            symbol (str): 股票代码
            df (pd.DataFrame): 股票数据
            freq (str): 数据频率，D-日线，W-周线，M-月线，1min/5min/15min/30min/60min-分钟线
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 添加symbol列
            df = df.copy()
            df['symbol'] = symbol
            
            # 重置索引，将日期列转换为普通列
            df.reset_index(inplace=True)
            
            # 添加更新时间
            df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 重命名列以匹配新的表结构
            df = df.rename(columns={
                'turnover': 'turnover_rate'
            })
            
            # 根据频率选择保存的表
            if freq == 'D':
                table_name = 'daily_bars'
            elif freq == 'W':
                table_name = 'weekly_bars'
            elif freq == 'M':
                table_name = 'monthly_bars'
            elif freq in ['1min', '5min', '15min', '30min', '60min']:
                table_name = 'minute_bars'
                df['freq'] = freq
            else:
                raise ValueError(f'不支持的数据频率：{freq}')
            
            # 保存到数据库，使用append模式并设置index=False
            df.to_sql(table_name, conn, if_exists='append', index=False)
            
            self.logger.info(f"成功保存{symbol}的{freq}周期数据到{table_name}表")
            return True

        except Exception as e:
            self.logger.error(f"保存股票数据失败: {str(e)}")
            return False

        finally:
            if conn:
                conn.close()

    def load_stock_data(self, symbol, start_date=None, end_date=None):
        """从数据库加载股票数据
        Args:
            symbol (str): 股票代码
            start_date (str): 开始日期，格式：YYYY-MM-DD
            end_date (str): 结束日期，格式：YYYY-MM-DD
        Returns:
            pd.DataFrame: 股票数据
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = f"SELECT date, symbol, open, high, low, close, volume, amount, amplitude, pct_change, price_change, turnover_rate FROM daily_bars WHERE symbol = '{symbol}'"
            if start_date:
                query += f" AND date >= '{start_date}'"
            if end_date:
                query += f" AND date <= '{end_date}'"
            query += " ORDER BY date"
            
            df = pd.read_sql(query, conn)
            
            # 设置日期索引
            df.set_index('date', inplace=True)
            df.index = pd.to_datetime(df.index)
            
            # 删除symbol列
            df.drop('symbol', axis=1, inplace=True)
            
            self.logger.info(f"成功从数据库加载{symbol}的股票数据")
            return df

        except Exception as e:
            self.logger.error(f"加载股票数据失败: {str(e)}")
            return None

        finally:
            if conn:
                conn.close()

    def save_indicators(self, symbol, date, indicators):
        """保存技术指标数据
        Args:
            symbol (str): 股票代码
            date (str): 日期
            indicators (dict): 指标数据，key为指标名称，value为指标值
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            for indicator_name, value in indicators.items():
                cursor.execute("""
                INSERT OR REPLACE INTO technical_indicators 
                (date, symbol, indicator_name, value, update_time) 
                VALUES (?, ?, ?, ?, datetime('now'))
                """, (date, symbol, indicator_name, value))

            conn.commit()
            self.logger.info(f"成功保存{symbol}的技术指标数据")
            return True

        except Exception as e:
            self.logger.error(f"保存技术指标数据失败: {str(e)}")
            return False

        finally:
            if conn:
                conn.close()

    def save_trade_record(self, record):
        """保存交易记录
        Args:
            record (dict): 交易记录
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
            INSERT INTO trade_records 
            (date, symbol, strategy_name, action, price, volume, amount, commission, update_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record['date'],
                record['symbol'],
                record['strategy_name'],
                record['action'],
                record['price'],
                record['volume'],
                record['amount'],
                record['commission']
            ))

            conn.commit()
            self.logger.info("成功保存交易记录")
            return True

        except Exception as e:
            self.logger.error(f"保存交易记录失败: {str(e)}")
            return False

        finally:
            if conn:
                conn.close()

    def save_strategy_performance(self, performance):
        """保存策略表现数据
        Args:
            performance (dict): 策略表现数据
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
            INSERT OR REPLACE INTO strategy_performance 
            (strategy_name, start_date, end_date, initial_capital, final_capital,
             total_return, annual_return, max_drawdown, win_rate, sharpe_ratio)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                performance['strategy_name'],
                performance['start_date'],
                performance['end_date'],
                performance['initial_capital'],
                performance['final_capital'],
                performance['total_return'],
                performance['annual_return'],
                performance['max_drawdown'],
                performance['win_rate'],
                performance['sharpe_ratio']
            ))

            conn.commit()
            self.logger.info("成功保存策略表现数据")
            return True

        except Exception as e:
            self.logger.error(f"保存策略表现数据失败: {str(e)}")
            return False

        finally:
            if conn:
                conn.close()