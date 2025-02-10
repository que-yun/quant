import akshare as ak
import pandas as pd
from datetime import datetime
from loguru import logger
import sqlalchemy as sa
from sqlalchemy import text

class MarketData:
    def __init__(self):
        self.engine = sa.create_engine("sqlite:////Users//admin//work//quant//trading.db")
        self._initialize_tables()
        
    def _initialize_tables(self):
        """初始化数据库表结构"""
        try:
            conn = self.engine.connect()
            
            # 创建个股基本面信息表
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_basic_info (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                industry TEXT,
                market TEXT,
                total_share REAL,
                circulating_share REAL,
                market_cap REAL,
                circulating_market_cap REAL,
                pe_ratio REAL,
                pb_ratio REAL,
                ps_ratio REAL,
                pcf_ratio REAL,
                update_time TEXT
            )
            """))
            
            conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_stock_basic_info_update ON stock_basic_info(symbol, update_time)
            """))
            
            # 创建日线数据表
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_bars (
                date TEXT,
                symbol TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                amplitude REAL,
                pct_change REAL,
                price_change REAL,
                turnover_rate REAL,
                update_time TEXT,
                PRIMARY KEY (date, symbol)
            )
            """))
            
            conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_daily_bars_update ON daily_bars(symbol, update_time)
            """))
            
            # 创建周线数据表
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS weekly_bars (
                date TEXT,
                symbol TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                amplitude REAL,
                pct_change REAL,
                price_change REAL,
                turnover_rate REAL,
                update_time TEXT,
                PRIMARY KEY (date, symbol)
            )
            """))
            
            conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_weekly_bars_update ON weekly_bars(symbol, update_time)
            """))
            
            # 创建月线数据表
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS monthly_bars (
                date TEXT,
                symbol TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                amplitude REAL,
                pct_change REAL,
                price_change REAL,
                turnover_rate REAL,
                update_time TEXT,
                PRIMARY KEY (date, symbol)
            )
            """))
            
            conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_monthly_bars_update ON monthly_bars(symbol, update_time)
            """))
            
            # 创建分钟数据表
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS minute_bars (
                date TEXT,
                symbol TEXT,
                freq TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                amplitude REAL,
                pct_change REAL,
                price_change REAL,
                turnover_rate REAL,
                update_time TEXT,
                PRIMARY KEY (date, symbol, freq)
            )
            """))
            
            conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_minute_bars_update ON minute_bars(symbol, update_time)
            """))
            
            # 创建策略表现表
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS strategy_performance (
                strategy_name TEXT,
                start_date TEXT,
                end_date TEXT,
                initial_capital REAL,
                final_capital REAL,
                total_return REAL,
                annual_return REAL,
                max_drawdown REAL,
                win_rate REAL,
                sharpe_ratio REAL,
                update_time TEXT,
                PRIMARY KEY (strategy_name, start_date, end_date)
            )
            """))
            
            # 创建资金流向表
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fund_flow (
                date TEXT,
                symbol TEXT,
                market TEXT,
                close REAL,
                pct_change REAL,
                main_net_flow REAL,
                main_net_flow_rate REAL,
                super_big_net_flow REAL,
                super_big_net_flow_rate REAL,
                big_net_flow REAL,
                big_net_flow_rate REAL,
                medium_net_flow REAL,
                medium_net_flow_rate REAL,
                small_net_flow REAL,
                small_net_flow_rate REAL,
                update_time TEXT,
                PRIMARY KEY (date, symbol)
            )
            """))
            
            conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_fund_flow_update ON fund_flow(symbol, update_time)
            """))
            
            conn.commit()
            logger.info("数据表初始化完成")
            
        except Exception as e:
            logger.error(f"数据表初始化失败: {str(e)}")
            raise
        finally:
            conn.close()
    
    def get_stock_list(self):
        """获取沪深A股股票列表和基本面数据"""
        try:
            # 获取沪市A股数据
            sh_df = ak.stock_sh_a_spot_em()
            # 获取深市A股数据
            sz_df = ak.stock_sz_a_spot_em()
            
            # 合并数据
            df = pd.concat([sh_df, sz_df], ignore_index=True)
            
            # 过滤掉退市股票（通过检查最新价是否为空或0来判断）
            df = df[df['最新价'].notna() & (df['最新价'] != 0)]
            
            # 标准化字段名
            df = df.rename(columns={
                "代码": "symbol",
                "名称": "name",
                "所属行业": "industry",
                "总股本": "total_share",
                "流通股": "circulating_share",
                "总市值": "market_cap",
                "流通市值": "circulating_market_cap",
                "市盈率-动态": "pe_ratio",
                "市净率": "pb_ratio",
                "市销率": "ps_ratio",
                "市现率": "pcf_ratio"
            })
            
            # 添加市场标识
            df['market'] = df['symbol'].apply(lambda x: 'sh' if x.startswith('6') else 'sz')
            df['symbol'] = df.apply(
                lambda x: f"{x['market']}{x['symbol']}",
                axis=1
            )
            
            # 添加更新时间
            df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 存储到数据库
            df.to_sql(
                "stock_basic_info",
                self.engine,
                if_exists="replace",
                index=False
            )
            
            logger.success(f"成功获取股票列表，共{len(df)}只股票")
            return df['symbol'].tolist()
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []
    
    def get_stock_info(self, symbol):
        """获取单个股票的基本信息"""
        try:
            # 从数据库获取股票信息
            sql = f"SELECT * FROM stock_basic_info WHERE symbol = '{symbol}'"
            df = pd.read_sql(sql, self.engine)
            
            if df.empty:
                logger.warning(f"未找到股票{symbol}的基本信息")
                return None
            
            return df.iloc[0].to_dict()
            
        except Exception as e:
            logger.error(f"获取股票{symbol}基本信息失败: {str(e)}")
            return None