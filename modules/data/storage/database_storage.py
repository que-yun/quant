import os
import sqlite3
import pandas as pd
import sqlalchemy as sa
from datetime import datetime, timedelta
from threading import Lock
from sqlalchemy.pool import QueuePool
from config.config_manager import ConfigManager
from modules.data.storage.merge_strategy import DataMergeStrategy
from modules.utils.log_manager import logger

class DatabaseStorage:
    _instance = None
    _initialized = False
    _db_initialized = False
    _lock = Lock()
    
    def __new__(cls, db_path=None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseStorage, cls).__new__(cls)
            return cls._instance

    def __init__(self, db_path=None):
        if not self._initialized:
            config = ConfigManager()
            self.db_path = db_path if db_path else config.database_path
            self.logger = logger
            
            # 使用DatabasePool获取数据库引擎
            self.db_pool = None
            self.engine = None
            self._initialize_db_pool()
            
            self._initialized = True
            self.logger.debug("数据库存储初始化完成")

    def _initialize_db_pool(self):
        """初始化数据库连接池"""
        try:
            from .db_pool import DatabasePool
            self.db_pool = DatabasePool()
            self.engine = self.db_pool.get_engine()
        except Exception as e:
            self.logger.error(f"初始化数据库连接池失败: {str(e)}")

    def _execute_migration_files(self, conn):
        """执行数据库迁移文件"""
        try:
            migrations_dir = os.path.join(os.path.dirname(self.db_path), 'migrations')
            if not os.path.exists(migrations_dir):
                self.logger.warning("未找到迁移文件目录，跳过执行迁移文件")
                return True

            # 获取所有SQL迁移文件并按文件名排序
            migration_files = sorted([f for f in os.listdir(migrations_dir) if f.endswith('.sql')])
            
            for file_name in migration_files:
                file_path = os.path.join(migrations_dir, file_name)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        sql_script = f.read()
                        cursor = conn.cursor()
                        cursor.executescript(sql_script)
                        conn.commit()
                        self.logger.info(f"成功执行迁移文件：{file_name}")
                except Exception as e:
                    self.logger.error(f"执行迁移文件 {file_name} 失败: {str(e)}")
                    return False
            return True
        except Exception as e:
            self.logger.error(f"执行迁移文件过程中发生错误: {str(e)}")
            return False

    def initialize(self):
        """初始化数据库，创建必要的表"""
        # 如果数据库已经初始化过，直接返回True
        if self._db_initialized:
            self.logger.debug("数据库已经初始化过，跳过初始化")
            return True
            
        if not os.path.exists(os.path.dirname(self.db_path)):
            os.makedirs(os.path.dirname(self.db_path))

        try:
            conn = sqlite3.connect(self.db_path)
            
            # 执行迁移文件
            if not self._execute_migration_files(conn):
                self.logger.error("执行迁移文件失败")
                return False

            self.logger.info("数据库初始化成功")
            self._db_initialized = True
            return True

        except Exception as e:
            self.logger.error(f"数据库初始化失败: {str(e)}")
            return False

        finally:
            if conn:
                conn.close()

    def save_stock_data(self, symbol: str, df: pd.DataFrame, freq: str = 'D') -> bool:
        """保存股票数据到数据库
        Args:
            symbol (str): 股票代码
            df (pd.DataFrame): 股票数据
            freq (str): 数据频率，D-日线，W-周线，M-月线，1min/5min/15min/30min/60min-分钟线
        """
        try:
            if df is None or df.empty:
                return False

            from .merge_strategy import DataMergeStrategy
            df = DataMergeStrategy.prepare_data_for_merge(df, symbol)
            
            # 确保DataFrame包含所需的所有列
            required_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'amplitude', 'pct_change', 'price_change', 'turnover']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            
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
            
            # 获取日期范围
            min_date = df['date'].min()
            max_date = df['date'].max()
            
            # 创建临时表名
            temp_table = f'temp_{table_name}_{datetime.now().strftime("%Y%m%d%H%M%S")}'
            
            with self.engine.connect() as conn:
                # 获取已有数据
                existing_data_query = f"""
                    SELECT * FROM {table_name} 
                    WHERE symbol = ? AND date NOT BETWEEN ? AND ?
                """
                params = [symbol, min_date, max_date]
                existing_df = pd.read_sql_query(existing_data_query, conn, params=params)
                
                if not existing_df.empty:
                    # 合并新旧数据
                    df = pd.concat([df, existing_df], ignore_index=True)
                
                # 保存合并后的数据到临时表
                df.to_sql(temp_table, conn, if_exists='replace', index=False)
                
                # 删除原表中的相关数据
                delete_query = f"DELETE FROM {table_name} WHERE symbol = ? AND date BETWEEN ? AND ?"
                conn.execute(delete_query, [symbol, min_date, max_date])
                
                # 从临时表插入数据到原表
                insert_query = f"INSERT INTO {table_name} SELECT * FROM {temp_table}"
                conn.execute(insert_query)
                
                # 删除临时表
                conn.execute(f"DROP TABLE {temp_table}")
                
                # 提交事务
                conn.commit()
            
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
            self.logger.error(f"从数据库加载股票数据失败: {str(e)}")
            return None

        finally:
            if conn:
                conn.close()

    def initialize_history_data(self, start_date: str = None, end_date: str = None) -> bool:
        """初始化历史数据
        Args:
            start_date: 开始日期，格式YYYYMMDD，默认为一年前
            end_date: 结束日期，格式YYYYMMDD，默认为当前日期
        Returns:
            bool: 是否初始化成功
        """
        try:
            # 获取市场数据服务和数据采集器
            from modules.data.service.market_data_service import MarketDataService
            from modules.data.collector.stock.stock_data_collector import StockDataCollector
            market_service = MarketDataService()
            
            # 获取配置信息
            config = market_service.get_config()
            if not config:
                self.logger.error("无法获取配置信息")
                return False
            
            # 设置默认日期范围
            if not end_date:
                end_date = (datetime.now() - timedelta(days=0)).strftime('%Y%m%d')
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            
            # 获取股票列表
            stock_list = market_service.get_stock_list()
            if stock_list is None or not isinstance(stock_list, pd.DataFrame) or stock_list.empty:
                self.logger.error("无法获取股票列表或数据格式错误")
                return False
            
            if 'symbol' not in stock_list.columns:
                self.logger.error("股票列表数据缺少symbol字段")
                return False
                
            symbols = stock_list['symbol'].tolist()
            
            # 初始化数据采集器
            collector = StockDataCollector()
            
            # 根据配置初始化日线和周线数据
            if config.get('init_daily_data', True) or config.get('init_weekly_data', True):
                # 使用批量查询优化数据存在性检查
                existing_data = {}
                batch_size = 100  # 每批处理的股票数量
                for i in range(0, len(symbols), batch_size):
                    batch_symbols = symbols[i:i + batch_size]
                    symbols_str = "','".join(batch_symbols)
                    query = f"SELECT DISTINCT symbol, date FROM daily_bars WHERE symbol IN ('{symbols_str}') AND date BETWEEN '{start_date}' AND '{end_date}'"
                    try:
                        df = pd.read_sql(query, self.engine)
                        for symbol, dates in df.groupby('symbol')['date']:
                            existing_data[symbol] = set(dates.astype(str))
                    except Exception as e:
                        self.logger.warning(f"批量检查数据存在性失败: {str(e)}")
                
                # 过滤出需要采集数据的股票
                symbols_to_collect = [symbol for symbol in symbols if symbol not in existing_data]
                if symbols_to_collect:
                    self.logger.info(f"开始采集{len(symbols_to_collect)}只股票的数据")
                    
                    # 采集日线数据
                    if config.get('init_daily_data', True):
                        daily_data = collector.batch_collect_daily_data(symbols_to_collect, start_date, end_date)
                        if daily_data is not None and not daily_data.empty:
                            # 获取已有数据
                            existing_daily_data = pd.read_sql(
                                f"SELECT * FROM daily_bars WHERE symbol IN ({','.join(['?']*len(symbols_to_collect))}) AND date BETWEEN ? AND ?",
                                self.engine,
                                params=symbols_to_collect + [start_date, end_date]
                            )
                            
                            # 使用 DataMergeStrategy 合并数据
                            if not existing_daily_data.empty:
                                combined_daily_data = DataMergeStrategy.merge_dataframes(
                                    daily_data,
                                    existing_daily_data
                                )
                            else:
                                combined_daily_data = daily_data
                            
                            try:
                                # 使用事务保证数据一致性
                                with self.engine.begin() as conn:
                                    # 删除要更新的数据范围
                                    delete_query = f"DELETE FROM daily_bars WHERE symbol IN ({','.join(['?']*len(symbols_to_collect))}) AND date BETWEEN ? AND ?"
                                    conn.execute(delete_query, symbols_to_collect + [start_date, end_date])
                                    
                                    # 保存合并后的数据
                                    combined_daily_data.to_sql('daily_bars', conn, if_exists='append', index=False, method='multi', chunksize=5000)
                                self.logger.info(f"成功保存{len(combined_daily_data)}条日线数据")
                            except Exception as e:
                                self.logger.error(f"保存日线数据失败: {str(e)}")
                    
                    # 采集周线数据
                    if config.get('init_weekly_data', True):
                        weekly_data_list = []
                        for batch_df in collector.batch_collect_weekly_data(symbols_to_collect, start_date, end_date):
                            if batch_df is not None and not batch_df.empty:
                                weekly_data_list.append(batch_df)
                        
                        if weekly_data_list:
                            # 合并所有周线数据
                            weekly_data = pd.concat(weekly_data_list, ignore_index=True)
                            
                            # 获取已有数据
                            existing_weekly_data = pd.read_sql(
                                f"SELECT * FROM weekly_bars WHERE symbol IN ({','.join(['?']*len(symbols_to_collect))}) AND date BETWEEN ? AND ?",
                                self.engine,
                                params=symbols_to_collect + [start_date, end_date]
                            )
                            # 使用 DataMergeStrategy 合并数据
                            if not existing_weekly_data.empty:
                                combined_weekly_data = DataMergeStrategy.merge_dataframes(
                                    weekly_data,
                                    existing_weekly_data
                                )
                            else:
                                combined_weekly_data = weekly_data
                            
                            try:
                                # 删除原有数据
                                self.engine.execute(
                                    f"DELETE FROM weekly_bars WHERE symbol IN ({','.join(['?']*len(symbols_to_collect))}) AND date BETWEEN ? AND ?",
                                    symbols_to_collect + [start_date, end_date]
                                )
                                # 保存合并后的数据
                                combined_weekly_data.to_sql('weekly_bars', self.engine, if_exists='append', index=False, method='multi', chunksize=5000)
                                self.logger.info(f"成功保存{len(combined_weekly_data)}条周线数据")
                            except Exception as e:
                                self.logger.error(f"保存周线数据失败: {str(e)}")
            
            # 根据配置初始化指数数据
            if config.get('init_index_data', True):
                # 从数据库获取所有 type = market 的指标
                try:
                    market_index_query = "SELECT DISTINCT symbol FROM index_basic_info WHERE type = 'market'"
                    market_indices = pd.read_sql(market_index_query, self.engine)
                    index_symbols = market_indices['symbol'].tolist() if not market_indices.empty else []
                    
                    # 如果数据库中没有市场指数，则使用默认配置
                    if not index_symbols:
                        index_symbols = config.get('index_list', ['000001', '000300', '399001', '399006'])
                        
                    self.logger.info(f"开始初始化指数数据，指数数量：{len(index_symbols)}")
                except Exception as e:
                    self.logger.error(f"获取市场指数列表失败: {str(e)}")
                    # 使用默认配置
                    index_symbols = config.get('index_list', ['000001', '000300', '399001', '399006'])
                    self.logger.info(f"使用默认指数列表，指数数量：{len(index_symbols)}")
                try:
                    index_data = collector.batch_collect_index_data(index_symbols, start_date, end_date)
                    if index_data is not None and not index_data.empty:
                        try:
                            # 获取已有数据
                            existing_index_data = pd.read_sql(
                                f"SELECT * FROM index_daily_data WHERE symbol IN ({','.join(['?']*len(index_symbols))}) AND date BETWEEN ? AND ?",
                                self.engine,
                                params=index_symbols + [start_date, end_date]
                            )

                            # 使用 DataMergeStrategy 合并数据
                            if not existing_index_data.empty:
                                combined_index_data = DataMergeStrategy.merge_dataframes(
                                index_data,
                                existing_index_data if not existing_index_data.empty else None
                            )
                            else:
                                combined_index_data = index_data
                            
                            # 删除原有数据
                            self.engine.execute(
                                f"DELETE FROM index_daily_data WHERE symbol IN ({','.join(['?']*len(index_symbols))}) AND date BETWEEN ? AND ?",
                                index_symbols + [start_date, end_date]
                            )
                            # 保存合并后的数据
                            combined_index_data.to_sql('index_daily_data', self.engine, if_exists='append', index=False, method='multi', chunksize=5000)
                            self.logger.info(f"成功保存{len(combined_index_data)}条指数数据")
                        except Exception as e:
                            self.logger.error(f"保存指数数据失败: {str(e)}")
                    else:
                        self.logger.warning("未能获取到任何指数数据，跳过指数数据初始化")
                except Exception as e:
                    self.logger.error(f"采集指数数据失败: {str(e)}")
                    # 指数数据初始化失败不应该影响整个初始化过程
            
            self.logger.info("数据初始化完成")
            return True
            
        except Exception as e:
            self.logger.error(f"数据初始化失败: {str(e)}")
            return False