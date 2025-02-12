from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from loguru import logger
from config.config_manager import ConfigManager

class DatabasePool:
    """数据库连接池，管理和复用数据库连接"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabasePool, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.logger = logger
            self._initialize_pool()
            DatabasePool._initialized = True
    
    def _initialize_pool(self):
        """初始化数据库连接池"""
        try:
            config = ConfigManager()
            self.db_path = config.database_path
            
            # 配置连接池
            self.engine = create_engine(
                f"sqlite:///{self.db_path}",
                poolclass=QueuePool,
                pool_size=20,          # 连接池大小
                max_overflow=10,       # 超过pool_size后最多可创建的连接数
                pool_timeout=30,       # 获取连接的超时时间
                pool_recycle=1800      # 连接重置时间(秒)
            )
            self.logger.debug("数据库连接池初始化完成")
            
        except Exception as e:
            self.logger.error(f"数据库连接池初始化失败: {str(e)}")
            raise
    
    def get_engine(self) -> Optional[Engine]:
        """获取数据库引擎实例
        Returns:
            Optional[Engine]: SQLAlchemy引擎实例
        """
        return self.engine
    
    def dispose(self):
        """释放连接池中的所有连接"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
            self.logger.debug("数据库连接池已释放")