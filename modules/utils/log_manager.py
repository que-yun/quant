import os
import sys
from loguru import logger

class LogManager:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LogManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._initialized = True
            self._setup_logger()

    def _setup_logger(self):
        """配置日志记录器"""
        try:
            # 获取项目根目录
            root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            log_dir = os.path.join(root_dir, 'logs')
            
            # 创建日志目录
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            # 移除默认的处理器
            logger.remove()

            # 添加控制台输出处理器
            logger.add(
                sys.stderr,
                format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
                level="DEBUG"
            )

            # 添加文件输出处理器
            logger.add(
                os.path.join(log_dir, "quant_{time:YYYY-MM-DD}.log"),
                rotation="00:00",  # 每天零点创建新文件
                retention="30 days",  # 保留30天的日志
                format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
                level="DEBUG",
                encoding="utf-8"
            )

            logger.debug("日志管理器初始化成功")

        except Exception as e:
            print(f"日志管理器初始化失败: {str(e)}")
            sys.exit(1)

    @staticmethod
    def get_logger():
        """获取logger实例"""
        return logger

# 创建全局日志管理器实例
log_manager = LogManager()