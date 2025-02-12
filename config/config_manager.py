import os
import yaml
from typing import Dict, Any

__all__ = ['ConfigManager']

class ConfigManager:
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self._load_config()

    def _load_config(self):
        """加载配置文件"""
        try:
            config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
        except Exception as e:
            raise Exception(f"配置文件加载失败: {str(e)}")

    @property
    def database_path(self) -> str:
        """获取数据库路径"""
        try:
            # 获取项目根目录
            root_dir = os.path.dirname(os.path.dirname(__file__))
            db_name = self._config['data_collection']['storage']['database']
            return os.path.join(root_dir, db_name)
        except KeyError:
            raise Exception("配置文件中缺少数据库路径配置")

    def get_config(self, section: str = None) -> Dict[str, Any]:
        """获取配置信息
        Args:
            section: 配置节点名称，如果为None则返回全部配置
        Returns:
            Dict[str, Any]: 配置信息
        """
        if not self._config:
            self._load_config()

        if section:
            return self._config.get(section, {})
        return self._config