import functools
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger
from typing import Any, Dict, List, Optional, Tuple, Union
from threading import Lock
from collections import OrderedDict

class CacheStorage:
    """缓存存储类，用于统一管理数据缓存"""
    
    def __init__(self, max_size: int = 1000):
        """初始化缓存存储
        Args:
            max_size: 最大缓存条目数，默认1000
        """
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._cache_times: Dict[str, datetime] = {}
        self._lock = Lock()
        self._max_size = max_size
    
    def get(self, key: str) -> Tuple[bool, Optional[Any]]:
        """获取缓存数据
        Args:
            key: 缓存键
        Returns:
            tuple: (是否命中, 缓存数据)
        """
        with self._lock:
            if key not in self._cache:
                return False, None
            
            cache_time = self._cache_times.get(key)
            if cache_time is None:
                return False, None
            
            if self.is_expired(key):
                self.clear(key)
                return False, None
            
            # 更新访问顺序
            value = self._cache.pop(key)
            self._cache[key] = value
            return True, value
    
    def set(self, key: str, value: Any, expire_seconds: int) -> None:
        """设置缓存数据
        Args:
            key: 缓存键
            value: 缓存值
            expire_seconds: 过期时间（秒）
        """
        with self._lock:
            # 检查容量限制
            if len(self._cache) >= self._max_size and key not in self._cache:
                # 移除最早访问的项
                oldest_key = next(iter(self._cache))
                self.clear(oldest_key)
                logger.debug(f"缓存已满，移除最早项: {oldest_key}")
            
            # 更新缓存
            if key in self._cache:
                self._cache.pop(key)
            self._cache[key] = value
            self._cache_times[key] = datetime.now() + timedelta(seconds=expire_seconds)
    
    def is_expired(self, key: str) -> bool:
        """检查缓存是否过期
        Args:
            key: 缓存键
        Returns:
            bool: 是否过期
        """
        if key not in self._cache_times:
            return True
        
        expire_time = self._cache_times[key]
        return datetime.now() > expire_time
    
    def clear(self, key: Optional[str] = None) -> None:
        """清除缓存
        Args:
            key: 缓存键，如果为None则清除所有缓存
        """
        with self._lock:
            if key is None:
                self._cache.clear()
                self._cache_times.clear()
                logger.info("已清除所有缓存")
            else:
                self._cache.pop(key, None)
                self._cache_times.pop(key, None)
                logger.debug(f"已清除缓存: {key}")
    
    def clear_by_prefix(self, prefix: str) -> None:
        """根据前缀清除缓存
        Args:
            prefix: 缓存键前缀
        """
        with self._lock:
            keys_to_clear = [k for k in self._cache.keys() if k.startswith(prefix)]
            for key in keys_to_clear:
                self.clear(key)
            if keys_to_clear:
                logger.info(f"已清除前缀为 {prefix} 的 {len(keys_to_clear)} 个缓存项")
    
    def get_info(self) -> Dict[str, Union[int, List[Dict[str, Union[str, int]]]]]:
        """获取缓存信息统计
        Returns:
            dict: 缓存信息统计
        """
        with self._lock:
            cache_info = {
                'total_items': len(self._cache),
                'items': []
            }
            
            for key in list(self._cache.keys()):
                try:
                    expire_time = self._cache_times.get(key)
                    if expire_time:
                        remaining = (expire_time - datetime.now()).total_seconds()
                        value = self._cache.get(key)
                        size = 'N/A'
                        if value is not None and isinstance(value, pd.DataFrame):
                            try:
                                size = len(value)
                            except Exception:
                                size = 'N/A'
                        
                        cache_info['items'].append({
                            'key': key,
                            'expire_in': f"{remaining:.1f}秒" if remaining > 0 else "已过期",
                            'size': size
                        })
                except Exception as e:
                    logger.warning(f"获取缓存项信息失败: {key}, 错误: {str(e)}")
                    continue
            
            return cache_info

# 创建全局缓存实例
_cache_storage = CacheStorage()

def cache_data(expire_seconds: int = 300, cache_df_only: bool = True, max_size: Optional[int] = None):
    """数据缓存装饰器
    Args:
        expire_seconds: 缓存过期时间（秒），默认5分钟
        cache_df_only: 是否只缓存DataFrame类型的结果，默认True
    Returns:
        function: 装饰器函数
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存key
            try:
                # 处理参数中的股票代码，移除前缀
                processed_args = []
                for arg in args:
                    if isinstance(arg, str) and (arg.startswith('sh') or arg.startswith('sz')):
                        processed_args.append(arg[2:])  # 移除sh或sz前缀
                    else:
                        processed_args.append(arg)
                
                processed_kwargs = {}
                for key, value in kwargs.items():
                    if isinstance(value, str) and (value.startswith('sh') or value.startswith('sz')):
                        processed_kwargs[key] = value[2:]  # 移除sh或sz前缀
                    else:
                        processed_kwargs[key] = value
                
                cache_key = f"{func.__name__}:{hash(str(processed_args))}:{hash(str(processed_kwargs))}"
            finally:
                pass
            # 检查是否有缓存且未过期
            hit, cached_data = _cache_storage.get(cache_key)
            if hit:
                logger.debug(f"命中缓存: {cache_key}")
                return cached_data
            
            # 调用原始函数获取数据
            result = func(*args, **kwargs)
            
            # 根据配置决定是否缓存结果
            if not cache_df_only or isinstance(result, pd.DataFrame):
                _cache_storage.set(cache_key, result, expire_seconds)
                logger.debug(f"更新缓存: {cache_key}, 过期时间: {expire_seconds}秒")
            
            return result
        return wrapper
    return decorator

def clear_cache(func_name: Optional[str] = None) -> None:
    """清除指定函数的缓存
    Args:
        func_name: 函数名，如果为None则清除所有缓存
    """
    if func_name is None:
        _cache_storage.clear()
        logger.info("已清除所有缓存")
    else:
        _cache_storage.clear_by_prefix(func_name + ':')
        logger.info(f"已清除{func_name}的缓存")

def get_cache_info() -> Dict[str, Union[int, List[Dict[str, Union[str, int]]]]]:
    """获取缓存信息
    Returns:
        dict: 缓存信息统计
    """
    return _cache_storage.get_info()