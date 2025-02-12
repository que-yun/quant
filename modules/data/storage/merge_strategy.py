import pandas as pd
from datetime import datetime
from typing import Optional, List
from modules.utils.log_manager import logger

class DataMergeStrategy:
    """数据合并策略类，用于统一处理数据合并的逻辑"""
    
    @staticmethod
    def merge_dataframes(new_data: pd.DataFrame,
                        existing_data: Optional[pd.DataFrame] = None,
                        merge_keys: List[str] = None,
                        exclude_merge_keys: List[str] = None) -> pd.DataFrame:
        """合并新旧数据框
        Args:
            new_data: 新数据
            existing_data: 已存在的数据
            merge_keys: 合并的键列表，默认为 ['symbol', 'date']
            exclude_merge_keys: 在合并时需要排除的键列表
        Returns:
            pd.DataFrame: 合并后的数据框
        """
        try:
            if new_data is None or new_data.empty:
                return existing_data if existing_data is not None else pd.DataFrame()
            
            if existing_data is None or existing_data.empty:
                return new_data
            
            # 设置默认合并键
            if merge_keys is None:
                merge_keys = ['symbol', 'date']
            
            # 确保合并键存在于两个数据框中
            if not all(key in new_data.columns and key in existing_data.columns for key in merge_keys):
                logger.error(f"合并键 {merge_keys} 不存在于数据框中")
                return new_data
            
            # 确保日期列类型一致
            if 'date' in new_data.columns and 'date' in existing_data.columns:
                new_data['date'] = pd.to_datetime(new_data['date'])
                existing_data['date'] = pd.to_datetime(existing_data['date'])
            
            # 移除空条目
            new_data = new_data.dropna(how='all')
            existing_data = existing_data.dropna(how='all')
            
            # 使用merge合并数据
            combined_data = pd.merge(
                new_data,
                existing_data,
                on=merge_keys,
                how='outer',
                suffixes=('_new', '_old')
            )
            
            # 优先使用新数据的字段
            for col in new_data.columns:
                if col not in merge_keys and (exclude_merge_keys is None or col not in exclude_merge_keys):
                    if f'{col}_new' in combined_data.columns:
                        # 使用新数据覆盖旧数据，避免空值合并警告
                        mask = combined_data[f'{col}_new'].notna()
                        combined_data.loc[mask, col] = combined_data.loc[mask, f'{col}_new']
                        combined_data.loc[~mask, col] = combined_data.loc[~mask, f'{col}_old']
                        combined_data = combined_data.drop([f'{col}_new', f'{col}_old'], axis=1)
            
            # 确保数据完整性
            combined_data = combined_data.sort_values(merge_keys)
            combined_data = combined_data.drop_duplicates(subset=merge_keys, keep='first')
            
            # 优化数据类型
            combined_data = combined_data.infer_objects()
            
            return combined_data
            
        except Exception as e:
            logger.error(f"合并数据框失败: {str(e)}")
            return new_data
    
    @staticmethod
    def prepare_data_for_merge(df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        """准备数据用于合并
        Args:
            df: 待处理的数据框
            symbol: 股票代码，如果提供则添加symbol列
        Returns:
            pd.DataFrame: 处理后的数据框
        """
        try:
            if df is None or df.empty:
                return pd.DataFrame()
            
            # 复制数据避免修改原始数据
            df = df.copy()
            
            # 如果是Series，转换为DataFrame
            if isinstance(df, pd.Series):
                df = pd.DataFrame([df.to_dict()])
            
            # 添加symbol列
            if symbol is not None and 'symbol' not in df.columns:
                df['symbol'] = symbol
            
            # 处理日期索引
            if df.index.name == 'date' or ('date' not in df.columns and isinstance(df.index, pd.DatetimeIndex)):
                df = df.reset_index()
            
            # 添加更新时间
            df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return df
            
        except Exception as e:
            logger.error(f"准备合并数据失败: {str(e)}")
            return df
    
    @staticmethod
    def ensure_required_fields(df: pd.DataFrame, required_fields: List[str]) -> pd.DataFrame:
        """确保数据框包含所有必要字段，如果缺少则添加默认值
        Args:
            df: 待处理的数据框
            required_fields: 必要字段列表
        Returns:
            pd.DataFrame: 处理后的数据框
        """
        try:
            if df is None or df.empty:
                return pd.DataFrame()
            
            # 复制数据避免修改原始数据
            df = df.copy()
            
            # 检查并添加缺失字段
            for field in required_fields:
                if field not in df.columns:
                    if field == 'update_time':
                        df[field] = pd.Timestamp.now()
                    elif field == 'amount':
                        df[field] = 0.0
                    elif field in ['amplitude', 'pct_change', 'price_change', 'turnover_rate']:
                        df[field] = 0.0
                    else:
                        logger.warning(f"添加缺失字段：{field}")
                        df[field] = None
            
            return df
            
        except Exception as e:
            logger.error(f"确保必要字段存在失败: {str(e)}")
            return df