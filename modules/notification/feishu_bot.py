import requests
from loguru import logger

class FeishuBot:
    def __init__(self, webhook_url: str):
        """初始化飞书机器人
        Args:
            webhook_url: 飞书机器人的webhook地址
        """
        self.webhook_url = webhook_url
        self.headers = {
            'Content-Type': 'application/json'
        }
    
    def send_text(self, content: str) -> bool:
        """发送纯文本消息
        Args:
            content: 消息内容
        Returns:
            bool: 是否发送成功
        """
        try:
            data = {
                'msg_type': 'text',
                'content': {
                    'text': content
                }
            }
            response = requests.post(self.webhook_url, json=data, headers=self.headers)
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.success(f'消息发送成功: {content}')
                    return True
                else:
                    logger.error(f'消息发送失败: {result.get("msg")}')
            else:
                logger.error(f'请求失败，状态码: {response.status_code}')
            return False
        except Exception as e:
            logger.error(f'发送消息异常: {str(e)}')
            return False
    
    def send_rich_text(self, title: str, content: list) -> bool:
        """发送富文本消息
        Args:
            title: 消息标题
            content: 消息内容列表，每个元素为一个段落，支持文本和链接
        Returns:
            bool: 是否发送成功
        """
        try:
            data = {
                'msg_type': 'post',
                'content': {
                    'post': {
                        'zh_cn': {
                            'title': title,
                            'content': content
                        }
                    }
                }
            }
            response = requests.post(self.webhook_url, json=data, headers=self.headers)
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.success(f'富文本消息发送成功: {title}')
                    return True
                else:
                    logger.error(f'富文本消息发送失败: {result.get("msg")}')
            else:
                logger.error(f'请求失败，状态码: {response.status_code}')
            return False
        except Exception as e:
            logger.error(f'发送富文本消息异常: {str(e)}')
            return False
    
    def send_markdown(self, title: str, content: str) -> bool:
        """发送markdown消息
        Args:
            title: 消息标题
            content: markdown格式的消息内容
        Returns:
            bool: 是否发送成功
        """
        try:
            data = {
                'msg_type': 'interactive',
                'card': {
                    'config': {
                        'wide_screen_mode': True
                    },
                    'header': {
                        'title': {
                            'tag': 'plain_text',
                            'content': title
                        }
                    },
                    'elements': [
                        {
                            'tag': 'markdown',
                            'content': content
                        }
                    ]
                }
            }
            response = requests.post(self.webhook_url, json=data, headers=self.headers)
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.success(f'Markdown消息发送成功: {title}')
                    return True
                else:
                    logger.error(f'Markdown消息发送失败: {result.get("msg")}')
            else:
                logger.error(f'请求失败，状态码: {response.status_code}')
            return False
        except Exception as e:
            logger.error(f'发送Markdown消息异常: {str(e)}')
            return False