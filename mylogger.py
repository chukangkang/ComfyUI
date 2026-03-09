import contextvars
import logging
import os
import yaml
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import Optional


class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            context_data = threadLocal.get()
            record.requestId = context_data.requestId or "-"
        except LookupError:
            record.requestId = "-"
        return True


@dataclass
class ThreadLocal:
    requestId: str = ''


def set_thread_local(requestId: str = ''):
    """设置请求上下文信息"""
    context_data = ThreadLocal(
            requestId=requestId,
    )
    threadLocal.set(context_data)


threadLocal: contextvars.ContextVar[ThreadLocal] = contextvars.ContextVar(
        'threadLocal', default=ThreadLocal())


def load_config():
    """加载配置文件"""
    with open('/root/ComfyUI/xly-oss-upload-config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def setup_logger(name: str = __name__) -> logging.Logger:
    """
    设置并返回配置好的日志记录器

    Args:
        name: 日志记录器名称

    Returns:
        配置好的Logger实例
    """
    # 加载配置
    config = load_config()
    log_config = config.get('logging', {})

    # 获取日志配置参数
    log_level = log_config.get('level', 'INFO')
    log_format = log_config.get('format',
                                '%(asctime)s - %(name)s - %(levelname)s - %(requestId)s - %(message)s')
    date_format = log_config.get('datefmt', '%Y-%m-%d %H:%M:%S')

    # 创建logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()) if isinstance(log_level,
                                                                      str) else log_level)

    # 避免重复添加处理器
    if logger.handlers:
        return logger

    request_id_filter = RequestIdFilter()
    logger.addFilter(request_id_filter)
    # 创建格式化器
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.addFilter(request_id_filter)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 如果配置了文件输出，则创建文件处理器
    if 'file' in log_config:
        log_file = log_config['file']
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        max_bytes = log_config.get('max_bytes', 10485760)  # 默认10MB
        backup_count = log_config.get('backup_count', 5)  # 默认保留5个备份

        file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(request_id_filter)
        logger.addHandler(file_handler)

    return logger


# 创建全局logger实例
logger = setup_logger(__name__)
