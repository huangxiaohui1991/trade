"""
统一日志工具 - 提供结构化日志输出到 stdout 和 data/cron.log
"""
import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

# 获取项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CRON_LOG_PATH = os.path.join(PROJECT_ROOT, "data", "cron.log")

# 日志格式
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
CRON_LOG_FORMAT = "%(asctime)s [CRON] %(name)s: %(message)s"

# 日志级别配置，默认为 INFO
LOG_LEVEL = logging.INFO


def _ensure_cron_log_dir():
    """确保 data/cron.log 目录存在"""
    log_dir = os.path.dirname(CRON_LOG_PATH)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)


def _get_file_handler(log_path: str, formatter: logging.Formatter) -> logging.FileHandler:
    """创建文件处理器，自动创建目录"""
    _ensure_cron_log_dir()
    handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8"
    )
    handler.setFormatter(formatter)
    return handler


def _get_stream_handler(formatter: logging.Formatter) -> logging.StreamHandler:
    """创建控制台处理器"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    return handler


def get_logger(name: str) -> logging.Logger:
    """
    获取普通模块日志记录器

    Args:
        name: 模块名称，用于日志标识

    Returns:
        logging.Logger 实例，同时输出到 stdout 和 data/cron.log
    """
    logger = logging.getLogger(name)

    # 避免重复添加 handler
    if logger.handlers:
        return logger

    logger.setLevel(LOG_LEVEL)

    # 普通日志格式
    formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)

    # 添加控制台处理器
    logger.addHandler(_get_stream_handler(formatter))

    # 添加文件处理器（写入 cron.log）
    logger.addHandler(_get_file_handler(CRON_LOG_PATH, formatter))

    return logger


def get_cron_logger() -> logging.Logger:
    """
    获取 cron 任务专用日志记录器

    Returns:
        logging.Logger 实例，专门用于 cron 任务，写入 data/cron.log
    """
    logger = logging.getLogger("cron")

    # 避免重复添加 handler
    if logger.handlers:
        return logger

    logger.setLevel(LOG_LEVEL)

    # Cron 日志格式（带 [CRON] 标记）
    formatter = logging.Formatter(CRON_LOG_FORMAT, LOG_DATE_FORMAT)

    # 只添加文件处理器到 cron.log
    logger.addHandler(_get_file_handler(CRON_LOG_PATH, formatter))

    return logger


def set_log_level(level: int) -> None:
    """
    设置全局日志级别

    Args:
        level: 日志级别，如 logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR
    """
    global LOG_LEVEL
    LOG_LEVEL = level
    # 更新所有已存在的 logger 的级别
    logging.getLogger("cron").setLevel(level)


def set_console_logging(enabled: bool) -> None:
    """
    动态开启/关闭控制台日志，文件日志保持不变。
    """
    target_level = LOG_LEVEL if enabled else logging.CRITICAL + 1
    manager = logging.Logger.manager
    for logger_obj in manager.loggerDict.values():
        if not isinstance(logger_obj, logging.Logger):
            continue
        for handler in logger_obj.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler):
                handler.setLevel(target_level)


if __name__ == "__main__":
    # 测试代码
    logger = get_logger("test_module")
    logger.info("This is an info message from test_module")
    logger.warning("This is a warning message")
    logger.error("This is an error message")

    cron_logger = get_cron_logger()
    cron_logger.info("This is a cron info message")
    cron_logger.warning("This is a cron warning message")
