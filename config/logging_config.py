"""
Production Logging Configuration Module.

Provides standard logging formatters, rotating file handlers,
and console streams across all pipeline modules.
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logger(
    name: str = "ecommerce_pipeline",
    log_dir: str | Path | None = None,
    log_file: str = "pipeline.log",
    log_level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
) -> logging.Logger:
    """
    Initializes and returns a configured logger with console and rotating file handlers.

    Args:
        name: Name of the logger instance.
        log_dir: Target directory for log files. Defaults to project 'logs/' directory.
        log_file: Name of the log file.
        log_level: Logging severity level (e.g., logging.INFO, logging.DEBUG).
        max_bytes: Maximum size per log file before rotation.
        backup_count: Number of rotated backup files to retain.

    Returns:
        logging.Logger: Fully configured logger instance.
    """
    logger = logging.getLogger(name)

    # Avoid duplicate handlers if already configured
    if logger.handlers:
        return logger

    logger.setLevel(log_level)
    logger.propagate = False

    # Determine log directory
    if log_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
        log_dir = base_dir / "logs"
    else:
        log_dir = Path(log_dir)

    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    # Formatter: Timestamp | Level | Logger | Function:Line | Message
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | [%(name)s] | %(module)s.%(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1. Console Stream Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. Rotating File Handler
    try:
        file_handler = RotatingFileHandler(
            filename=str(log_path),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.warning("Failed to initialize file handler at %s: %s", log_path, e)

    return logger


# Default root pipeline logger
logger = setup_logger()
