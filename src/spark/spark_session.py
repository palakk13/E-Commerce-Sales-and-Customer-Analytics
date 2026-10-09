"""
SparkSession Factory and Configuration Module.

Provides managed, cross-platform PySpark sessions with optimized settings
for local multi-core distributed processing and Windows socket compatibility.
"""

import os
import sys
from typing import Optional

from pyspark.sql import SparkSession

from config.logging_config import setup_logger
from config.pipeline_config import config

logger = setup_logger("spark_session")


def get_spark_session(app_name: Optional[str] = None) -> SparkSession:
    """
    Constructs or retrieves an active SparkSession with appropriate driver
    bindings, memory allocation, and shuffle partition settings.

    Args:
        app_name: Optional custom application name.

    Returns:
        SparkSession: Configured active Spark session.
    """
    # Ensure Windows Python environment path is explicitly passed to PySpark workers
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    name = app_name or config.spark.app_name

    logger.info("Initializing PySpark session: app_name='%s', master='%s'...", name, config.spark.master)

    spark = (
        SparkSession.builder.appName(name)
        .master(config.spark.master)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", config.spark.driver_memory)
        .config("spark.sql.shuffle.partitions", config.spark.shuffle_partitions)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )

    # Set root log level to WARN to eliminate verbose Spark heartbeat noise
    spark.sparkContext.setLogLevel("WARN")

    logger.info("PySpark session successfully active (Spark Version: %s).", spark.version)
    return spark
