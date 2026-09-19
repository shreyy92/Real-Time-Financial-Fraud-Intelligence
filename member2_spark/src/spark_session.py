"""
Spark Session Provider for Member 2 — Financial Fraud Intelligence Platform.

Configures and instantiates a reusable, optimized PySpark session.
Designed for both local development/testing and cluster deployments.
"""

from typing import Dict, Any, Optional
import os
import sys
from pyspark.sql import SparkSession

# Ensure PySpark workers on Windows invoke the active Python interpreter
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    bin_path = r"C:\hadoop\bin"
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")


def create_spark_session(
    app_name: str = "FinancialFraud-Member2",
    master: str = "local[*]",
    config_overrides: Optional[Dict[str, Any]] = None,
) -> SparkSession:
    """
    Create or retrieve a pre-configured PySpark SparkSession.

    Args:
        app_name: Name of the Spark application. Default is 'FinancialFraud-Member2'.
        master: Spark master URL. Default is 'local[*]'.
        config_overrides: Optional key-value dictionary to override Spark configuration.

    Returns:
        SparkSession: The initialized or retrieved SparkSession instance.
    """
    builder = SparkSession.builder.appName(app_name)

    # Set master only if not running inside a cluster manager (e.g., YARN/K8s)
    if master and "SPARK_MASTER" not in os.environ and "MASTER" not in os.environ:
        builder = builder.master(master)

    # Production & laptop-friendly defaults based on Member 1 audit recommendations
    default_configs = {
        "spark.sql.shuffle.partitions": "16",
        "spark.driver.memory": "4g",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.ui.enabled": "false",  # Avoid local port conflicts during automated testing
        "spark.sql.session.timeZone": "UTC",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.adaptive.enabled": "true",
    }

    # Apply defaults
    for key, value in default_configs.items():
        builder = builder.config(key, str(value))

    # Apply user-specified overrides
    if config_overrides:
        for key, value in config_overrides.items():
            builder = builder.config(key, str(value))

    return builder.getOrCreate()


def get_spark_session(
    app_name: str = "FinancialFraud-Member2",
    master: str = "local[*]",
    config_overrides: Optional[Dict[str, Any]] = None,
) -> SparkSession:
    """Convenience alias for create_spark_session."""
    return create_spark_session(app_name, master, config_overrides)
