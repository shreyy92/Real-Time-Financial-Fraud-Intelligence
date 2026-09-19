"""
Unit tests for SparkSession provider.
"""

import pytest
from src.spark_session import create_spark_session, get_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session(
        app_name="Test-SparkSession",
        master="local[1]",
        config_overrides={
            "spark.sql.shuffle.partitions": "2",
            "spark.ui.enabled": "false",
        },
    )
    yield session
    session.stop()


def test_spark_session_creation(spark):
    """Test that SparkSession initializes and reports expected application configuration."""
    assert spark is not None
    assert spark.sparkContext.appName is not None
    assert len(spark.sparkContext.appName) > 0
    assert spark.version is not None

    # Test basic DataFrame operation
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "val"])
    assert df.count() == 2
    assert set(df.columns) == {"id", "val"}


def test_get_spark_session_alias():
    """Test that get_spark_session returns an active session."""
    session = get_spark_session()
    assert session is not None
    assert not session.sparkContext._jsc.sc().isStopped()
