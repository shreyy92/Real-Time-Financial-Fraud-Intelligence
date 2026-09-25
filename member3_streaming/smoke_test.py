import os
import sys

# Configure HADOOP_HOME for Windows before initializing Spark
if sys.platform == "win32":
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
        os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

def run_smoke_consumer():
    print("Starting PySpark Structured Streaming consumer for 'financial_transactions_paysim'...")
    spark = SparkSession.builder \
        .appName("PaySimSmokeTest") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "financial_transactions_paysim") \
        .option("startingOffsets", "earliest") \
        .load()

    query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp") \
        .writeStream \
        .format("console") \
        .option("truncate", "false") \
        .outputMode("append") \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()
    print("Smoke test stream processing completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_smoke_consumer()
