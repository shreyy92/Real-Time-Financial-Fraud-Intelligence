"""Clean, precise reproduction script for R3.a (Stateless) and R3.b (HDFS-backed stateful)."""

import os
import sys
import time
import json
import shutil
import subprocess
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

sys.stdout.reconfigure(line_buffering=True)
BROKER = "localhost:9092"

if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

def reset_topic(topic):
    admin = KafkaAdminClient(bootstrap_servers=[BROKER], request_timeout_ms=5000)
    existing = admin.list_topics()
    if topic in existing:
        admin.delete_topics([topic])
        time.sleep(2)
    admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    admin.close()

def run_r3a():
    print("=" * 80, flush=True)
    print("RUNNING R3.a: Minimal Stateless Kafka Passthrough (No State Store)", flush=True)
    print("=" * 80, flush=True)

    in_topic = "r3a_in"
    out_topic = "r3a_out"
    chk_dir = os.path.abspath("member3_streaming/checkpoints/r3a_chk")

    reset_topic(in_topic)
    reset_topic(out_topic)
    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)

    # Produce 1,000 rows
    producer = KafkaProducer(bootstrap_servers=[BROKER])
    for i in range(1000):
        producer.send(in_topic, key=str(i % 2).encode("utf-8"), value=json.dumps({"id": i, "val": f"data_{i}"}).encode("utf-8"))
    producer.flush()
    producer.close()
    print("Produced 1,000 messages to", in_topic, flush=True)

    # Worker script: partition 0 writes immediately; partition 1 sleeps 15 seconds
    worker_code = f"""
import sys, os, time
if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\\hadoop"
    os.environ["PATH"] = r"C:\\hadoop\\bin" + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = (
    SparkSession.builder.master("local[2]")
    .appName("R3aStatelessWorker")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
try:
    spark._jvm.java.lang.System.load(r"C:\\hadoop\\bin\\hadoop.dll")
except Exception:
    pass

@F.udf(returnType=StringType())
def delay_partition(key, val):
    if str(key) == "1":
        time.sleep(15)  # partition 1 sleeps 15s, allowing partition 0 to write to Kafka first
    return val

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "{BROKER}")
    .option("subscribe", "{in_topic}")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "500")
    .load()
)

# Re-partition into 2 partitions based on key ("0" vs "1")
q = (
    df.repartition(2, F.col("key"))
    .select(
        F.col("key").cast("string").alias("key"),
        delay_partition(F.col("key"), F.col("value")).alias("value")
    )
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "{BROKER}")
    .option("topic", "{out_topic}")
    .option("checkpointLocation", r"{chk_dir}")
    .start()
)
q.awaitTermination()
"""
    worker_file = os.path.abspath("member3_streaming/scratch_r3a_worker.py")
    with open(worker_file, "w") as f:
        f.write(worker_code)

    log1 = os.path.abspath("member3_streaming/scratch_r3a_p1.log")
    with open(log1, "w") as lf:
        proc1 = subprocess.Popen([sys.executable, worker_file], stdout=lf, stderr=subprocess.STDOUT)
    print(f"Stateless worker 1 started (PID {proc1.pid}). Waiting for mid-batch kill condition...", flush=True)

    offsets_dir = os.path.join(chk_dir, "offsets")
    commits_dir = os.path.join(chk_dir, "commits")

    kill_count = 0
    t0 = time.time()
    while time.time() - t0 < 60:
        time.sleep(0.3)
        consumer = KafkaConsumer(bootstrap_servers=[BROKER], consumer_timeout_ms=300)
        tp = TopicPartition(out_topic, 0)
        curr = consumer.end_offsets([tp]).get(tp, 0)
        consumer.close()

        if os.path.exists(offsets_dir):
            offs = [f for f in os.listdir(offsets_dir) if f.isdigit()]
            comms = [f for f in os.listdir(commits_dir) if f.isdigit()] if os.path.exists(commits_dir) else []
            # When batch 0 has committed (500 rows) and batch 1 is mid-stream (curr > 550 and curr % 500 != 0)
            if curr > 550 and curr % 500 != 0 and len(offs) > len(comms):
                print(f"Mid-batch condition met in batch 1! output_count={curr} (curr % 500 = {curr % 500} != 0), offsets={offs}, commits={comms}", flush=True)
                subprocess.run(f"taskkill /F /T /PID {proc1.pid}", shell=True, capture_output=True)
                time.sleep(2)
                kill_count = curr
                break

    if proc1.poll() is None:
        subprocess.run(f"taskkill /F /T /PID {proc1.pid}", shell=True, capture_output=True)
        time.sleep(2)

    consumer = KafkaConsumer(bootstrap_servers=[BROKER], consumer_timeout_ms=1000)
    tp = TopicPartition(out_topic, 0)
    final_kill_count = consumer.end_offsets([tp]).get(tp, 0)
    consumer.close()

    offs = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()]) if os.path.exists(offsets_dir) else []
    comms = sorted([int(f) for f in os.listdir(commits_dir) if f.isdigit()]) if os.path.exists(commits_dir) else []

    print(f"\nR3.a Status at Kill Time:", flush=True)
    print(f"  Output count at kill: {final_kill_count}", flush=True)
    print(f"  Arithmetic: {final_kill_count} % 500 = {final_kill_count % 500} (strictly mid-batch: {final_kill_count % 500 != 0})", flush=True)
    print(f"  Offsets written on disk: {offs}", flush=True)
    print(f"  Commits written on disk: {comms}", flush=True)
    print(f"  Uncommitted batch present? {len(offs) > len(comms)}", flush=True)

    # Attempt restart WITHOUT orphan-offset workaround
    print("\nRestarting stateless consumer WITHOUT orphan-offset workaround...", flush=True)
    log2 = os.path.abspath("member3_streaming/scratch_r3a_p2.log")
    with open(log2, "w") as lf:
        proc2 = subprocess.Popen([sys.executable, worker_file], stdout=lf, stderr=subprocess.STDOUT)

    time.sleep(15)
    ret2 = proc2.poll()
    print(f"Restart return code after 15s: {ret2}", flush=True)

    with open(log2, "r", encoding="utf-8", errors="ignore") as lf:
        log2_content = lf.read()

    npe_found = "NullPointerException" in log2_content and "KafkaMicroBatchStream" in log2_content
    print(f"\nDid NPE reproduce on stateless query? {npe_found}", flush=True)
    print("\n--- RAW TRACE FROM STATELESS RESTART ---", flush=True)
    matched_lines = []
    capture = False
    for line in log2_content.splitlines():
        if "ERROR MicroBatchExecution" in line or "java.lang.NullPointerException" in line:
            capture = True
        if capture:
            matched_lines.append(line)
            if len(matched_lines) >= 20:
                break
    print("\n".join(matched_lines), flush=True)

    if proc2.poll() is None:
        subprocess.run(f"taskkill /F /T /PID {proc2.pid}", shell=True, capture_output=True)
        time.sleep(2)

    return npe_found, final_kill_count, matched_lines

def run_r3b():
    print("\n" + "=" * 80, flush=True)
    print("RUNNING R3.b: Fresh HDFSBackedStateStoreProvider Stateful Query", flush=True)
    print("=" * 80, flush=True)

    in_topic = "financial_transactions_paysim"
    out_topic = "r3b_hdfs_out"
    dlq_topic = "r3b_hdfs_dlq"
    chk_dir = os.path.abspath("member3_streaming/checkpoints/r3b_hdfs_chk")

    reset_topic(in_topic)
    reset_topic(out_topic)
    reset_topic(dlq_topic)
    if os.path.exists(chk_dir):
        shutil.rmtree(chk_dir, ignore_errors=True)

    # Produce 2,000 PaySim rows
    print("Producing 2,000 PaySim rows...", flush=True)
    res = subprocess.run([
        sys.executable, "member3_streaming/producer_paysim.py",
        "--limit", "2000", "--delay", "0"
    ], capture_output=True, text=True)
    assert res.returncode == 0, f"Producer failed: {res.stderr}"

    # Start consumer configured with HDFSBackedStateStoreProvider and trickle sleep
    env = os.environ.copy()
    env["PAYSIM_TRICKLE_SLEEP"] = "0.005"
    env["SPARK_STATE_STORE_PROVIDER"] = "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"

    consumer_cmd = [
        sys.executable, "-m", "member3_streaming.consumer_paysim",
        "--bootstrap-servers", BROKER,
        "--input-topic", in_topic,
        "--output-topic", out_topic,
        "--dlq-topic", dlq_topic,
        "--checkpoint-location", chk_dir,
        "--max-offsets-per-trigger", "500",
    ]

    log1 = os.path.abspath("member3_streaming/scratch_r3b_p1.log")
    with open(log1, "w") as lf:
        proc1 = subprocess.Popen(consumer_cmd, env=env, stdout=lf, stderr=subprocess.STDOUT)
    print(f"HDFS stateful worker 1 started (PID {proc1.pid}). Waiting for mid-batch kill condition...", flush=True)

    offsets_dir = os.path.join(chk_dir, "features", "offsets")
    commits_dir = os.path.join(chk_dir, "features", "commits")

    kill_count = 0
    t0 = time.time()
    while time.time() - t0 < 60:
        time.sleep(0.3)
        consumer = KafkaConsumer(bootstrap_servers=[BROKER], consumer_timeout_ms=300)
        tp = TopicPartition(out_topic, 0)
        curr = consumer.end_offsets([tp]).get(tp, 0)
        consumer.close()

        if os.path.exists(offsets_dir):
            offs = [f for f in os.listdir(offsets_dir) if f.isdigit()]
            comms = [f for f in os.listdir(commits_dir) if f.isdigit()] if os.path.exists(commits_dir) else []
            if curr > 550 and curr % 500 != 0 and len(offs) > len(comms):
                print(f"Mid-batch condition met in batch 1! output_count={curr} (curr % 500 = {curr % 500} != 0), offsets={offs}, commits={comms}", flush=True)
                subprocess.run(f"taskkill /F /T /PID {proc1.pid}", shell=True, capture_output=True)
                time.sleep(2)
                kill_count = curr
                break

    if proc1.poll() is None:
        subprocess.run(f"taskkill /F /T /PID {proc1.pid}", shell=True, capture_output=True)
        time.sleep(2)

    consumer = KafkaConsumer(bootstrap_servers=[BROKER], consumer_timeout_ms=1000)
    tp = TopicPartition(out_topic, 0)
    final_kill_count = consumer.end_offsets([tp]).get(tp, 0)
    consumer.close()

    offs = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()]) if os.path.exists(offsets_dir) else []
    comms = sorted([int(f) for f in os.listdir(commits_dir) if f.isdigit()]) if os.path.exists(commits_dir) else []

    # Read offsets/0 line that names HDFSBackedStateStoreProvider
    off0_file = os.path.join(offsets_dir, "0")
    provider_line = ""
    if os.path.exists(off0_file):
        with open(off0_file, "r") as f:
            for l in f:
                if "providerClass" in l:
                    provider_line = l.strip()
                    break

    print(f"\nR3.b Status at Kill Time:", flush=True)
    print(f"  Output count at kill: {final_kill_count}", flush=True)
    print(f"  Arithmetic: {final_kill_count} % 500 = {final_kill_count % 500} (strictly mid-batch: {final_kill_count % 500 != 0})", flush=True)
    print(f"  Offsets written on disk: {offs}", flush=True)
    print(f"  Commits written on disk: {comms}", flush=True)
    print(f"  Uncommitted batch present? {len(offs) > len(comms)}", flush=True)
    print(f"  offsets/0 provider line:\n    {provider_line}", flush=True)

    # Attempt restart WITHOUT orphan-offset workaround
    print("\nRestarting HDFS consumer WITHOUT orphan-offset workaround...", flush=True)
    log2 = os.path.abspath("member3_streaming/scratch_r3b_p2.log")
    with open(log2, "w") as lf:
        proc2 = subprocess.Popen(consumer_cmd, env=env, stdout=lf, stderr=subprocess.STDOUT)

    time.sleep(15)
    ret2 = proc2.poll()
    print(f"Restart return code after 15s: {ret2}", flush=True)

    with open(log2, "r", encoding="utf-8", errors="ignore") as lf:
        log2_content = lf.read()

    npe_found = "NullPointerException" in log2_content and "KafkaMicroBatchStream" in log2_content
    print(f"\nDid NPE reproduce on fresh HDFS query? {npe_found}", flush=True)
    print("\n--- RAW TRACE FROM HDFS RESTART ---", flush=True)
    matched_lines = []
    capture = False
    for line in log2_content.splitlines():
        if "ERROR MicroBatchExecution" in line or "java.lang.NullPointerException" in line:
            capture = True
        if capture:
            matched_lines.append(line)
            if len(matched_lines) >= 20:
                break
    print("\n".join(matched_lines), flush=True)

    if proc2.poll() is None:
        subprocess.run(f"taskkill /F /T /PID {proc2.pid}", shell=True, capture_output=True)
        time.sleep(2)

if __name__ == "__main__":
    run_r3a()
    run_r3b()
