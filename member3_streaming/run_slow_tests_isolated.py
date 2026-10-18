"""Isolated Test Runner for Member 3 Test Suite.

Runs all fast tests, then executes each @pytest.mark.slow streaming test in its own
isolated pytest process with a 60-second OS socket cooldown pause between runs.
This eliminates Windows Netty ephemeral port exhaustion (TIME_WAIT state) and
prevents BlockManagerMasterEndpoint registration collisions.
"""

import os
import sys
import time
import shutil
import subprocess

BROKER = "127.0.0.1:9092"

FAST_TESTS = [
    "member3_streaming/tests/test_contract.py::test_contract_columns_match_paysim_output_schema",
    "member3_streaming/tests/test_contract.py::test_contract_dlq_codes_match_code_constants",
    "member3_streaming/tests/test_contract.py::test_contract_tier_b_rationale",
    "member3_streaming/tests/test_contract.py::test_dlq_record_schema_fields",
    "member3_streaming/tests/test_fast_unit.py::test_kafka_connectivity",
    "member3_streaming/tests/test_fast_unit.py::test_schema_parsing_valid_and_malformed",
    "member3_streaming/tests/test_fast_unit.py::test_all_dlq_reason_codes_unit",
    "member3_streaming/tests/test_fast_unit.py::test_negative_control_parity_failure",
    "member3_streaming/tests/test_ieee_contract.py::test_ieee_contract_columns_match_spark_schema_and_parquet",
    "member3_streaming/tests/test_ieee_contract.py::test_ieee_contract_dlq_codes_match_code_constants",
    "member3_streaming/tests/test_ieee_contract.py::test_ieee_dlq_record_schema_fields",
    "member3_streaming/tests/test_ieee_streaming.py::test_ieee_parity_negative_control",
]

SLOW_TESTS = [
    "member3_streaming/tests/test_ieee_streaming.py::test_ieee_stateless_parity_small",
    "member3_streaming/tests/test_ieee_streaming.py::test_ieee_stateful_tie_boundary_oracle",
    "member3_streaming/tests/test_ieee_streaming.py::test_ieee_stateful_long_span_parity",
    "member3_streaming/tests/test_ieee_streaming.py::test_ieee_late_event_routing",
    "member3_streaming/tests/test_ieee_streaming.py::test_ieee_all_dlq_codes",
    "member3_streaming/tests/test_ieee_streaming.py::test_ieee_checkpoint_recovery",
    "member3_streaming/tests/test_real_streaming.py::test_real_streaming_parity",
    "member3_streaming/tests/test_real_streaming.py::test_real_streaming_all_dlq_codes",
    "member3_streaming/tests/test_real_streaming.py::test_real_streaming_late_event_routing",
    "member3_streaming/tests/test_real_streaming.py::test_real_streaming_late_event_pending",
    "member3_streaming/tests/test_real_streaming.py::test_real_checkpoint_recovery",
    "member3_streaming/tests/test_real_streaming.py::test_real_checkpoint_plain_restart_failure",
    "member3_streaming/tests/test_real_streaming.py::test_real_streaming_long_span_parity",
]


def kill_lingering():
    subprocess.run(["taskkill", "/F", "/IM", "java.exe"], capture_output=True)


def main():
    root_dir = os.getcwd()
    env = os.environ.copy()
    if "HADOOP_HOME" not in env and os.path.exists(r"C:\hadoop"):
        env["HADOOP_HOME"] = r"C:\hadoop"
    env["PYTHONUNBUFFERED"] = "1"

    raw_dir = os.path.join(root_dir, "member3_streaming", "reports", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    report_file_path = os.path.join(raw_dir, "PDG_slow_tests_isolated.txt")
    log_f = open(report_file_path, "w", encoding="utf-8")

    def log(msg="", end="\n"):
        print(msg, end=end, flush=True)
        log_f.write(msg + end)
        log_f.flush()

    results = []
    log("=" * 80)
    log("MEMBER 3 ISOLATED TEST RUNNER (ALL 25 TESTS)")
    log(f"Total Tests: {len(FAST_TESTS)} Fast, {len(SLOW_TESTS)} Slow (Isolated with 60s cooldown)")
    log("=" * 80 + "\n")

    # 1. Run Fast Tests
    log("--- PART 1: Fast Unit & Contract Tests ---")
    for test_id in FAST_TESTS:
        t0 = time.time()
        res = subprocess.run(
            [sys.executable, "-m", "pytest", "-v", test_id],
            cwd=root_dir,
            capture_output=True,
            text=True,
            env=env,
        )
        duration = time.time() - t0
        status = "PASSED" if res.returncode == 0 else "FAILED"
        results.append((test_id, status, duration, res.stdout, res.stderr))
        log(f"[{status}] {test_id} ({duration:.2f}s)")
        if res.returncode != 0:
            log(f"FAILURE OUTPUT:\n{res.stdout}\n{res.stderr}")

    # 2. Run Slow Tests with 60s cooldown
    log("\n--- PART 2: Slow Streaming Tests (Isolated + 60s Cooldown) ---")
    for idx, test_id in enumerate(SLOW_TESTS, 1):
        kill_lingering()
        time.sleep(3)

        log(f"\n[{idx}/{len(SLOW_TESTS)}] Running slow test: {test_id} ...")
        t0 = time.time()
        res = subprocess.run(
            [sys.executable, "-m", "pytest", "-v", "-s", test_id],
            cwd=root_dir,
            capture_output=True,
            text=True,
            env=env,
        )
        duration = time.time() - t0
        status = "PASSED" if res.returncode == 0 else "FAILED"
        results.append((test_id, status, duration, res.stdout, res.stderr))
        log(f"--> [{status}] {test_id} ({duration:.2f}s)")
        if res.returncode != 0:
            log(f"FAILURE OUTPUT:\n{res.stdout}\n{res.stderr}")

        kill_lingering()
        if idx < len(SLOW_TESTS):
            log(f"Cooling down for 60 seconds (TCP socket TIME_WAIT release)...")
            time.sleep(60)

    # 3. Print Pass/Fail Table
    log("\n" + "=" * 80)
    log("MEMBER 3 TEST SUITE SUMMARY (ISOLATED RUN)")
    log("=" * 80)
    log(f"{'Test ID':<75} | {'Result':<8} | {'Duration'}")
    log("-" * 95)

    passed_count = 0
    failed_count = 0
    for test_id, status, duration, _, _ in results:
        short_id = test_id.split("member3_streaming/tests/")[-1]
        log(f"{short_id:<75} | {status:<8} | {duration:.2f}s")
        if status == "PASSED":
            passed_count += 1
        else:
            failed_count += 1

    log("-" * 95)
    log(f"TOTAL: {len(results)} tests | PASSED: {passed_count} | FAILED: {failed_count}")
    log("=" * 80 + "\n")

    log_f.close()

    # Exit code
    if failed_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()

