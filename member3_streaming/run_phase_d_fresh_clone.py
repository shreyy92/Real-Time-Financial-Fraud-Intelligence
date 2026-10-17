"""Fresh-clone verification script for Phase D.

Clones origin/member3-streaming to an isolated temporary directory, runs:
1. Import check for consumer_paysim and consumer_ieee.
2. Full pytest test suite across all 25 tests.
Saves raw logs to member3_streaming/reports/raw/PD_fresh_clone_pytest.txt.
"""
import os
import sys
import shutil
import tempfile
import subprocess

REPO_URL = "https://github.com/shreyy92/Real-Time-Financial-Fraud-Intelligence.git"
BRANCH = "member3-streaming"
RAW_OUT_PATH = os.path.abspath("member3_streaming/reports/raw/PD_fresh_clone_pytest.txt")

def main():
    tmp_dir = tempfile.mkdtemp(prefix="fresh_clone_phase_d_")
    log_lines = []

    def log(msg):
        print(msg, flush=True)
        log_lines.append(str(msg))

    try:
        log("=====================================================================")
        log(f"PHASE D: FRESH-CLONE VERIFICATION ON BRANCH {BRANCH}")
        log("=====================================================================")
        log(f"Cloning {REPO_URL} (branch: {BRANCH}) to {tmp_dir}...")
        res_clone = subprocess.run(
            ["git", "clone", "--branch", BRANCH, "--single-branch", REPO_URL, tmp_dir],
            capture_output=True,
            text=True
        )
        log("GIT CLONE STDOUT:\n" + res_clone.stdout)
        log("GIT CLONE STDERR:\n" + res_clone.stderr)
        assert res_clone.returncode == 0, f"Git clone failed with exit code {res_clone.returncode}"

        # 1. Fresh-clone import check
        log("\n--- Task 1: Fresh-Clone Clean Import Check ---")
        import_test_code = (
            "import sys, os\n"
            "root = os.getcwd()\n"
            "sys.path.insert(0, root)\n"
            "sys.path.insert(0, os.path.join(root, 'member2_spark'))\n"
            "import member3_streaming.consumer_paysim as cp\n"
            "import member3_streaming.consumer_ieee as ci\n"
            "print('SUCCESS: Both consumer_paysim and consumer_ieee imported cleanly without missing dependencies!')\n"
        )
        import_file = os.path.join(tmp_dir, "test_fresh_import.py")
        with open(import_file, "w", encoding="utf-8") as f:
            f.write(import_test_code)

        env = os.environ.copy()
        if "HADOOP_HOME" not in env and os.path.exists(r"C:\hadoop"):
            env["HADOOP_HOME"] = r"C:\hadoop"

        res_import = subprocess.run([sys.executable, import_file], cwd=tmp_dir, capture_output=True, text=True, env=env)
        log("IMPORT TEST STDOUT:\n" + res_import.stdout)
        log("IMPORT TEST STDERR:\n" + res_import.stderr)
        assert res_import.returncode == 0, f"Import check failed with exit code {res_import.returncode}"

        # 2. Fresh-clone full test suite run
        log("\n--- Task 2: Fresh-Clone Full Pytest Suite Run (25 tests) ---")
        # Link project dataset to fresh clone for parity evaluation
        target_data = os.path.join(tmp_dir, "data")
        source_data = os.path.abspath("data")
        if os.path.exists(source_data) and not os.path.exists(target_data):
            log(f"Linking project dataset from {source_data} to {target_data}...")
            subprocess.run(["cmd.exe", "/c", "mklink", "/J", target_data, source_data], capture_output=True)
        cmd_pytest = [sys.executable, "-m", "pytest", "-v", "--durations=0", "member3_streaming/tests"]
        log(f"Running command: {' '.join(cmd_pytest)} in {tmp_dir}...")

        proc_pytest = subprocess.Popen(
            cmd_pytest,
            cwd=tmp_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env
        )

        for line in proc_pytest.stdout:
            print(line, end="", flush=True)
            log_lines.append(line.rstrip())

        proc_pytest.wait()
        log(f"\nPytest exited with return code: {proc_pytest.returncode}")
        assert proc_pytest.returncode == 0, f"Pytest suite in fresh clone failed with code {proc_pytest.returncode}"
        log("\n=====================================================================")
        log("ALL FRESH-CLONE VERIFICATIONS PASSED SUCCESSFULLY!")
        log("=====================================================================")

    finally:
        try:
            if os.path.exists(target_data):
                subprocess.run(["cmd.exe", "/c", "rmdir", target_data], capture_output=True)
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception as e:
            log(f"Cleanup warning: {e}")

        os.makedirs(os.path.dirname(RAW_OUT_PATH), exist_ok=True)
        with open(RAW_OUT_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(log_lines) + "\n")
        print(f"\nRaw output written to {RAW_OUT_PATH}")

if __name__ == "__main__":
    main()
