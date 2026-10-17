"""Fresh-clone import check per Guidelines Section 0.6."""
import os
import sys
import shutil
import tempfile
import subprocess

def main():
    tmp_dir = tempfile.mkdtemp(prefix="fresh_clone_")
    raw_path = os.path.abspath("member3_streaming/reports/raw/PA_hygiene_clone.txt")
    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(str(msg))

    try:
        log("=== Fresh-Clone Import Check ===")
        log(f"Cloning branch member3-streaming to {tmp_dir}...")
        res = subprocess.run(
            ["git", "clone", "--branch", "member3-streaming", "--single-branch", "https://github.com/shreyy92/Real-Time-Financial-Fraud-Intelligence.git", tmp_dir],
            capture_output=True,
            text=True
        )
        log("GIT CLONE STDOUT:")
        log(res.stdout)
        log("GIT CLONE STDERR:")
        log(res.stderr)
        assert res.returncode == 0, f"Git clone failed with code {res.returncode}"

        py_code = (
            "import sys, os\n"
            "repo_root = os.getcwd()\n"
            "sys.path.insert(0, repo_root)\n"
            "sys.path.insert(0, os.path.join(repo_root, 'member2_spark'))\n"
            "import member3_streaming.consumer_paysim\n"
            "import member3_streaming.consumer_ieee\n"
            "print('SUCCESS: Both consumer_paysim and consumer_ieee imported cleanly!')\n"
        )
        test_script = os.path.join(tmp_dir, "test_fresh_import.py")
        with open(test_script, "w", encoding="utf-8") as f:
            f.write(py_code)

        log(f"Running import check in {tmp_dir}...")
        env = os.environ.copy()
        if "HADOOP_HOME" not in env and os.path.exists(r"C:\hadoop"):
            env["HADOOP_HOME"] = r"C:\hadoop"
        res_import = subprocess.run([sys.executable, test_script], cwd=tmp_dir, capture_output=True, text=True, env=env)
        log("IMPORT TEST STDOUT:")
        log(res_import.stdout)
        log("IMPORT TEST STDERR:")
        log(res_import.stderr)
        log(f"IMPORT TEST RETURNCODE: {res_import.returncode}")
        assert res_import.returncode == 0, "Fresh clone import check failed!"
        log("=== Fresh-Clone Import Check PASSED ===")
    finally:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception as e:
            log(f"Cleanup warning: {e}")
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write("\n".join(log_lines) + "\n")

if __name__ == "__main__":
    main()
