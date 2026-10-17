import subprocess
import sys
import os

raw_path = "member3_streaming/reports/raw/PC_pytest_full.txt"
os.makedirs("member3_streaming/reports/raw", exist_ok=True)

cmd = [sys.executable, "-m", "pytest", "-v", "--durations=0", "member3_streaming/tests"]
print(f"Running: {' '.join(cmd)}")

proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
lines = []
with open(raw_path, "w", encoding="utf-8") as f:
    for line in proc.stdout:
        print(line, end="", flush=True)
        f.write(line)
        lines.append(line)

proc.wait()
print(f"\nPytest exited with return code: {proc.returncode}")
sys.exit(proc.returncode)
