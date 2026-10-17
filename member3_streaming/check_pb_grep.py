import os
import re

patterns = {
    'SPARK- (speculative ticket IDs)': re.compile(r'SPARK-\d+', re.IGNORECASE),
    'sub-second': re.compile(r'sub-second', re.IGNORECASE),
    'Kafka connector bug': re.compile(r'Kafka connector bug', re.IGNORECASE),
    'all standard forward pushes': re.compile(r'all standard forward pushes', re.IGNORECASE),
}

results = {k: [] for k in patterns}

base_dir = 'member3_streaming'
for root, dirs, files in os.walk(base_dir):
    if any(x in root for x in ['_c09_wip', '__pycache__', 'checkpoints', 'reports/raw', 'reports\\raw', '.git']):
        continue
    for f in files:
        if f == 'check_pb_grep.py':
            continue
        if f.endswith(('.py', '.md', '.txt')):
            path = os.path.join(root, f)
            with open(path, 'r', encoding='utf-8', errors='ignore') as fp:
                for idx, line in enumerate(fp, 1):
                    for name, pat in patterns.items():
                        if pat.search(line):
                            # Note if it is a historical correction note explicitly acknowledging removal
                            results[name].append(f'{path}:{idx}: {line.strip()}')

out_lines = []
out_lines.append('=== Phase B Forbidden Phrases Grep Check in member3_streaming ===')
for name, hits in results.items():
    out_lines.append(f'Pattern: "{name}" -> {len(hits)} occurrences')
    for h in hits:
        out_lines.append(f'  {h}')

out_text = '\n'.join(out_lines)
print(out_text)
with open('member3_streaming/reports/raw/PB_grep_check.txt', 'w', encoding='utf-8') as fp:
    fp.write(out_text + '\n')
