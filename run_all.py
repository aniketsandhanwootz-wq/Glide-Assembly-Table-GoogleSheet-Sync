import os
import sys
import subprocess
from pathlib import Path

JOBS = [
    "jobs/01_dashboard_updates_append_only.py",
    "jobs/02_project_glide_to_sheet.py",
    "jobs/03_ccp_two_way.py",
    "jobs/04_suppliers_sheet_to_glide.py",
]

def main():
    root = Path(__file__).resolve().parent
    env = os.environ.copy()

    # Run sequentially by default (predictable logs/quota)
    failed = 0
    for job in JOBS:
        p = root / job
        print(f"\n=== RUN: {job} ===")
        r = subprocess.run([sys.executable, str(p)], env=env)
        if r.returncode != 0:
            failed += 1
            print(f"!! FAILED: {job} (code={r.returncode})")

    if failed:
        sys.exit(1)

if __name__ == "__main__":
    main()