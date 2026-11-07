import argparse
import json
import os
import time

from dotenv import load_dotenv

load_dotenv()

from kiyohara_screener import run_screening_process_pool as run_screening

parser = argparse.ArgumentParser()
parser.add_argument("--workers", type=int, default=4)
parser.add_argument("--days", type=int, default=7)
parser.add_argument("--verbose", action="store_true")
args = parser.parse_args()

api_key = os.environ.get("EDINET_API_KEY") or ""
if not api_key:
    print("ERROR: EDINET_API_KEY not set in environment or .env")
    raise SystemExit(1)

start = time.time()
print(f"=== BENCH START workers={args.workers} days={args.days} ===")
results = run_screening(
    api_key=api_key,
    days_back=args.days,
    min_ratio=1.0,
    max_cap_oku=500,
    min_cap_oku=20,
    min_pe=None,
    max_pe=None,
    output_csv=os.devnull,
    filter_sec_code=None,
    verbose=args.verbose,
    progress_callback=None,
    num_workers=args.workers,
)
end = time.time()

summary = {
    "workers": args.workers,
    "days": args.days,
    "duration_sec": round(end - start, 2),
    "results_count": len(results or []),
}
print("=== BENCH END ===")
print(json.dumps(summary, ensure_ascii=False))
