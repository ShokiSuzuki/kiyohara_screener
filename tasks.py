import glob
import io
import json
import os
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime

from kiyohara_screener import run_screening, run_screening_for_date_range

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


def _write_status(job_dir: str, data: dict):
    try:
        with open(os.path.join(job_dir, "status.json"), "w", encoding="utf-8") as sf:
            json.dump(data, sf, ensure_ascii=False, indent=2)
    except Exception:
        pass


def run_job_chunk(
    job_id: str, chunk_id: str, params: dict, start_date: str, end_date: str
) -> bool:
    """
    RQ task: Run screening for a specific date range (chunk).
    Results are saved as results.jsonl (no final CSV).
    """
    job_dir = os.path.join("outputs", "jobs", job_id, "chunks", chunk_id)
    os.makedirs(job_dir, exist_ok=True)
    log_path = os.path.join(job_dir, "log.txt")
    result_jsonl = os.path.join(job_dir, "results.jsonl")

    _write_status(
        job_dir, {"status": "running", "start_time": datetime.now().isoformat()}
    )

    with (
        open(log_path, "a", encoding="utf-8") as logf,
        open(result_jsonl, "a", encoding="utf-8") as resf,
    ):

        def progress_callback(row: dict):
            try:
                resf.write(json.dumps(row, ensure_ascii=False) + "\n")
                resf.flush()
            except Exception:
                pass

        try:

            class FlushingWriter:
                def __init__(self, f):
                    self.f = f

                def write(self, s):
                    try:
                        self.f.write(s)
                        self.f.flush()
                    except Exception:
                        pass

                def flush(self):
                    try:
                        self.f.flush()
                    except Exception:
                        pass

            writer = FlushingWriter(logf)

            with redirect_stdout(writer), redirect_stderr(writer):
                results = run_screening_for_date_range(
                    api_key=params.get("api_key"),
                    start_date_str=start_date,
                    end_date_str=end_date,
                    min_ratio=params.get("ratio"),
                    max_cap_oku=params.get("max_cap"),
                    min_cap_oku=params.get("min_cap"),
                    min_pe=params.get("min_pe"),
                    max_pe=params.get("max_pe"),
                    output_csv=None,  # No CSV for chunks
                    filter_sec_code=params.get("sec_code"),
                    verbose=params.get("verbose", False),
                    progress_callback=progress_callback,
                    num_workers=params.get("num_workers"),
                )

            _write_status(
                job_dir,
                {
                    "status": "finished",
                    "end_time": datetime.now().isoformat(),
                    "result_count": len(results or []),
                },
            )
            return True

        except Exception as e:
            try:
                logf.write("\n[ERROR] " + str(e) + "\n")
                logf.flush()
            except Exception:
                pass
            _write_status(
                job_dir,
                {
                    "status": "failed",
                    "error": str(e),
                    "end_time": datetime.now().isoformat(),
                },
            )
            return False


def run_job(job_id: str, params: dict) -> bool:
    """RQ task: run screening and write logs/partial results under outputs/jobs/<job_id>/"""
    job_dir = os.path.join("outputs", "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    log_path = os.path.join(job_dir, "log.txt")
    partial_path = os.path.join(job_dir, "results.jsonl")
    result_json = os.path.join(job_dir, "results.json")

    _write_status(
        job_dir, {"status": "running", "start_time": datetime.now().isoformat()}
    )

    with (
        open(log_path, "a", encoding="utf-8") as logf,
        open(partial_path, "a", encoding="utf-8") as partf,
    ):

        def progress_callback(row: dict):
            try:
                partf.write(json.dumps(row, ensure_ascii=False) + "\n")
                partf.flush()
            except Exception:
                pass

        try:

            class FlushingWriter:
                def __init__(self, f):
                    self.f = f

                def write(self, s):
                    try:
                        self.f.write(s)
                        self.f.flush()
                    except Exception:
                        pass

                def flush(self):
                    try:
                        self.f.flush()
                    except Exception:
                        pass

            writer = FlushingWriter(logf)

            with redirect_stdout(writer), redirect_stderr(writer):
                results = run_screening(
                    api_key=params.get("api_key"),
                    days_back=params.get("days"),
                    min_ratio=params.get("ratio"),
                    max_cap_oku=params.get("max_cap"),
                    min_cap_oku=params.get("min_cap"),
                    min_pe=params.get("min_pe"),
                    max_pe=params.get("max_pe"),
                    output_csv=params.get("out_csv"),
                    filter_sec_code=params.get("sec_code"),
                    verbose=params.get("verbose", False),
                    progress_callback=progress_callback,
                )

            # Save final results JSON
            try:
                with open(result_json, "w", encoding="utf-8") as jf:
                    json.dump(results or [], jf, ensure_ascii=False, indent=2)
            except Exception:
                pass

            _write_status(
                job_dir,
                {
                    "status": "finished",
                    "end_time": datetime.now().isoformat(),
                    "csv": params.get("csv_name"),
                    "result_count": len(results or []),
                },
            )
            return True

        except Exception as e:
            try:
                logf.write("\n[ERROR] " + str(e) + "\n")
                logf.flush()
            except Exception:
                pass
            _write_status(
                job_dir,
                {
                    "status": "failed",
                    "error": str(e),
                    "end_time": datetime.now().isoformat(),
                },
            )
            return False


def run_job_master(job_id: str, params: dict, num_chunks: int) -> bool:
    """
    RQ task: Master job that waits for all chunk jobs to complete,
    then merges results and outputs final CSV.
    """
    job_dir = os.path.join("outputs", "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    log_path = os.path.join(job_dir, "log.txt")
    result_json = os.path.join(job_dir, "results.json")

    _write_status(
        job_dir, {"status": "running", "start_time": datetime.now().isoformat()}
    )

    with open(log_path, "a", encoding="utf-8") as logf:
        try:
            logf.write(f"\n[MASTER] Waiting for {num_chunks} chunks to complete...\n")
            logf.flush()

            # Wait for all chunk results
            all_results = []
            max_wait_seconds = 3600  # 1 hour
            start_time = time.time()

            while time.time() - start_time < max_wait_seconds:
                completed_chunks = 0
                for chunk_idx in range(num_chunks):
                    chunk_dir = os.path.join(job_dir, "chunks", f"chunk_{chunk_idx}")
                    status_file = os.path.join(chunk_dir, "status.json")
                    result_jsonl = os.path.join(chunk_dir, "results.jsonl")

                    if os.path.exists(status_file):
                        try:
                            with open(status_file, "r", encoding="utf-8") as sf:
                                status = json.load(sf)
                                if status.get("status") in ("finished", "failed"):
                                    completed_chunks += 1
                                    # Load results from chunk
                                    if status.get(
                                        "status"
                                    ) == "finished" and os.path.exists(result_jsonl):
                                        try:
                                            with open(
                                                result_jsonl, "r", encoding="utf-8"
                                            ) as rf:
                                                for line in rf:
                                                    try:
                                                        row = json.loads(line)
                                                        all_results.append(row)
                                                    except Exception:
                                                        pass
                                        except Exception:
                                            pass
                        except Exception:
                            pass

                if completed_chunks >= num_chunks:
                    logf.write(
                        f"[MASTER] All {num_chunks} chunks completed. Merging results...\n"
                    )
                    logf.flush()
                    break

                time.sleep(5)  # Check every 5 seconds

            # Remove duplicates (keep latest by submitDateTime)
            seen = {}
            for row in all_results:
                code = row.get("銘柄コード")
                if code not in seen or row.get("提出日", "") > seen[code].get(
                    "提出日", ""
                ):
                    seen[code] = row
            final_results = list(seen.values())

            # Sort by ratio descending
            final_results.sort(
                key=lambda x: x.get("ネットキャッシュ比率", 0), reverse=True
            )

            # Save merged JSON
            try:
                with open(result_json, "w", encoding="utf-8") as jf:
                    json.dump(final_results, jf, ensure_ascii=False, indent=2)
            except Exception:
                pass

            # Output CSV
            out_csv = params.get("out_csv")
            if out_csv and final_results:
                try:
                    if HAS_PANDAS:
                        df = pd.DataFrame(final_results)
                        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
                    else:
                        import csv

                        with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
                            writer = csv.DictWriter(
                                f, fieldnames=final_results[0].keys()
                            )
                            writer.writeheader()
                            writer.writerows(final_results)
                    logf.write(
                        f"[MASTER] Final CSV saved to {out_csv} ({len(final_results)} rows)\n"
                    )
                    logf.flush()
                except Exception as e:
                    logf.write(f"[MASTER] CSV save error: {str(e)}\n")
                    logf.flush()

            _write_status(
                job_dir,
                {
                    "status": "finished",
                    "end_time": datetime.now().isoformat(),
                    "csv": params.get("csv_name"),
                    "result_count": len(final_results),
                },
            )
            return True

        except Exception as e:
            try:
                logf.write(f"\n[ERROR] Master job error: {str(e)}\n")
                logf.flush()
            except Exception:
                pass
            _write_status(
                job_dir,
                {
                    "status": "failed",
                    "error": str(e),
                    "end_time": datetime.now().isoformat(),
                },
            )
            return False
