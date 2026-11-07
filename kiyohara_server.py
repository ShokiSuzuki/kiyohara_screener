import io
import json
import os
import threading
import time
import uuid
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta

from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory

# Load .env into environment for local development
load_dotenv()

# Import the screening function and defaults
import pandas as pd
from redis import Redis
from rq import Queue

from kiyohara_screener import (
    DEFAULT_API_KEY,
    DEFAULT_MAX_CAP_OKU,
    DEFAULT_MIN_CAP_OKU,
    run_screening,
)

# RQ / Redis queue
redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
redis_conn = Redis.from_url(redis_url)
queue = Queue("kiyohara", connection=redis_conn)

app = Flask(__name__, static_folder=".", static_url_path="")

# Job store
JOBS: dict = {}


@app.route("/")
def index():
    return send_from_directory(".", "kiyohara_dashboard.html")


def _worker(job_id: str, params: dict):
    job = JOBS[job_id]
    job["status"] = "running"
    job["start_time"] = datetime.now().isoformat()

    job_dir = job["job_dir"]
    os.makedirs(job_dir, exist_ok=True)
    log_path = os.path.join(job_dir, "log.txt")
    partial_path = os.path.join(job_dir, "results.jsonl")
    result_json = os.path.join(job_dir, "results.json")

    # Open log file and partial results file
    with (
        open(log_path, "a", encoding="utf-8") as logf,
        open(partial_path, "a", encoding="utf-8") as partf,
    ):

        def progress_callback(row: dict):
            try:
                partf.write(json.dumps(row, ensure_ascii=False) + "\n")
                partf.flush()
                job["partial_count"] = job.get("partial_count", 0) + 1
            except Exception:
                pass

        try:
            # Wrap log file so writes are flushed immediately
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

            # Redirect stdout/stderr to log file for streaming
            with redirect_stdout(writer), redirect_stderr(writer):
                # Run screening (this will write CSV to outputs/ root)
                results = run_screening(
                    api_key=params["api_key"],
                    days_back=params["days"],
                    min_ratio=params["ratio"],
                    max_cap_oku=params["max_cap"],
                    min_cap_oku=params["min_cap"],
                    min_pe=params.get("min_pe"),
                    max_pe=params.get("max_pe"),
                    output_csv=params["out_csv"],
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

            job["status"] = "finished"
            job["end_time"] = datetime.now().isoformat()
            job["csv"] = params["csv_name"]
            job["result_count"] = len(results or [])
        except Exception as e:
            # Log exception and mark failed
            try:
                logf.write("\n[ERROR] " + str(e) + "\n")
                logf.flush()
            except Exception:
                pass
            job["status"] = "failed"
            job["error"] = str(e)
            job["end_time"] = datetime.now().isoformat()


@app.route("/api/run", methods=["POST"])
def api_run():
    payload = request.get_json(force=True) or {}
    # Use server-side EDINET API key only. Do NOT accept client-supplied keys.
    server_api_key = os.environ.get("EDINET_API_KEY") or DEFAULT_API_KEY
    if not server_api_key:
        return jsonify(
            {"ok": False, "error": "Server EDINET_API_KEY not configured"}
        ), 500
    api_key = server_api_key
    days = int(payload.get("days", 30) or 30)
    ratio = float(payload.get("ratio", 1.0) or 1.0)
    max_cap = float(payload.get("max_cap", DEFAULT_MAX_CAP_OKU) or DEFAULT_MAX_CAP_OKU)
    min_cap = float(payload.get("min_cap", DEFAULT_MIN_CAP_OKU) or DEFAULT_MIN_CAP_OKU)

    min_pe = payload.get("min_pe")
    max_pe = payload.get("max_pe")
    try:
        min_pe = float(min_pe) if min_pe not in (None, "") else None
    except Exception:
        min_pe = None
    try:
        max_pe = float(max_pe) if max_pe not in (None, "") else None
    except Exception:
        max_pe = None

    sec_code = payload.get("sec_code") or None
    verbose = bool(payload.get("verbose", False))

    # Prepare job
    os.makedirs("outputs", exist_ok=True)
    job_id = uuid.uuid4().hex
    job_dir = os.path.join("outputs", "jobs", job_id)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_name = f"kiyohara_results_{timestamp}.csv"
    out_csv = os.path.join("outputs", csv_name)

    JOBS[job_id] = {
        "status": "queued",
        "job_dir": job_dir,
        "csv": None,
        "csv_name": csv_name,
        "start_time": None,
        "end_time": None,
        "partial_count": 0,
    }

    # Ensure job directory exists and write initial queued status so other
    # server processes (Gunicorn workers) can discover the job.
    try:
        os.makedirs(job_dir, exist_ok=True)
        status_file = os.path.join(job_dir, "status.json")
        with open(status_file, "w", encoding="utf-8") as sf:
            json.dump(
                {"status": "queued", "csv": csv_name}, sf, ensure_ascii=False, indent=2
            )
    except Exception:
        pass

    # Allow overriding num_workers via payload or server ENV (useful when running in Docker Compose)
    payload_num_workers = payload.get("num_workers")
    if payload_num_workers not in (None, ""):
        try:
            payload_num_workers = int(payload_num_workers)
        except Exception:
            payload_num_workers = None

    env_num_workers = None
    try:
        env_val = os.environ.get("SCREENING_WORKERS")
        if env_val not in (None, ""):
            env_num_workers = int(env_val)
    except Exception:
        env_num_workers = None

    params = {
        "api_key": api_key,
        "days": days,
        "ratio": ratio,
        "max_cap": max_cap,
        "min_cap": min_cap,
        "min_pe": min_pe,
        "max_pe": max_pe,
        "sec_code": sec_code,
        "verbose": verbose,
        "out_csv": out_csv,
        "csv_name": csv_name,
        "num_workers": payload_num_workers
        if payload_num_workers is not None
        else env_num_workers,
    }

    # Split date range into chunks (5 days each) for parallel processing
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    # Split into 5-day chunks
    chunk_days = 5
    chunks = []
    cur = start_date
    while cur < end_date:
        chunk_end = min(cur + timedelta(days=chunk_days), end_date)
        chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cur = chunk_end

    if len(chunks) <= 1:
        # Single chunk or very small range: use regular job
        try:
            rq_job = queue.enqueue("tasks.run_job", job_id, params, job_timeout=3600)
            JOBS[job_id]["rq_id"] = rq_job.get_id()
        except Exception as e:
            t = threading.Thread(target=_worker, args=(job_id, params), daemon=True)
            JOBS[job_id]["thread"] = t
            t.start()
    else:
        # Multiple chunks: use parallel chunk processing with master job
        try:
            chunk_job_ids = []
            for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks):
                chunk_rq_job = queue.enqueue(
                    "tasks.run_job_chunk",
                    job_id,
                    f"chunk_{chunk_idx}",
                    params,
                    chunk_start,
                    chunk_end,
                    job_timeout=3600,
                )
                chunk_job_ids.append((chunk_idx, chunk_rq_job.get_id()))

            # Enqueue master job to wait for all chunks and merge results
            master_rq_job = queue.enqueue(
                "tasks.run_job_master", job_id, params, len(chunks), job_timeout=3600
            )
            JOBS[job_id]["rq_id"] = master_rq_job.get_id()
            JOBS[job_id]["chunk_job_ids"] = chunk_job_ids
        except Exception as e:
            # Fallback: single-threaded job
            try:
                rq_job = queue.enqueue(
                    "tasks.run_job", job_id, params, job_timeout=3600
                )
                JOBS[job_id]["rq_id"] = rq_job.get_id()
            except Exception:
                t = threading.Thread(target=_worker, args=(job_id, params), daemon=True)
                JOBS[job_id]["thread"] = t
                t.start()

    return jsonify({"ok": True, "job_id": job_id})


@app.route("/api/status/<job_id>", methods=["GET"])
def api_status(job_id):
    job = JOBS.get(job_id)
    job_dir = os.path.join("outputs", "jobs", job_id)

    # If the job isn't in the in-memory store (e.g. different Gunicorn worker),
    # try to discover it from the file-based status written by the worker.
    status_file = os.path.join(job_dir, "status.json")
    file_status = None
    if not job:
        if os.path.exists(status_file):
            try:
                with open(status_file, "r", encoding="utf-8") as sf:
                    file_status = json.load(sf)
                job = {
                    "status": file_status.get("status"),
                    "job_dir": job_dir,
                    "csv": file_status.get("csv"),
                    "csv_name": file_status.get("csv"),
                    "start_time": file_status.get("start_time"),
                    "end_time": file_status.get("end_time"),
                    "partial_count": 0,
                }
            except Exception:
                job = None
    if not job:
        return jsonify({"ok": False, "error": "job not found"}), 404
    log_path = os.path.join(job_dir, "log.txt")
    partial_path = os.path.join(job_dir, "results.jsonl")

    logs = ""
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                data = f.read()
                logs = data[-20000:]
        except Exception:
            logs = ""

    partial = []
    if os.path.exists(partial_path):
        try:
            with open(partial_path, "r", encoding="utf-8") as f:
                lines = f.readlines()[-200:]
                for ln in lines:
                    try:
                        partial.append(json.loads(ln))
                    except Exception:
                        pass
        except Exception:
            partial = []

    # If we haven't loaded file_status earlier, attempt to read it now.
    if file_status is None and os.path.exists(os.path.join(job_dir, "status.json")):
        try:
            with open(
                os.path.join(job_dir, "status.json"), "r", encoding="utf-8"
            ) as sf:
                file_status = json.load(sf)
        except Exception:
            file_status = None

    # Prefer file-based status if available
    status = file_status.get("status") if file_status else job.get("status")
    if file_status:
        # merge some fields from file_status into job view
        job_view = {**job, **file_status}
    else:
        job_view = job

    return jsonify(
        {
            "ok": True,
            "job_id": job_id,
            "status": status,
            "logs": logs,
            "partial": partial,
            "csv": job_view.get("csv") or job.get("csv_name")
            if status == "finished"
            else None,
            "partial_count": job.get("partial_count", 0),
            "meta": {
                k: job_view.get(k)
                for k in ("start_time", "end_time", "result_count", "error")
                if job_view.get(k) is not None
            },
        }
    )


@app.route("/api/results/<job_id>", methods=["GET"])
def api_results(job_id):
    job = JOBS.get(job_id)
    if job:
        job_dir = job["job_dir"]
    else:
        # Fallback to file-based job directory
        job_dir = os.path.join("outputs", "jobs", job_id)
    result_json = os.path.join(job_dir, "results.json")
    if os.path.exists(result_json):
        try:
            with open(result_json, "r", encoding="utf-8") as f:
                data = json.load(f)
            return jsonify({"ok": True, "results": data})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": False, "error": "results not ready"}), 404


@app.route("/outputs/<path:filename>")
def serve_output(filename):
    return send_from_directory("outputs", filename, as_attachment=True)


if __name__ == "__main__":
    # Run with: python kiyohara_server.py
    app.run(host="0.0.0.0", port=5000, debug=True)
