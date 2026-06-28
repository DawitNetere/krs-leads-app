import csv
import io
import threading
import uuid
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from krs_leads import get_bulletin, fetch_company, parse_lead, LEGAL_FORM_FILTER, CSV_FIELDS, ACCOUNTING_PKD_PREFIXES

app = FastAPI()

jobs: dict = {}
jobs_lock = threading.Lock()


class ScrapeRequest(BaseModel):
    start_date: str
    end_date: str
    all_forms: bool = False
    exclude_accounting: bool = True


def run_scrape_job(job_id: str, dates: list[str], legal_form_filter: str | None, excluded_pkd: set):
    all_leads = []
    seen_krs = set()

    try:
        for i, day in enumerate(dates):
            with jobs_lock:
                jobs[job_id]["message"] = f"Fetching bulletin for {day}…"

            krs_numbers = get_bulletin(day)

            if not krs_numbers:
                with jobs_lock:
                    jobs[job_id]["message"] = f"No KRS changes found for {day}."
                continue

            total = len(krs_numbers)
            processed = 0

            with ThreadPoolExecutor(max_workers=10) as pool:
                futures = {pool.submit(fetch_company, k): k for k in krs_numbers}
                for future in as_completed(futures):
                    processed += 1
                    data = future.result()
                    if data:
                        lead = parse_lead(data, day, legal_form_filter)
                        if lead and lead["krs"] not in seen_krs:
                            if not any(lead["pkd"].startswith(p) for p in excluded_pkd):
                                seen_krs.add(lead["krs"])
                                all_leads.append(lead)

                    day_pct = processed / total
                    overall_pct = (i + day_pct) / len(dates)
                    with jobs_lock:
                        jobs[job_id]["progress_pct"] = round(overall_pct * 100)
                        jobs[job_id]["leads_found"] = len(all_leads)
                        jobs[job_id]["records_checked"] = i * total + processed
                        jobs[job_id]["message"] = f"Checking {day} ({processed}/{total})…"

        with jobs_lock:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["results"] = all_leads
            jobs[job_id]["leads_found"] = len(all_leads)
            jobs[job_id]["progress_pct"] = 100
            jobs[job_id]["message"] = f"Done! Found {len(all_leads)} leads across {len(dates)} day(s)."

    except Exception as exc:
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["message"] = f"Error: {exc}"


@app.post("/scrape")
def start_scrape(req: ScrapeRequest):
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(req.end_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    if start > end:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date.")

    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    legal_form_filter = None if req.all_forms else LEGAL_FORM_FILTER
    excluded_pkd = ACCOUNTING_PKD_PREFIXES if req.exclude_accounting else set()
    job_id = uuid.uuid4().hex

    with jobs_lock:
        jobs[job_id] = {
            "status": "running",
            "message": "Starting…",
            "leads_found": 0,
            "records_checked": 0,
            "progress_pct": 0,
            "results": [],
        }

    thread = threading.Thread(
        target=run_scrape_job, args=(job_id, dates, legal_form_filter, excluded_pkd), daemon=True
    )
    thread.start()

    return {"job_id": job_id}


@app.get("/status/{job_id}")
def get_status(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {
        "status": job["status"],
        "message": job["message"],
        "leads_found": job["leads_found"],
        "progress_pct": job["progress_pct"],
    }


@app.get("/preview/{job_id}")
def get_preview(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job["status"] != "done":
        raise HTTPException(status_code=400, detail="Job not finished yet.")
    return {"leads": job["results"][:20], "total": len(job["results"])}


@app.get("/download/{job_id}")
def download_csv(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job["status"] != "done":
        raise HTTPException(status_code=400, detail="Job not finished yet.")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS)
    writer.writeheader()
    writer.writerows(job["results"])
    buf.seek(0)

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=krs_leads.csv"},
    )


app.mount("/", StaticFiles(directory="static", html=True), name="static")
