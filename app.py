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
from regon import enrich_leads

app = FastAPI()

jobs: dict = {}
jobs_lock = threading.Lock()


class ScrapeRequest(BaseModel):
    start_date: str
    end_date: str
    all_forms: bool = False
    exclude_accounting: bool = True


def run_scrape_job(job_id: str, dates: list[str], legal_form_filter: str | None, excluded_pkd: set):
    try:
        # ── Step 1: fetch all bulletins in parallel ───────────────────────────
        with jobs_lock:
            jobs[job_id]["message"] = f"Fetching bulletins for {len(dates)} day(s)…"

        day_krs: dict[str, list] = {}
        with ThreadPoolExecutor(max_workers=len(dates)) as pool:
            bulletin_futures = {pool.submit(get_bulletin, day): day for day in dates}
            for future in as_completed(bulletin_futures):
                day = bulletin_futures[future]
                day_krs[day] = list(dict.fromkeys(future.result()))

        # ── Step 2: deduplicate KRS across all days before fetching ──────────
        # Each (krs, day) pair: keep first day a KRS is seen so parse_lead
        # can match registration date correctly.
        seen_krs: set = set()
        tasks: list[tuple[str, str]] = []
        for day in dates:
            for krs in day_krs.get(day, []):
                if krs not in seen_krs:
                    seen_krs.add(krs)
                    tasks.append((krs, day))

        total = len(tasks)
        with jobs_lock:
            jobs[job_id]["message"] = f"Processing {total} unique records across {len(dates)} day(s)…"

        # ── Step 3: fetch all companies in one large pool ────────────────────
        all_leads = []
        seen_result_krs: set = set()
        processed = 0

        with ThreadPoolExecutor(max_workers=100) as pool:
            futures = {pool.submit(fetch_company, krs): (krs, day) for krs, day in tasks}
            for future in as_completed(futures):
                krs, day = futures[future]
                processed += 1
                data = future.result()
                if data:
                    lead = parse_lead(data, day, legal_form_filter)
                    if lead and lead["krs"] not in seen_result_krs:
                        if not any(lead["pkd"].startswith(p) for p in excluded_pkd):
                            seen_result_krs.add(lead["krs"])
                            all_leads.append(lead)

                with jobs_lock:
                    jobs[job_id]["progress_pct"] = round(processed / total * 90 / 100)
                    jobs[job_id]["leads_found"] = len(all_leads)
                    jobs[job_id]["message"] = f"Checked {processed}/{total} records — {len(all_leads)} candidates…"

        # ── Step 4: enrich with REGON (phone + email) ────────────────────────
        with jobs_lock:
            jobs[job_id]["message"] = f"Enriching {len(all_leads)} leads via REGON API…"

        def regon_progress(done, total_r):
            with jobs_lock:
                jobs[job_id]["progress_pct"] = 90 + round(done / total_r * 10)
                jobs[job_id]["message"] = f"REGON enrichment: {done}/{total_r}…"

        all_leads = enrich_leads(all_leads, progress_cb=regon_progress)

        # Keep only leads that have an email (from KRS or REGON)
        all_leads = [l for l in all_leads if l.get("email", "").strip()]

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
