# KRS Lead Scraper — Web App PRD

## What we're building
A simple local web app that scrapes newly registered Polish companies from the free KRS government API and lets the user download results as a CSV. No database, no login, no deployment needed — runs locally in the browser.

I'm attaching `krs_leads.py` which already contains all the scraping logic. Your job is to wrap it in a web interface.

---

## Tech Stack
- **Backend:** Python + FastAPI
- **Frontend:** Single HTML file (vanilla JS, no frameworks)
- **Styling:** Minimal, clean — use Tailwind CDN
- **No database required**

---

## Pages / UI

### Single page app — one screen only

**Header**
- App name: "KRS Lead Scraper"
- Subtitle: "Newly registered Polish companies"

**Form (top of page)**
- Start date picker (default: today)
- End date picker (default: today)
- Dropdown: Legal form filter
  - "Sp. z o.o. only" (default)
  - "All legal forms"
- Big "Scrape Leads" button

**Progress section (shown after clicking Scrape)**
- Status text e.g. "Fetching bulletin for 2026-05-08…"
- Progress bar (updates live via polling)
- Stats: "X records checked / Y leads found"

**Results section (shown when done)**
- Summary: "Found 312 leads across 3 days"
- Table preview: first 20 rows, columns: nazwa, nip, email, miejscowosc, pkd_opis, data_rejestracji
- "Download CSV" button — downloads the full results

**Error handling**
- If the API is unreachable, show a friendly error message
- If no leads found, say so clearly

---

## Backend API Endpoints

### `POST /scrape`
Starts a scrape job. Returns a `job_id`.

Request body:
```json
{
  "start_date": "2026-05-08",
  "end_date": "2026-05-10",
  "all_forms": false
}
```

Response:
```json
{ "job_id": "abc123" }
```

### `GET /status/{job_id}`
Poll this every 2 seconds from the frontend.

Response:
```json
{
  "status": "running",         // "running" | "done" | "error"
  "message": "Checking 2026-05-08 (1200/5081)...",
  "leads_found": 87,
  "progress_pct": 24
}
```

### `GET /download/{job_id}`
Returns the CSV file as a download attachment.

---

## Scraper Integration

Import and call the functions from the attached `krs_leads.py` directly. Specifically:
- `get_bulletin(day)` — gets KRS numbers for a date
- `fetch_company(krs)` — fetches one company record
- `parse_lead(data, target_date, legal_form_filter)` — extracts fields

Run each scrape job in a **background thread** so the API stays responsive during scraping. Store job state (status, results) in a simple in-memory dict keyed by `job_id`.

---

## CSV Output Format

Same columns as the attached script:
`nazwa, nip, email, telefon, www, pkd, pkd_opis, data_rejestracji, miejscowosc, kod, ulica, nr_domu, krs`

---

## How to run (include in README)
```bash
pip install fastapi uvicorn requests
uvicorn app:app --reload
# Open http://localhost:8000
```

---

## Out of scope
- User accounts / auth
- Saving history between sessions
- Deployment to cloud
- Mobile responsiveness (desktop only is fine)
