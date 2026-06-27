#!/usr/bin/env python3
"""
Poland KRS Lead Scraper
=======================
Pulls newly registered sp. z o.o. companies from the free, public KRS API
and saves them as a CSV — same format as the "leads poland.csv" sample.

How it works:
  1. Calls the KRS Bulletin API to get all KRS numbers that changed on a date
  2. Fetches the full company record for each number (parallel, 10 workers)
  3. Keeps only brand-new registrations (registered on that exact date)
  4. Optionally filters by legal form (default: sp. z o.o. only)
  5. Writes a CSV with: nazwa, nip, email, telefon, www, pkd, pkd_opis,
     data_rejestracji, miejscowosc, kod, ulica, nr_domu, krs

Usage:
  python krs_leads.py                          # today
  python krs_leads.py 2026-05-08               # specific date
  python krs_leads.py 2026-05-08 2026-05-22    # date range
  python krs_leads.py 2026-05-08 --all-forms   # all legal forms, not just sp. z o.o.

Requirements:
  pip install requests
"""

import csv
import sys
import time
import requests
from datetime import date, timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_URL       = "https://api-krs.ms.gov.pl/api"
MAX_WORKERS    = 10       # parallel HTTP workers
RETRY_ATTEMPTS = 3        # retries per failed request
RETRY_DELAY    = 2        # seconds between retries
REQUEST_TIMEOUT = 20      # seconds per request

# Filter — change to None to keep all legal forms
LEGAL_FORM_FILTER = "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ"

CSV_FIELDS = [
    "nazwa", "nip", "email", "telefon", "www",
    "pkd", "pkd_opis", "data_rejestracji",
    "miejscowosc", "kod", "ulica", "nr_domu", "krs",
]

# ── API helpers ───────────────────────────────────────────────────────────────

def get_bulletin(day: str) -> list:
    """Return list of KRS numbers that had any change on `day` (YYYY-MM-DD)."""
    url = f"{BASE_URL}/Krs/Biuletyn/{day}"
    for attempt in range(RETRY_ATTEMPTS):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                return data if isinstance(data, list) else []
            if r.status_code == 404:
                return []
        except Exception:
            pass
        if attempt < RETRY_ATTEMPTS - 1:
            time.sleep(RETRY_DELAY)
    return []


def fetch_company(krs: str) -> dict | None:
    """Fetch full KRS record for one company. Returns raw JSON or None."""
    url = f"{BASE_URL}/krs/OdpisAktualny/{krs}?rejestr=P&format=json"
    for attempt in range(RETRY_ATTEMPTS):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (204, 404):
                return None
        except Exception:
            pass
        if attempt < RETRY_ATTEMPTS - 1:
            time.sleep(RETRY_DELAY)
    return None


# ── Data extraction ───────────────────────────────────────────────────────────

def parse_lead(data: dict, target_date: str, legal_form_filter: str | None) -> dict | None:
    """
    Parse a KRS JSON response.
    Returns a lead dict if the company matches criteria, else None.
    """
    odpis  = data.get("odpis", {})
    header = odpis.get("naglowekA", {})
    dzial1 = odpis.get("dane", {}).get("dzial1", {})
    dzial3 = odpis.get("dane", {}).get("dzial3", {})

    # ── Filter 1: only NEW registrations on target_date ──────────────────────
    raw_date = header.get("dataRejestracjiWKRS", "")
    try:
        reg_date = datetime.strptime(raw_date, "%d.%m.%Y").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None
    if reg_date != target_date:
        return None

    # ── Filter 2: legal form ──────────────────────────────────────────────────
    dane_podmiotu = dzial1.get("danePodmiotu", {})
    legal_form    = dane_podmiotu.get("formaPrawna", "")
    if legal_form_filter and legal_form_filter not in legal_form:
        return None

    # ── Address ───────────────────────────────────────────────────────────────
    sia    = dzial1.get("siedzibaIAdres", {})
    siedz  = sia.get("siedziba", {})
    adres  = sia.get("adres", {})

    # ── Main PKD industry code ────────────────────────────────────────────────
    pkd_code = pkd_desc = ""
    prev = dzial3.get("przedmiotDzialalnosci", {}).get(
        "przedmiotPrzewazajacejDzialalnosci", []
    )
    if isinstance(prev, list) and prev:
        first = prev[0]
        if isinstance(first, dict):
            vals = list(first.values())
            # vals[0] = sequence number, vals[1] = pkd code, vals[2] = description
            pkd_code = vals[1] if len(vals) > 1 else ""
            pkd_desc = vals[2] if len(vals) > 2 else ""

    return {
        "nazwa":            dane_podmiotu.get("nazwa", ""),
        "nip":              dane_podmiotu.get("identyfikatory", {}).get("nip", ""),
        "email":            sia.get("adresPocztyElektronicznej", ""),
        "telefon":          sia.get("telefon", ""),
        "www":              sia.get("adresStronyInternetowej", ""),
        "pkd":              pkd_code,
        "pkd_opis":         pkd_desc,
        "data_rejestracji": reg_date,
        "miejscowosc":      siedz.get("miejscowosc", ""),
        "kod":              adres.get("kodPocztowy", ""),
        "ulica":            adres.get("ulica", ""),
        "nr_domu":          adres.get("nrDomu", ""),
        "krs":              header.get("numerKRS", ""),
    }


# ── Core scraper ──────────────────────────────────────────────────────────────

def scrape_date(day: str, legal_form_filter: str | None = LEGAL_FORM_FILTER) -> list:
    """Scrape all matching new registrations for a single date."""
    print(f"\n📅  {day} — fetching KRS bulletin...")
    krs_numbers = get_bulletin(day)

    if not krs_numbers:
        print(f"    No KRS changes found.")
        return []

    total = len(krs_numbers)
    print(f"    {total} KRS records changed. Checking for new registrations...")

    leads     = []
    processed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_company, k): k for k in krs_numbers}

        for future in as_completed(futures):
            processed += 1
            data = future.result()
            if data:
                lead = parse_lead(data, day, legal_form_filter)
                if lead:
                    leads.append(lead)

            if processed % 500 == 0 or processed == total:
                pct = round(processed / total * 100)
                print(f"    {processed}/{total} ({pct}%) — {len(leads)} leads found")

    print(f"    ✅  {len(leads)} new registrations on {day}")
    return leads


# ── CSV output ────────────────────────────────────────────────────────────────

def save_csv(leads: list, filepath: str):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(leads)
    print(f"\n💾  Saved {len(leads)} leads → {filepath}")


# ── Entry point ───────────────────────────────────────────────────────────────

def parse_args():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = [a for a in sys.argv[1:] if a.startswith("--")]

    all_forms = "--all-forms" in flags
    legal_form = None if all_forms else LEGAL_FORM_FILTER

    today = date.today().strftime("%Y-%m-%d")

    if len(args) == 0:
        dates = [today]
    elif len(args) == 1:
        dates = [args[0]]
    elif len(args) == 2:
        start = datetime.strptime(args[0], "%Y-%m-%d").date()
        end   = datetime.strptime(args[1], "%Y-%m-%d").date()
        dates = []
        cur = start
        while cur <= end:
            dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
    else:
        print("Usage: python krs_leads.py [start_date] [end_date] [--all-forms]")
        sys.exit(1)

    return dates, legal_form


if __name__ == "__main__":
    dates, legal_form = parse_args()

    print("=" * 55)
    print("  Poland KRS Lead Scraper")
    print(f"  Dates  : {dates[0]}" + (f" → {dates[-1]}" if len(dates) > 1 else ""))
    print(f"  Filter : {legal_form or 'all legal forms'}")
    print("=" * 55)

    all_leads = []
    for day in dates:
        day_leads = scrape_date(day, legal_form)
        all_leads.extend(day_leads)

    if not all_leads:
        print("\nNo leads found.")
        sys.exit(0)

    # Output filename
    if len(dates) == 1:
        out = f"leads_poland_{dates[0]}.csv"
    else:
        out = f"leads_poland_{dates[0]}_to_{dates[-1]}.csv"

    save_csv(all_leads, out)
    print(f"\n🎉  Total: {len(all_leads)} leads across {len(dates)} day(s)")
