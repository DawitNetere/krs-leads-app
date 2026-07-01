#!/usr/bin/env python3
"""
Daily automated job: scrape today's KRS leads -> push to Instantly campaign.

Required env vars:
  INSTANTLY_API_KEY      — your Instantly API key
  INSTANTLY_CAMPAIGN_ID  — the campaign to add leads to

Optional:
  REGON_API_KEY          — overrides the default key in regon.py
  SCRAPE_DATE            — override date (YYYY-MM-DD), defaults to today
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import requests

# Load .env when running locally (ignored in CI where env vars come from secrets)
try:
    from pathlib import Path
    _env = Path(__file__).parent / ".env"
    if _env.exists():
        for _line in _env.read_text().splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                import os as _os; _os.environ.setdefault(_k.strip(), _v.strip())
except Exception:
    pass

import regon as _regon_module
from krs_leads import (
    ACCOUNTING_PKD_PREFIXES,
    LEGAL_FORM_FILTER,
    fetch_company,
    get_bulletin,
    parse_lead,
)
from regon import enrich_leads

# Allow REGON key override via env without touching regon.py
_regon_module.REGON_API_KEY = os.getenv("REGON_API_KEY", _regon_module.REGON_API_KEY)

INSTANTLY_API_KEY = os.environ["INSTANTLY_API_KEY"]
INSTANTLY_CAMPAIGN_ID = os.environ["INSTANTLY_CAMPAIGN_ID"]
INSTANTLY_LEADS_URL = "https://api.instantly.ai/api/v2/leads"
INSTANTLY_BATCH_SIZE = 1000


def scrape_day(day: str) -> list:
    print(f"Scraping KRS bulletin for {day}...")
    krs_numbers = list(dict.fromkeys(get_bulletin(day)))
    if not krs_numbers:
        print("No KRS changes found.")
        return []

    print(f"{len(krs_numbers)} KRS records changed. Processing...")
    leads = []
    seen_krs: set = set()

    with ThreadPoolExecutor(max_workers=100) as pool:
        futures = {pool.submit(fetch_company, krs): krs for krs in krs_numbers}
        for i, future in enumerate(as_completed(futures), 1):
            data = future.result()
            if data:
                lead = parse_lead(data, day, LEGAL_FORM_FILTER)
                if lead and lead["krs"] not in seen_krs:
                    if not any(lead["pkd"].startswith(p) for p in ACCOUNTING_PKD_PREFIXES):
                        seen_krs.add(lead["krs"])
                        leads.append(lead)
            if i % 500 == 0 or i == len(krs_numbers):
                print(f"  {i}/{len(krs_numbers)} checked — {len(leads)} candidates")

    print(f"Enriching {len(leads)} leads via REGON...")
    leads = enrich_leads(leads)
    leads = [l for l in leads if l.get("email", "").strip()]
    print(f"{len(leads)} leads have an email address.")
    return leads


def push_to_instantly(leads: list) -> int:
    if not leads:
        return 0

    headers = {
        "Authorization": f"Bearer {INSTANTLY_API_KEY}",
        "Content-Type": "application/json",
    }
    pushed = 0

    for i in range(0, len(leads), INSTANTLY_BATCH_SIZE):
        batch = leads[i : i + INSTANTLY_BATCH_SIZE]
        payload = {
            "campaign_id": INSTANTLY_CAMPAIGN_ID,
            "leads": [
                {
                    "email": l["email"],
                    "company_name": l["nazwa"],
                    "phone": l.get("telefon", ""),
                    "website": l.get("www", ""),
                    "variables": {
                        "nip": l.get("nip", ""),
                        "pkd": l.get("pkd", ""),
                        "pkd_opis": l.get("pkd_opis", ""),
                        "data_rejestracji": l.get("data_rejestracji", ""),
                        "miejscowosc": l.get("miejscowosc", ""),
                        "krs": l.get("krs", ""),
                    },
                }
                for l in batch
            ],
        }
        resp = requests.post(INSTANTLY_LEADS_URL, json=payload, headers=headers, timeout=30)
        if resp.status_code not in (200, 201):
            print(f"  Instantly error {resp.status_code}: {resp.text[:300]}", file=sys.stderr)
            sys.exit(1)
        pushed += len(batch)
        print(f"  Pushed batch of {len(batch)} leads to Instantly.")

    return pushed


if __name__ == "__main__":
    day = os.getenv("SCRAPE_DATE") or date.today().strftime("%Y-%m-%d")
    leads = scrape_day(day)
    pushed = push_to_instantly(leads)
    if pushed:
        print(f"\nDone! {pushed} leads added to Instantly campaign {INSTANTLY_CAMPAIGN_ID}.")
    else:
        print("\nNo leads to push today.")
