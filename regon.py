import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

REGON_URL = 'https://wyszukiwarkaregon.stat.gov.pl/wsBIR/UslugaBIRzewnPubl.svc'
REGON_API_KEY = 'e437d0d95d92445990fe'


def _soap(sid: str | None, action: str, body_inner: str) -> str:
    sid_header = f'<ns:sid>{sid}</ns:sid>' if sid else ''
    envelope = f'''<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
  xmlns:ns="http://CIS/BIR/PUBL/2014/07"
  xmlns:dat="http://CIS/BIR/PUBL/2014/07/DataContract">
  <soap:Header xmlns:wsa="http://www.w3.org/2005/08/addressing">
    <wsa:To>{REGON_URL}</wsa:To>
    <wsa:Action>http://CIS/BIR/PUBL/2014/07/IUslugaBIRzewnPubl/{action}</wsa:Action>
    {sid_header}
  </soap:Header>
  <soap:Body>{body_inner}</soap:Body>
</soap:Envelope>'''
    headers = {'Content-Type': 'application/soap+xml;charset=UTF-8'}
    if sid:
        headers['sid'] = sid
    try:
        r = requests.post(REGON_URL, data=envelope.encode(), headers=headers, timeout=20)
        return r.text
    except Exception:
        return ''


def _decode(text: str, tag: str) -> str:
    m = re.search(rf'<{tag}[^>]*>(.*?)</{tag}>', text, re.DOTALL)
    if not m:
        return ''
    return m.group(1).replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&').strip()


def login() -> str | None:
    resp = _soap(None, 'Zaloguj',
        f'<ns:Zaloguj><ns:pKluczUzytkownika>{REGON_API_KEY}</ns:pKluczUzytkownika></ns:Zaloguj>')
    m = re.search(r'<ZalogujResult>(.+?)</ZalogujResult>', resp)
    return m.group(1) if m and m.group(1) else None


def _get_regon_number(sid: str, nip: str) -> str | None:
    resp = _soap(sid, 'DaneSzukajPodmioty', f'''<ns:DaneSzukajPodmioty>
  <ns:pParametryWyszukiwania><dat:Nip>{nip}</dat:Nip></ns:pParametryWyszukiwania>
</ns:DaneSzukajPodmioty>''')
    xml = _decode(resp, 'DaneSzukajPodmiotyResult')
    m = re.search(r'<Regon>(\d+)</Regon>', xml)
    return m.group(1) if m else None


def _get_full_report(sid: str, regon: str) -> dict:
    resp = _soap(sid, 'DanePobierzPelnyRaport', f'''<ns:DanePobierzPelnyRaport>
  <ns:pRegon>{regon}</ns:pRegon>
  <ns:pNazwaRaportu>BIR11OsPrawna</ns:pNazwaRaportu>
</ns:DanePobierzPelnyRaport>''')
    xml = _decode(resp, 'DanePobierzPelnyRaportResult')
    fields = dict(re.findall(r'<(\w+)>([^<]*)</\1>', xml))
    return {
        'telefon': fields.get('praw_numerTelefonu', '').strip(),
        'email':   fields.get('praw_adresEmail', '').strip(),
    }


def enrich_lead(sid: str, lead: dict) -> dict:
    nip = lead.get('nip', '').strip()
    if not nip:
        return lead

    regon = _get_regon_number(sid, nip)
    if not regon:
        return lead

    extra = _get_full_report(sid, regon)

    if not lead.get('telefon') and extra['telefon']:
        lead['telefon'] = extra['telefon']
    if not lead.get('email') and extra['email']:
        lead['email'] = extra['email']

    return lead


def enrich_leads(leads: list, progress_cb=None) -> list:
    sid = login()
    if not sid:
        return leads

    enriched = []
    total = len(leads)

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(enrich_lead, sid, lead): i for i, lead in enumerate(leads)}
        done = 0
        for future in as_completed(futures):
            enriched.append(future.result())
            done += 1
            if progress_cb:
                progress_cb(done, total)

    return enriched
