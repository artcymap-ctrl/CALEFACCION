#!/usr/bin/env python3
import csv, io, os, re, sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup

URL = "https://www.aemet.es/es/eltiempo/observacion/ultimosdatos?k=pva&l=9091R&w=0&datos=det&x=&f=temperatura"
OUT = "data/9091R_temp_hourly.csv"
TZ_LOCAL = ZoneInfo("Europe/Madrid")
HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; CYMAP-collector)"}
TIMEOUT = 30

def ensure_dirs():
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

def fetch_html():
    r = requests.get(URL, timeout=TIMEOUT, headers=HEADERS)
    r.raise_for_status()
    return r.text

def try_csv(html: str):
    soup = BeautifulSoup(html, "html.parser")
    a = soup.find("a", href=re.compile(r"\.csv(\?|$)", re.I))
    if not a: return None
    href = a["href"]
    if href.startswith("/"): href = "https://www.aemet.es"+href
    r = requests.get(href, timeout=TIMEOUT, headers=HEADERS); r.raise_for_status()
    for enc in ("utf-8","latin-1","cp1252"):
        try:
            text = r.content.decode(enc)
            break
        except UnicodeDecodeError:
            text = None
    if text is None: text = r.text
    return parse_csv(text)

def parse_csv(text: str):
    f = io.StringIO(text); sample = f.read(2000); f.seek(0)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
    except Exception:
        dialect = csv.excel; dialect.delimiter = ';'
    rd = csv.reader(f, dialect)
    headers = next(rd, [])
    hlow = [h.strip().lower() for h in headers]
    i_fecha = next((i for i,h in enumerate(hlow) if "fecha" in h or "hora" in h), None)
    i_temp  = next((i for i,h in enumerate(hlow) if "temperatura" in h or "ºc" in h or h=="t"), None)
    out=[]
    for row in rd:
        if not row or i_fecha is None or i_temp is None: continue
        ds = str(row[i_fecha]).strip()
        temp = to_float(row[i_temp])
        if temp is None: continue
        try:
            ts_loc = parse_dt_es(ds)
        except Exception:
            continue
        out.append((align_hour_utc(ts_loc), temp))
    return out

def parse_table(html: str):
    soup = BeautifulSoup(html, "html.parser")
    out=[]
    for tr in soup.select("table tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all(["td","th"])]
        if len(cols)<2: continue
        joined = " ".join(cols)
        m = re.search(r"(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})", joined)
        mtemp = re.search(r"(-?\d+[.,]?\d*)", joined)
        if not m or not mtemp: continue
        try:
            ts_loc = parse_dt_es(f"{m.group(1)} {m.group(2)}")
        except Exception:
            continue
        temp = to_float(mtemp.group(1))
        if temp is None: continue
        out.append((align_hour_utc(ts_loc), temp))
    return out

def parse_dt_es(ds: str) -> datetime:
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"):
        try:
            d = datetime.strptime(ds, fmt)
            return d.replace(tzinfo=TZ_LOCAL)
        except Exception:
            pass
    raise ValueError(f"No puedo parsear: {ds}")

def align_hour_utc(dt_local: datetime) -> datetime:
    dt_local = dt_local.replace(minute=0, second=0, microsecond=0)
    return dt_local.astimezone(timezone.utc)

def to_float(s):
    try: return float(str(s).replace(',', '.').strip())
    except Exception: return None

def write_merged(path: str, new_rows):
    old=[]
    if os.path.exists(path):
        with open(path, "r", newline="", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            for r in rd:
                try:
                    dt = datetime.fromisoformat(r["datetime_utc"].replace("Z","+00:00"))
                    old.append((dt, float(r["temp_c"])))
                except Exception:
                    continue
    m = { dt.isoformat().replace("+00:00","Z"): temp for dt,temp in old }
    for dt,temp in new_rows:
        m[dt.isoformat().replace("+00:00","Z")] = temp
    ordered = sorted(((datetime.fromisoformat(k.replace("Z","+00:00")),v) for k,v in m.items()), key=lambda x:x[0])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(["date_local","time_local","datetime_utc","temp_c","source"])
        for dt_utc,temp in ordered:
            dt_local = dt_utc.astimezone(TZ_LOCAL)
            wr.writerow([
                dt_local.strftime("%Y-%m-%d"),
                dt_local.strftime("%H:%M"),
                dt_utc.replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z"),
                f"{temp:.1f}",
                "AEMET_ult24h"
            ])

def main():
    try:
        ensure_dirs()
        html = fetch_html()
        rows = try_csv(html)
        if not rows:
            rows = parse_table(html)
        if not rows:
            print("WARN: no se extrajeron filas (AEMET sin datos o formato cambió). Dejamos job como OK.")
            return
        write_merged(OUT, rows)
        print(f"OK: {len(rows)} registros. CSV -> {OUT}")
    except Exception as e:
        print(f"WARN: excepción no crítica: {e.__class__.__name__}: {e}")
        return

if __name__ == "__main__":
    main()
