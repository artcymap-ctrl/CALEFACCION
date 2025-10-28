#!/usr/bin/env python3
import csv, io, os, re, json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup

URL = "https://www.aemet.es/es/eltiempo/observacion/ultimosdatos?k=pva&l=9091R&w=0&datos=det&x=&f=temperatura"
OUT = "data/9091R_temp_hourly.csv"
TZ_LOCAL = ZoneInfo("Europe/Madrid")
HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; CYMAP-collector)"}

def ensure_dirs():
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

def fetch_html():
    r = requests.get(URL, timeout=30, headers=HEADERS)
    r.raise_for_status()
    return r.text

def try_csv(html: str):
    soup = BeautifulSoup(html, "html.parser")
    a = soup.find("a", href=re.compile(r"\.csv(\?|$)", re.I))
    if not a: return None
    href = a["href"]
    if href.startswith("/"): href = "https://www.aemet.es"+href
    r = requests.get(href, timeout=30, headers=HEADERS); r.raise_for_status()
    text = None
    for enc in ("utf-8","latin-1","cp1252"):
        try: text = r.content.decode(enc); break
        except UnicodeDecodeError: pass
    if text is None: text = r.text
    return parse_csv(text)

def parse_csv(text: str):
    """
    Lee el CSV de AEMET y selecciona de forma estricta:
    - Fecha + Hora (si existen por separado) o bien un campo combinado.
    - Temperatura (ºC) del aire (no tmin, tmax, ni temperatura de suelo ts).
    """
    f = io.StringIO(text)
    sample = f.read(4000); f.seek(0)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
    except Exception:
        dialect = csv.excel
        dialect.delimiter = ';'

    rd = csv.reader(f, dialect)
    headers = next(rd, [])
    # normalizamos encabezados
    norm = [re.sub(r"\s+", " ", h.strip().lower()) for h in headers]

    # --- localizar fecha/hora ---
    # candidatos razonables en AEMET: "fecha", "hora", "fecha (hora local)", etc.
    idx_fecha = None
    idx_hora  = None
    idx_dt_combinado = None

    for i, h in enumerate(norm):
        if "fecha" in h and "hora" in h:
            idx_dt_combinado = i
            break

    if idx_dt_combinado is None:
        for i, h in enumerate(norm):
            if "fecha" in h:
                idx_fecha = i; break
        for i, h in enumerate(norm):
            if "hora" in h:
                idx_hora = i; break

    # --- localizar temperatura del aire ---
    # reglas: debe contener "temperatura" y, opcionalmente, "ºc"
    # se excluyen expresamente máximas/mínimas/suelo
    bad_tokens = ("máx", "max", "mín", "min", "suelo", "ts")
    def is_temp_col(h):
        if "temperatura" not in h:
            return False
        if any(bt in h for bt in bad_tokens):
            return False
        return True

    temp_idx = None
    for i, h in enumerate(norm):
        if is_temp_col(h):
            temp_idx = i
            break

    # fallback muy conservador: si no se encontró nada, intenta "ºc" pero sin máx/mín/ts
    if temp_idx is None:
        for i, h in enumerate(norm):
            if "ºc" in h and not any(bt in h for bt in bad_tokens):
                temp_idx = i
                break

    if temp_idx is None:
        print(f"WARN: no pude localizar columna de temperatura. Encabezados: {headers}")
        return []

    print(f"INFO: columnas elegidas → temp='{headers[temp_idx]}' ; "
          f"{'datetime='+headers[idx_dt_combinado] if idx_dt_combinado is not None else f'fecha={headers[idx_fecha]} hora={headers[idx_hora]}'}")

    out = []
    for row in rd:
        if not row:
            continue
        # temperatura
        t = to_float(row[temp_idx] if temp_idx < len(row) else None)
        if t is None:
            continue

        # datetime local
        try:
            if idx_dt_combinado is not None:
                ds = str(row[idx_dt_combinado]).strip()
            else:
                if idx_fecha is None or idx_hora is None:
                    # último recurso: primer campo que parezca fecha+hora
                    ds = " ".join(row[:2])
                else:
                    ds = f"{str(row[idx_fecha]).strip()} {str(row[idx_hora]).strip()}"
            ts_loc = parse_dt_es(ds)
        except Exception:
            # no descartes toda la fila si no encaja el formato exacto; intenta variantes
            ok = False
            for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    d = datetime.strptime(ds, fmt)
                    ts_loc = d.replace(tzinfo=TZ_LOCAL)
                    ok = True
                    break
                except Exception:
                    pass
            if not ok:
                continue

        out.append((align_hour_utc(ts_loc), t))

    return out

def parse_table(html: str):
    soup = BeautifulSoup(html, "html.parser")
    out=[]
    for tr in soup.select("table tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all(["td","th"])]
        if len(cols)<2: continue
        joined = " ".join(cols)
        m = re.search(r"(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})", joined)
        if not m: continue
        ds = f"{m.group(1)} {m.group(2)}"
        mtemp = re.search(r"(-?\d+[.,]?\d*)", joined)
        if not mtemp: continue
        ts_loc = parse_dt_es(ds)
        temp = to_float(mtemp.group(1))
        if temp is None: continue
        out.append((align_hour_utc(ts_loc), temp))
    return out

def parse_dt_es(ds: str) -> datetime:
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"):
        try:
            d = datetime.strptime(ds, fmt)
            return d.replace(tzinfo=TZ_LOCAL)
        except Exception: pass
    raise ValueError(f"No puedo parsear: {ds}")

def align_hour_utc(dt_local: datetime) -> datetime:
    dt_local = dt_local.replace(minute=0, second=0, microsecond=0)
    return dt_local.astimezone(timezone.utc)

def to_float(s):
    try: return float(str(s).replace(',', '.').strip())
    except Exception: return None

def write_merged(path: str, new_rows):
    # new_rows: list[(dt_utc, temp)]
    old=[]
    if os.path.exists(path):
        with open(path, "r", newline="", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            for r in rd:
                dt = datetime.fromisoformat(r["datetime_utc"].replace("Z","+00:00"))
                old.append((dt, float(r["temp_c"])))
    # dedupe prefer last
    m = { dt.isoformat().replace("+00:00","Z"): temp for dt,temp in old }
    for dt,temp in new_rows:
        m[dt.isoformat().replace("+00:00","Z")] = temp
    ordered = sorted(((datetime.fromisoformat(k.replace("Z","+00:00")),v) for k,v in m.items()), key=lambda x:x[0])
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

def write_last_update(path_json="data/last_update.json", path_csv="data/last_update.csv", n_rows=None):
    """
    Genera dos ficheros ligeros con la marca temporal de la última captura:
      - JSON para que la web (index.html) pueda mostrar un badge de “última actualización”
      - CSV paralelo por si quieres consultarlo en Excel/R/etc.
    """
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(TZ_LOCAL)

    meta = {
        "updated_utc": now_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "updated_local": now_local.isoformat(timespec="seconds"),
        "tz_local": "Europe/Madrid",
    }
    if n_rows is not None:
        meta["rows_last_run"] = int(n_rows)

    # JSON para la web
    os.makedirs(os.path.dirname(path_json), exist_ok=True)
    with open(path_json, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    # CSV paralelo (opcional)
    with open(path_csv, "w", encoding="utf-8") as f:
        f.write("updated_local,updated_utc,tz_local,rows_last_run\n")
        f.write(f"{meta['updated_local']},{meta['updated_utc']},{meta['tz_local']},{meta.get('rows_last_run','')}\n")


def main():
    os.makedirs("data", exist_ok=True)
    html = fetch_html()
    rows = try_csv(html)
    if not rows: rows = parse_table(html)
    if not rows: raise SystemExit("No pude extraer filas.")
    write_merged(OUT, rows)
    write_last_update(n_rows=len(rows))
    print(f"OK: {len(rows)} registros. CSV -> {OUT}")

if __name__ == "__main__":
    main()
