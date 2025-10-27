#!/usr/bin/env python3
import csv, os, sys, re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# === Configuración ===
URL = "https://www.aemet.es/es/eltiempo/observacion/ultimosdatos?k=pva&l=9091R&w=0&datos=det&x=&f=temperatura"
OUT = "data/9091R_temp_hourly.csv"
TZ_LOCAL = ZoneInfo("Europe/Madrid")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CYMAP-collector)"}
TIMEOUT = 30


# ---------- Utilidades ----------
def ensure_dirs():
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

def fetch_html():
    r = requests.get(URL, timeout=TIMEOUT, headers=HEADERS)
    r.raise_for_status()
    return r.text

def _clean_text(s: str) -> str:
    return (s or "").strip().replace("\xa0", " ")

def _parse_float_celsius(s: str):
    s = _clean_text(s)
    if s in ("", "-", "ND"):
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def _is_temp_header(h: str) -> bool:
    """
    Detecta cabeceras tipo:
      - 'Temperatura (ºC)'
      - 'Temp. (°C)'
      - variantes con º/°, con o sin paréntesis/puntos
    """
    raw = _clean_text(h).lower()
    # Normalizamos símbolos y puntuación para ser tolerantes
    norm = raw.replace("º", "°")
    norm = re.sub(r"[\.\(\)\[\],%/-]+", " ", norm)  # quita puntuación común
    norm = re.sub(r"\s+", " ", norm).strip()
    # Reglas: contiene 'temp' o 'temperatura' Y hace referencia a 'c' o '°c'
    has_temp_word = ("temp" in norm) or ("temperatura" in norm)
    has_c_unit = ("°c" in raw) or ("ºc" in raw) or re.search(r"\bc\b", norm) is not None
    return has_temp_word and has_c_unit


# ---------- Parser robusto por cabeceras ----------
def parse_aemet_html_last24(html: str):
    soup = BeautifulSoup(html, "html.parser")

    table = soup.select_one("table.tabla_datos, table#table, table")
    if not table:
        raise RuntimeError("No se encontró la tabla de datos en el HTML de AEMET")

    thead = table.find("thead")
    if not thead:
        raise RuntimeError("La tabla no contiene THEAD con cabeceras")

    headers = [_clean_text(th.get_text()) for th in thead.select("th")]
    if not headers:
        raise RuntimeError("No se pudieron leer cabeceras de la tabla")

    # índices por inspección de cabeceras
    idx_fecha = -1
    idx_temp = -1
    for i, h in enumerate(headers):
        hlow = _clean_text(h).lower()
        if idx_fecha < 0 and ("fecha" in hlow and "hora" in hlow):
            idx_fecha = i
        if idx_temp < 0 and _is_temp_header(h):
            idx_temp = i

    if idx_fecha < 0:
        raise RuntimeError(f"No encontré la columna de fecha/hora. Cabeceras: {headers}")
    if idx_temp < 0:
        raise RuntimeError(f"No encontré la columna de temperatura. Cabeceras: {headers}")

    out = []
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) <= max(idx_fecha, idx_temp):
            continue

        fecha_txt = _clean_text(tds[idx_fecha].get_text())
        temp_txt  = _clean_text(tds[idx_temp ].get_text())

        # Formato habitual AEMET: "dd/mm/YYYY HH:MM" (a veces con " h")
        try:
            dt_local = datetime.strptime(fecha_txt, "%d/%m/%Y %H:%M").replace(tzinfo=TZ_LOCAL)
        except ValueError:
            fecha_txt2 = fecha_txt.replace(" h", "")
            try:
                dt_local = datetime.strptime(fecha_txt2, "%d/%m/%Y %H:%M").replace(tzinfo=TZ_LOCAL)
            except Exception:
                continue

        ts_utc = dt_local.astimezone(timezone.utc)
        temp_c = _parse_float_celsius(temp_txt)
        if temp_c is None:
            continue

        out.append((ts_utc, temp_c))

    # ordenar y deduplicar por timestamp
    out.sort(key=lambda x: x[0])
    uniq = {}
    for ts, v in out:
        uniq[ts] = v
    return [(ts, uniq[ts]) for ts in sorted(uniq.keys())]


# ---------- Escritura CSV ----------
def write_csv(pairs):
    """
    pairs: lista de (ts_utc, temp_c)
    Guarda OUT con cabecera: date_local,time_local,datetime_utc,temp_c,source
    """
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date_local", "time_local", "datetime_utc", "temp_c", "source"])
        for ts_utc, temp in pairs:
            ts_loc = ts_utc.astimezone(TZ_LOCAL)
            w.writerow([
                ts_loc.strftime("%Y-%m-%d"),
                ts_loc.strftime("%H:%M"),
                ts_utc.replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z"),
                f"{temp:.1f}",
                "AEMET_ult24h"
            ])


# ---------- Main ----------
def main():
    try:
        ensure_dirs()
        html = fetch_html()
        pairs = parse_aemet_html_last24(html)
        print(f"INFO: HTML AEMET: {len(pairs)} registros válidos")
        if not pairs:
            print("ERROR: No se obtuvieron registros", file=sys.stderr)
            sys.exit(2)
        write_csv(pairs)
        print(f"OK: {len(pairs)} registros. CSV -> {OUT}")
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
