#!/usr/bin/env python3
import csv, os, sys
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

def _pick_index(headers, *candidates):
    """
    Devuelve el índice de la cabecera que contenga TODOS los fragmentos de `candidates`
    (búsqueda case-insensitive). Ej: _pick_index(H, "temperatura", "ºc")
    """
    H = [h.lower() for h in headers]
    for i, h in enumerate(H):
        if all(frag.lower() in h for frag in candidates):
            return i
    return -1


# ---------- Parser robusto por cabeceras ----------
def parse_aemet_html_last24(html: str):
    """
    Lee la tabla HTML de AEMET 'últimos datos' y devuelve lista de tuplas:
      [(ts_utc, temp_c), ...]
    La detección es por cabeceras:
      - Fecha y hora oficial
      - Temperatura (ºC)
    """
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

    idx_fecha = _pick_index(headers, "fecha", "hora")
    idx_temp  = _pick_index(headers, "temperatura", "ºc")

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

        # AEMET usa formato "dd/mm/YYYY HH:MM" (a veces con " h" al final)
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
