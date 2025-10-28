#!/usr/bin/env python3
import csv, os

HOURLY = "docs/data/9091R_temp_hourly.csv"
ARCHIVE = "docs/data/9091R_temp_history.csv"

def read_csv(path):
    if not os.path.exists(path): return []
    with open(path, encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)

def write_csv(path, rows):
    fields = ["date_local","time_local","datetime_utc","temp_c","source"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

def main():
    hourly = read_csv(HOURLY)
    if not hourly:
        print("WARN: hourly vacío, nada que archivar"); return

    arch = read_csv(ARCHIVE)
    by_key = { r["datetime_utc"]: r for r in arch }  # existente
    for r in hourly:
        by_key[r["datetime_utc"]] = r  # inserta/actualiza

    merged = list(by_key.values())
    merged.sort(key=lambda r: r["datetime_utc"])
    write_csv(ARCHIVE, merged)
    print(f"OK: histórico actualizado con {len(hourly)} nuevas/actualizadas; total={len(merged)}")

if __name__ == "__main__":
    main()
