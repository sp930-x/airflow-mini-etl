import csv
from pathlib import Path

def validate_processed_csv(processed_path: str) -> str:
    f = Path(processed_path)

    if not f.exists():
        raise FileNotFoundError(f"Processed file not found: {processed_path}")
    if f.stat().st_size < 10:
        raise ValueError(f"Processed file seems too small: {processed_path}")

    with f.open("r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)

    if len(rows) < 200:
        raise ValueError(f"Too few rows: {len(rows)} (expected >= 200)")

    for r in rows:
        if not r.get("time"):
            raise ValueError("Missing time value")
        if not r.get("temperature_2m"):
            raise ValueError("Missing temperature value")

        temp = float(r["temperature_2m"])
        if temp < -50 or temp > 60:
            raise ValueError(f"Temperature out of expected range: {temp}")

    return f"OK: {processed_path} (rows={len(rows)})"

if __name__ == "__main__":
    print(validate_processed_csv("data/processed_weather.csv"))
