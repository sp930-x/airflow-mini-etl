import json
import csv
from pathlib import Path


def transform_raw_to_csv(
    raw_path: str = "data/raw_weather.json",
    processed_path: str = "data/processed_weather.csv",
) -> str:
    """
    Transform multi-region Open-Meteo archive JSON into a flat CSV:
      region,time,temperature_2m
    """
    raw_file = Path(raw_path)
    out_file = Path(processed_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    data = json.loads(raw_file.read_text(encoding="utf-8"))

    regions = data.get("regions")
    if not isinstance(regions, list) or len(regions) == 0:
        raise ValueError("Invalid raw schema: expected top-level key 'regions' as a non-empty list")

    rows = []
    for item in regions:
        region = item.get("region")
        payload = item.get("payload", {})
        hourly = payload.get("hourly", {})

        times = hourly.get("time")
        temps = hourly.get("temperature_2m")

        if not region:
            raise ValueError("Missing 'region' in one of regions[]")
        if not isinstance(times, list) or not isinstance(temps, list):
            raise ValueError(f"Missing hourly.time / hourly.temperature_2m for region={region}")
        if len(times) != len(temps):
            raise ValueError(f"Length mismatch for region={region}: time={len(times)} temp={len(temps)}")

        for t, temp in zip(times, temps):
            # Keep time as the ISO8601 string from Open-Meteo (UTC in the extract)
            rows.append((region, t, temp))

    with out_file.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["region", "time", "temperature_2m"])
        w.writerows(rows)

    return str(out_file)


if __name__ == "__main__":
    out = transform_raw_to_csv()
    print(f"✅ Wrote: {out}")
