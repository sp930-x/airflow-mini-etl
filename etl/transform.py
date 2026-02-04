# scripts/transform.py
import json
import csv
from pathlib import Path

def transform_raw_to_csv(raw_path: str, processed_path: str) -> str:
    raw_file = Path(raw_path)

    out_file = Path(processed_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    # convert raw json to python dict
    payload = json.loads(raw_file.read_text(encoding="utf-8"))

    # validate the data
    if (
        "hourly" not in payload
        or "time" not in payload["hourly"]
        or "temperature_2m" not in payload["hourly"]
    ):
        raise KeyError(
            "Unexpected API response schema: missing hourly/time/temperature_2m"
        )

    # extract data
    hourly = payload["hourly"]
    times = hourly["time"]
    temps = hourly["temperature_2m"]

    # validate the extracted data
    if len(times) != len(temps):
        raise ValueError(f"Length mismatch: time={len(times)} temp={len(temps)}")
    
    # save as csv
    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["time", "temperature_2m"])
        for t, temp in zip(times, temps):
            writer.writerow([t, temp])

    return str(out_file)

if __name__ == "__main__":
    result = transform_raw_to_csv("data/raw_weather.json", "data/processed_weather.csv")
    print(result)
