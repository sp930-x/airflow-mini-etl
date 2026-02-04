import json
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

def extract_weather(raw_path: str, latitude: float = 51.4556, longitude: float = 7.0116) -> str:
    """
    Extract hourly temperature data from Open-Meteo API and store as raw JSON.
    Default coords are Essen-ish. You can change later.
    """
    raw_file = Path(raw_path)
    raw_file.parent.mkdir(parents=True, exist_ok=True)

    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={latitude}&longitude={longitude}"
        "&hourly=temperature_2m"
        "&forecast_days=1"
        "&timezone=Europe%2FBerlin"
    )

    with urllib.request.urlopen(url, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    payload["_meta"] = {
    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    "fetched_at_berlin": datetime.now(ZoneInfo("Europe/Berlin")).isoformat(),
    "timezone": "Europe/Berlin",
        }

    raw_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(raw_file)

if __name__ == "__main__":
    # Quick manual test
    print(extract_weather("data/raw_weather.json"))
