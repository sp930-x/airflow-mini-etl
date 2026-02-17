import json
import urllib.request
from datetime import timedelta, datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from urllib.parse import quote

REGIONS = {
    "DE-NW": (51.4556, 7.0116),
    "DE-BY": (48.1351, 11.5820),
    "DE-BE": (52.5200, 13.4050),
}

def extract_weather_archive(
    raw_path: str,
    regions: dict = REGIONS,
    days: int = 30,
    timezone_name: str = "UTC",
) -> str:
    raw_file = Path(raw_path)
    raw_file.parent.mkdir(parents=True, exist_ok=True)

    tz = ZoneInfo(timezone_name)

    # Use "yesterday" to avoid partial-day issues.
    today = datetime.now(timezone.utc).date()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)

    out = {
        "_meta": {
            "source": "open-meteo-archive",
            "timezone": timezone_name,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "fetched_at_local": datetime.now(tz).isoformat(),
        },
        "regions": [],
    }

    for region, (lat, lon) in regions.items():
        url = (
            "https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&start_date={start_date.isoformat()}&end_date={end_date.isoformat()}"
            "&hourly=temperature_2m"
            f"&timezone={quote(timezone_name)}"
        )

        with urllib.request.urlopen(url, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))

        out["regions"].append(
            {"region": region, "latitude": lat, "longitude": lon, "payload": payload}
        )

    raw_file.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(raw_file)

if __name__ == "__main__":
    print(extract_weather_archive("data/raw_weather.json"))
