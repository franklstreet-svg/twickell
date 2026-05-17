MODULE_MANIFEST = {
    "id": "M031",
    "name": "Weather",
    "category": "Core",
    "description": "Get current weather conditions and forecasts for any location.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["get_weather"],
    "min_bridge_version": "1.0.0"
}

import requests
import logging

log = logging.getLogger(__name__)

# Uses Open-Meteo (free, no API key) + geocoding from nominatim
WMO_CODES = {
    0: 'Clear sky', 1: 'Mainly clear', 2: 'Partly cloudy', 3: 'Overcast',
    45: 'Foggy', 48: 'Depositing rime fog',
    51: 'Light drizzle', 53: 'Moderate drizzle', 55: 'Dense drizzle',
    61: 'Slight rain', 63: 'Moderate rain', 65: 'Heavy rain',
    71: 'Slight snow', 73: 'Moderate snow', 75: 'Heavy snow',
    80: 'Slight showers', 81: 'Moderate showers', 82: 'Violent showers',
    95: 'Thunderstorm', 96: 'Thunderstorm with slight hail',
    99: 'Thunderstorm with heavy hail',
}


def _geocode(location: str):
    r = requests.get(
        'https://nominatim.openstreetmap.org/search',
        params={'q': location, 'format': 'json', 'limit': 1},
        headers={'User-Agent': 'MyOrby/1.0'},
        timeout=5
    )
    r.raise_for_status()
    results = r.json()
    if not results:
        return None, None
    return float(results[0]['lat']), float(results[0]['lon'])


def get_weather(location: str) -> str:
    try:
        lat, lon = _geocode(location)
        if lat is None:
            return f"I couldn't find '{location}' — try a city name."

        r = requests.get(
            'https://api.open-meteo.com/v1/forecast',
            params={
                'latitude': lat, 'longitude': lon,
                'current': 'temperature_2m,apparent_temperature,weathercode,windspeed_10m,precipitation',
                'daily': 'weathercode,temperature_2m_max,temperature_2m_min,precipitation_sum',
                'temperature_unit': 'fahrenheit',
                'wind_speed_unit': 'mph',
                'precipitation_unit': 'inch',
                'forecast_days': 3,
                'timezone': 'auto'
            },
            timeout=8
        )
        r.raise_for_status()
        data = r.json()
        cur = data['current']
        daily = data['daily']

        condition = WMO_CODES.get(cur['weathercode'], 'Unknown')
        temp = cur['temperature_2m']
        feels = cur['apparent_temperature']
        wind = cur['windspeed_10m']

        lines = [
            f"Current in {location}: {condition}, {temp:.0f}°F (feels like {feels:.0f}°F), wind {wind:.0f} mph."
        ]

        for i in range(min(3, len(daily['time']))):
            day_cond = WMO_CODES.get(daily['weathercode'][i], 'Unknown')
            hi = daily['temperature_2m_max'][i]
            lo = daily['temperature_2m_min'][i]
            precip = daily['precipitation_sum'][i]
            lines.append(
                f"{daily['time'][i]}: {day_cond}, {lo:.0f}–{hi:.0f}°F" +
                (f", {precip:.2f}\" rain" if precip > 0 else "")
            )

        return "\n".join(lines)

    except Exception as e:
        log.warning('Weather error: %s', e)
        return f"I couldn't get the weather right now. Try again in a moment."
