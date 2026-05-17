from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

CITY_TZ = {
    # United States
    'new york': 'America/New_York', 'nyc': 'America/New_York', 'new york city': 'America/New_York',
    'los angeles': 'America/Los_Angeles', 'la': 'America/Los_Angeles',
    'chicago': 'America/Chicago', 'houston': 'America/Chicago', 'dallas': 'America/Chicago',
    'san antonio': 'America/Chicago', 'austin': 'America/Chicago',
    'phoenix': 'America/Phoenix', 'tucson': 'America/Phoenix',
    'philadelphia': 'America/New_York', 'miami': 'America/New_York',
    'atlanta': 'America/New_York', 'boston': 'America/New_York',
    'washington': 'America/New_York', 'dc': 'America/New_York',
    'san diego': 'America/Los_Angeles', 'san jose': 'America/Los_Angeles',
    'san francisco': 'America/Los_Angeles', 'sf': 'America/Los_Angeles',
    'seattle': 'America/Los_Angeles', 'portland': 'America/Los_Angeles',
    'las vegas': 'America/Los_Angeles', 'reno': 'America/Los_Angeles',
    'denver': 'America/Denver', 'salt lake city': 'America/Denver', 'slc': 'America/Denver',
    'honolulu': 'Pacific/Honolulu', 'hawaii': 'Pacific/Honolulu',
    'anchorage': 'America/Anchorage', 'alaska': 'America/Anchorage',
    'minneapolis': 'America/Chicago', 'st louis': 'America/Chicago',
    'kansas city': 'America/Chicago', 'omaha': 'America/Chicago',
    'nashville': 'America/Chicago', 'new orleans': 'America/Chicago',
    'detroit': 'America/Detroit', 'cleveland': 'America/New_York',
    'indianapolis': 'America/Indiana/Indianapolis',
    # Canada
    'toronto': 'America/Toronto', 'canada': 'America/Toronto',
    'vancouver': 'America/Vancouver', 'montreal': 'America/Toronto',
    'calgary': 'America/Edmonton', 'ottawa': 'America/Toronto',
    # Latin America
    'mexico city': 'America/Mexico_City', 'mexico': 'America/Mexico_City',
    'sao paulo': 'America/Sao_Paulo', 'brazil': 'America/Sao_Paulo',
    'rio de janeiro': 'America/Sao_Paulo', 'rio': 'America/Sao_Paulo',
    'buenos aires': 'America/Argentina/Buenos_Aires', 'argentina': 'America/Argentina/Buenos_Aires',
    'bogota': 'America/Bogota', 'colombia': 'America/Bogota',
    'lima': 'America/Lima', 'peru': 'America/Lima',
    'santiago': 'America/Santiago', 'chile': 'America/Santiago',
    'caracas': 'America/Caracas', 'venezuela': 'America/Caracas',
    'havana': 'America/Havana', 'cuba': 'America/Havana',
    # Europe
    'london': 'Europe/London', 'uk': 'Europe/London', 'england': 'Europe/London',
    'britain': 'Europe/London', 'edinburgh': 'Europe/London', 'dublin': 'Europe/Dublin',
    'ireland': 'Europe/Dublin',
    'paris': 'Europe/Paris', 'france': 'Europe/Paris', 'lyon': 'Europe/Paris',
    'berlin': 'Europe/Berlin', 'germany': 'Europe/Berlin', 'munich': 'Europe/Berlin',
    'hamburg': 'Europe/Berlin', 'frankfurt': 'Europe/Berlin',
    'madrid': 'Europe/Madrid', 'spain': 'Europe/Madrid', 'barcelona': 'Europe/Madrid',
    'rome': 'Europe/Rome', 'italy': 'Europe/Rome', 'milan': 'Europe/Rome',
    'amsterdam': 'Europe/Amsterdam', 'netherlands': 'Europe/Amsterdam', 'holland': 'Europe/Amsterdam',
    'brussels': 'Europe/Brussels', 'belgium': 'Europe/Brussels',
    'zurich': 'Europe/Zurich', 'switzerland': 'Europe/Zurich', 'geneva': 'Europe/Zurich',
    'vienna': 'Europe/Vienna', 'austria': 'Europe/Vienna',
    'stockholm': 'Europe/Stockholm', 'sweden': 'Europe/Stockholm',
    'oslo': 'Europe/Oslo', 'norway': 'Europe/Oslo',
    'copenhagen': 'Europe/Copenhagen', 'denmark': 'Europe/Copenhagen',
    'helsinki': 'Europe/Helsinki', 'finland': 'Europe/Helsinki',
    'warsaw': 'Europe/Warsaw', 'poland': 'Europe/Warsaw',
    'prague': 'Europe/Prague', 'czech republic': 'Europe/Prague', 'czechia': 'Europe/Prague',
    'budapest': 'Europe/Budapest', 'hungary': 'Europe/Budapest',
    'bucharest': 'Europe/Bucharest', 'romania': 'Europe/Bucharest',
    'athens': 'Europe/Athens', 'greece': 'Europe/Athens',
    'moscow': 'Europe/Moscow', 'russia': 'Europe/Moscow', 'st petersburg': 'Europe/Moscow',
    'kyiv': 'Europe/Kiev', 'ukraine': 'Europe/Kiev', 'kiev': 'Europe/Kiev',
    'istanbul': 'Europe/Istanbul', 'turkey': 'Europe/Istanbul', 'ankara': 'Europe/Istanbul',
    'lisbon': 'Europe/Lisbon', 'portugal': 'Europe/Lisbon',
    'bern': 'Europe/Zurich', 'luxembourg': 'Europe/Luxembourg',
    'zagreb': 'Europe/Zagreb', 'croatia': 'Europe/Zagreb',
    'belgrade': 'Europe/Belgrade', 'serbia': 'Europe/Belgrade',
    'sofia': 'Europe/Sofia', 'bulgaria': 'Europe/Sofia',
    'riga': 'Europe/Riga', 'latvia': 'Europe/Riga',
    'vilnius': 'Europe/Vilnius', 'lithuania': 'Europe/Vilnius',
    'tallinn': 'Europe/Tallinn', 'estonia': 'Europe/Tallinn',
    # Middle East
    'dubai': 'Asia/Dubai', 'uae': 'Asia/Dubai', 'abu dhabi': 'Asia/Dubai',
    'riyadh': 'Asia/Riyadh', 'saudi arabia': 'Asia/Riyadh', 'jeddah': 'Asia/Riyadh',
    'doha': 'Asia/Qatar', 'qatar': 'Asia/Qatar',
    'kuwait': 'Asia/Kuwait', 'kuwait city': 'Asia/Kuwait',
    'bahrain': 'Asia/Bahrain', 'manama': 'Asia/Bahrain',
    'muscat': 'Asia/Muscat', 'oman': 'Asia/Muscat',
    'tehran': 'Asia/Tehran', 'iran': 'Asia/Tehran',
    'baghdad': 'Asia/Baghdad', 'iraq': 'Asia/Baghdad',
    'israel': 'Asia/Jerusalem', 'jerusalem': 'Asia/Jerusalem', 'tel aviv': 'Asia/Jerusalem',
    'beirut': 'Asia/Beirut', 'lebanon': 'Asia/Beirut',
    'damascus': 'Asia/Damascus', 'syria': 'Asia/Damascus',
    'amman': 'Asia/Amman', 'jordan': 'Asia/Amman',
    # Africa
    'cairo': 'Africa/Cairo', 'egypt': 'Africa/Cairo', 'alexandria': 'Africa/Cairo',
    'lagos': 'Africa/Lagos', 'nigeria': 'Africa/Lagos', 'abuja': 'Africa/Lagos',
    'nairobi': 'Africa/Nairobi', 'kenya': 'Africa/Nairobi',
    'johannesburg': 'Africa/Johannesburg', 'south africa': 'Africa/Johannesburg',
    'cape town': 'Africa/Johannesburg', 'durban': 'Africa/Johannesburg',
    'casablanca': 'Africa/Casablanca', 'morocco': 'Africa/Casablanca',
    'accra': 'Africa/Accra', 'ghana': 'Africa/Accra',
    'addis ababa': 'Africa/Addis_Ababa', 'ethiopia': 'Africa/Addis_Ababa',
    'dar es salaam': 'Africa/Dar_es_Salaam', 'tanzania': 'Africa/Dar_es_Salaam',
    'khartoum': 'Africa/Khartoum', 'sudan': 'Africa/Khartoum',
    'tunis': 'Africa/Tunis', 'tunisia': 'Africa/Tunis',
    'algiers': 'Africa/Algiers', 'algeria': 'Africa/Algiers',
    'tripoli': 'Africa/Tripoli', 'libya': 'Africa/Tripoli',
    'dakar': 'Africa/Dakar', 'senegal': 'Africa/Dakar',
    # Asia
    'tokyo': 'Asia/Tokyo', 'japan': 'Asia/Tokyo', 'osaka': 'Asia/Tokyo',
    'beijing': 'Asia/Shanghai', 'shanghai': 'Asia/Shanghai', 'china': 'Asia/Shanghai',
    'shenzhen': 'Asia/Shanghai', 'guangzhou': 'Asia/Shanghai', 'chengdu': 'Asia/Shanghai',
    'hong kong': 'Asia/Hong_Kong',
    'seoul': 'Asia/Seoul', 'korea': 'Asia/Seoul', 'south korea': 'Asia/Seoul', 'busan': 'Asia/Seoul',
    'singapore': 'Asia/Singapore',
    'taipei': 'Asia/Taipei', 'taiwan': 'Asia/Taipei',
    'bangkok': 'Asia/Bangkok', 'thailand': 'Asia/Bangkok',
    'jakarta': 'Asia/Jakarta', 'indonesia': 'Asia/Jakarta',
    'manila': 'Asia/Manila', 'philippines': 'Asia/Manila',
    'mumbai': 'Asia/Kolkata', 'delhi': 'Asia/Kolkata', 'india': 'Asia/Kolkata',
    'new delhi': 'Asia/Kolkata', 'kolkata': 'Asia/Kolkata', 'bangalore': 'Asia/Kolkata',
    'chennai': 'Asia/Kolkata', 'hyderabad': 'Asia/Kolkata',
    'karachi': 'Asia/Karachi', 'pakistan': 'Asia/Karachi', 'islamabad': 'Asia/Karachi',
    'lahore': 'Asia/Karachi',
    'dhaka': 'Asia/Dhaka', 'bangladesh': 'Asia/Dhaka',
    'colombo': 'Asia/Colombo', 'sri lanka': 'Asia/Colombo',
    'kathmandu': 'Asia/Kathmandu', 'nepal': 'Asia/Kathmandu',
    'kabul': 'Asia/Kabul', 'afghanistan': 'Asia/Kabul',
    'tashkent': 'Asia/Tashkent', 'uzbekistan': 'Asia/Tashkent',
    'almaty': 'Asia/Almaty', 'kazakhstan': 'Asia/Almaty',
    'kuala lumpur': 'Asia/Kuala_Lumpur', 'malaysia': 'Asia/Kuala_Lumpur', 'kl': 'Asia/Kuala_Lumpur',
    'hanoi': 'Asia/Ho_Chi_Minh', 'ho chi minh': 'Asia/Ho_Chi_Minh', 'vietnam': 'Asia/Ho_Chi_Minh',
    'yangon': 'Asia/Rangoon', 'myanmar': 'Asia/Rangoon', 'burma': 'Asia/Rangoon',
    'phnom penh': 'Asia/Phnom_Penh', 'cambodia': 'Asia/Phnom_Penh',
    'vientiane': 'Asia/Vientiane', 'laos': 'Asia/Vientiane',
    'ulaanbaatar': 'Asia/Ulaanbaatar', 'mongolia': 'Asia/Ulaanbaatar',
    'colombo': 'Asia/Colombo',
    'baku': 'Asia/Baku', 'azerbaijan': 'Asia/Baku',
    'yerevan': 'Asia/Yerevan', 'armenia': 'Asia/Yerevan',
    'tbilisi': 'Asia/Tbilisi', 'georgia': 'Asia/Tbilisi',
    'vladivostok': 'Asia/Vladivostok',
    'novosibirsk': 'Asia/Novosibirsk',
    'ekaterinburg': 'Asia/Yekaterinburg',
    # Pacific & Oceania
    'sydney': 'Australia/Sydney', 'australia': 'Australia/Sydney',
    'melbourne': 'Australia/Melbourne',
    'brisbane': 'Australia/Brisbane', 'queensland': 'Australia/Brisbane',
    'perth': 'Australia/Perth', 'western australia': 'Australia/Perth',
    'adelaide': 'Australia/Adelaide', 'south australia': 'Australia/Adelaide',
    'darwin': 'Australia/Darwin',
    'auckland': 'Pacific/Auckland', 'new zealand': 'Pacific/Auckland',
    'wellington': 'Pacific/Auckland', 'christchurch': 'Pacific/Auckland',
    'fiji': 'Pacific/Fiji', 'suva': 'Pacific/Fiji',
    'guam': 'Pacific/Guam',
    'samoa': 'Pacific/Apia',
    'tahiti': 'Pacific/Tahiti',
}


def get_world_time(location: str) -> str:
    key = location.lower().strip()
    tz_name = CITY_TZ.get(key)

    if not tz_name:
        for city, tz in CITY_TZ.items():
            if key in city or city in key:
                tz_name = tz
                break

    if not tz_name:
        try:
            ZoneInfo(location)
            tz_name = location
        except (ZoneInfoNotFoundError, KeyError, Exception):
            pass

    if not tz_name:
        return (f"[WORLD CLOCK]\nI don't have '{location}' in my database. "
                f"Try a city or country name — like Tokyo, London, Dubai, or India.")

    try:
        tz = ZoneInfo(tz_name)
        now = datetime.now(tz)
        offset = now.strftime('%z')
        utc_offset = f"UTC{offset[:3]}:{offset[3:]}" if offset else ''
        return (f"[WORLD CLOCK]\n"
                f"Location: {location.title()}\n"
                f"Current time: {now.strftime('%I:%M %p')} on {now.strftime('%A, %B %d, %Y')}\n"
                f"Timezone: {tz_name} ({utc_offset})")
    except Exception as e:
        return f"[WORLD CLOCK] Couldn't get time for '{location}'."


def world_clock_snapshot() -> str:
    """Current time in 8 major cities at once."""
    spots = [
        ('New York',   'America/New_York'),
        ('London',     'Europe/London'),
        ('Paris',      'Europe/Paris'),
        ('Dubai',      'Asia/Dubai'),
        ('Mumbai',     'Asia/Kolkata'),
        ('Singapore',  'Asia/Singapore'),
        ('Tokyo',      'Asia/Tokyo'),
        ('Sydney',     'Australia/Sydney'),
    ]
    lines = ['[WORLD CLOCK SNAPSHOT]']
    for city, tz_name in spots:
        try:
            now = datetime.now(ZoneInfo(tz_name))
            lines.append(f"  {city:<12} {now.strftime('%I:%M %p')}  ({now.strftime('%a')})")
        except Exception:
            pass
    return '\n'.join(lines)
