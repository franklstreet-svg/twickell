from datetime import datetime, timezone

REQUIRED_SCRAPE_FIELDS = [
    "name",
    "business_type",
    "description",
    "contact",
    "service_area",
    "services",
    "hours",
]

OWNER_FOLLOWUP_FIELDS = [
    "policies",
    "pricing",
    "booking_rules",
    "faqs",
    "emergency_after_hours",
    "appointment_rules",
    "social_links",  # only nice-to-have
]


def _now():
    return datetime.now(timezone.utc).isoformat()


def _find_missing(profile, fields):
    missing = []
    for field in fields:
        value = profile.get(field)
        if value in (None, "", [], {}):
            missing.append(field)
    return missing


def _build_full_site_text(pages: list | None) -> str:
    parts = []
    for page in pages or []:
        if not isinstance(page, dict):
            continue
        text = str(page.get("text") or "").strip()
        if not text:
            continue
        url = str(page.get("url") or "").strip()
        if url:
            parts.append(f"URL: {url}\n{text}")
        else:
            parts.append(text)
    return "\n\n".join(parts).strip()


def build_scrape_result(data: dict, pages: list | None = None):
    profile = {}

    for key, value in (data or {}).items():
        if value:
            profile[key] = value

    scrape_gaps = _find_missing(profile, REQUIRED_SCRAPE_FIELDS)
    owner_followup = _find_missing(profile, OWNER_FOLLOWUP_FIELDS)
    pages = pages or []
    full_site_text = _build_full_site_text(pages)

    return {
        "business_profile": profile,
        "scraped_pages": pages,
        "full_site_text": full_site_text,
        "page_count": len(pages),
        "crawl_stats": {
            "page_count": len(pages),
            "full_site_text_length": len(full_site_text),
        },
        "scrape_gaps": scrape_gaps,
        "owner_followup_needed": owner_followup,
        "knowledge_gaps": scrape_gaps + owner_followup,
        "scraper_ready": len(scrape_gaps) == 0,
        "receptionist_ready": False,
        "last_updated": _now(),
    }
