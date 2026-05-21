"""Universal business data extractor.

Takes the structured per-page data (title, meta, jsonld, headings, list_items,
text, internal_links, social_links) from page_parser and produces a comprehensive
business_profile draft that the brain can use OR show the owner to confirm.

NO hardcoded business names. Works on any website.
"""

import re
from urllib.parse import urlparse


# ── US states (for address parsing) ────────────────────────────────────────
US_STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia',
}

# ── Business type keywords (industry classifier) ───────────────────────────
BUSINESS_TYPE_KEYWORDS = [
    # (label, keywords — match ANY)
    # Restaurant: require SPECIFIC food-business phrases, not generic words.
    # ("menu" alone is too common as nav; "chef" alone could be a cooking module.)
    ('Restaurant',         ['our restaurant', 'our menu', 'make a reservation', 'dine in',
                            'takeout orders', 'cuisine', 'order online', 'entree',
                            'private dining', 'chef-driven', 'tasting menu',
                            'today\'s menu', 'view our menu', 'book a table',
                            'happy hour', 'lunch and dinner']),
    ('Bar / Pub',          ['cocktail menu', 'tap list', 'craft beer', 'happy hour',
                            'brewery tour', 'wine list']),
    ('Cafe / Coffee Shop', ['coffee shop', 'espresso', 'latte', 'pastries', 'cafe', 'café']),
    ('Bakery',             ['bakery', 'fresh baked', 'cakes', 'pastries']),
    ('Auto Repair',        ['auto repair', 'mechanic', 'oil change', 'brake service', 'transmission', 'auto shop']),
    ('Auto Parts',         ['auto parts', 'car parts', 'aftermarket parts']),
    ('Plumbing',           ['plumber', 'plumbing', 'drain cleaning', 'pipe repair', 'water heater']),
    ('Electrician',        ['electrician', 'electrical service', 'wiring', 'electrical repair']),
    ('HVAC',               ['hvac', 'heating and cooling', 'air conditioning service', 'furnace repair']),
    ('Roofing',            ['roofing', 'roof repair', 'roof replacement']),
    ('Landscaping',        ['landscaping', 'lawn care', 'lawn maintenance']),
    ('Cleaning Service',   ['cleaning service', 'house cleaning', 'janitorial', 'maid service']),
    ('Construction',       ['general contractor', 'construction', 'remodeling', 'home builder']),
    ('Construction Plan Room / Builders Exchange',
                            ['plan room', 'builders exchange', 'plan distribution', 'bid results',
                             'contractors source', 'plan service']),
    ('Real Estate',        ['real estate', 'realtor', 'mls listings', 'home for sale', 'listings']),
    ('Law Firm',           ['law firm', 'attorney', 'lawyer', 'legal services', 'litigation', 'family law']),
    ('Medical Office',     ['medical practice', 'family medicine', 'physician', 'patient portal', 'medical care']),
    ('Dental Office',      ['dentist', 'dental office', 'dental care', 'teeth cleaning', 'orthodont']),
    ('Chiropractic',       ['chiropractor', 'chiropractic', 'spinal adjustment']),
    ('Veterinary',         ['veterinarian', 'veterinary', 'animal hospital', 'pet clinic']),
    ('Fitness / Gym',      ['gym', 'fitness', 'personal trainer', 'group classes', 'crossfit', 'yoga studio']),
    # Salon/Spa: require specific salon/spa phrases (not just "spa" or "massage" alone)
    ('Salon / Spa',        ['hair salon', 'beauty salon', 'day spa', 'medical spa', 'med spa',
                            'massage therapy', 'manicure and pedicure', 'esthetician',
                            'salon services', 'hair stylist', 'book a service', 'spa packages']),
    ('Photography',        ['photographer', 'photography', 'wedding photographer', 'portrait']),
    ('Retail',             ['shop our', 'free shipping', 'add to cart', 'product catalog']),
    ('E-commerce',         ['add to cart', 'checkout', 'shopping cart', 'online store']),
    ('SaaS / Software',    ['saas', 'software as a service', 'free trial', 'api', 'integrations', 'subscription plans']),
    ('Education / School', ['school', 'enroll', 'tuition', 'students', 'classroom', 'academy']),
    ('Consulting',         ['consulting', 'consultant', 'advisory services']),
    ('Marketing Agency',   ['marketing agency', 'digital marketing', 'seo services', 'ad campaigns']),
    ('Hotel / Lodging',    ['hotel', 'inn', 'bed and breakfast', 'rooms available']),
    ('Insurance',          ['insurance agency', 'insurance broker', 'auto insurance', 'home insurance']),
    ('Financial Services', ['financial advisor', 'wealth management', 'investment services']),
    ('AI / Tech',          ['artificial intelligence', 'ai-powered', 'automation platform']),
]


# ── Day / hours patterns ───────────────────────────────────────────────────
_DAY_NAMES = r'(?:mon(?:day)?|tue(?:s|sday)?|wed(?:nesday)?|thu(?:r|rs|rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)'
_TIME = r'\d{1,2}(?::\d{2})?\s*(?:am|pm|AM|PM|noon|midnight)'
_HOURS_PATTERNS = [
    # "Mon-Fri 8am-5pm" / "Monday-Friday: 9am to 6pm"
    re.compile(rf'{_DAY_NAMES}\s*[-–to]+\s*{_DAY_NAMES}\s*:?\s*{_TIME}\s*[-–to]+\s*{_TIME}', re.IGNORECASE),
    # "open from 9am to 5pm weekdays"
    re.compile(rf'open\s+from\s+{_TIME}\s*[-–to]+\s*{_TIME}(?:\s+(?:weekdays|weekends|daily))?', re.IGNORECASE),
    # "Monday: 9am - 5pm"
    re.compile(rf'{_DAY_NAMES}\s*:?\s*{_TIME}\s*[-–to]+\s*{_TIME}', re.IGNORECASE),
    # "Hours: 9am-5pm Monday through Friday"
    re.compile(rf'(?:hours|open)\s*:?\s*{_TIME}\s*[-–to]+\s*{_TIME}\s+{_DAY_NAMES}\s+(?:through|to|and|-)\s+{_DAY_NAMES}', re.IGNORECASE),
    # "24/7" or "24 hours"
    re.compile(r'(?:24\s*/\s*7|24\s*hours?(?:\s+a\s+day)?(?:\s*,?\s*7\s*days?\s*a\s*week)?|open\s+24\s+hours)', re.IGNORECASE),
    # "by appointment only"
    re.compile(r'by\s+appointment(?:\s+only)?', re.IGNORECASE),
]


# ── Helpers ────────────────────────────────────────────────────────────────

def _clean(text):
    return re.sub(r'\s+', ' ', str(text or '')).strip()


def _unique(items):
    out, seen = [], set()
    for item in items:
        item = _clean(item)
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            out.append(item)
    return out


def _gather_jsonld(pages):
    """Flatten JSON-LD blocks from all pages, expand @graph."""
    out = []
    for page in pages or []:
        for blk in (page.get('jsonld') or []):
            if isinstance(blk, dict):
                # @graph contains a list of records
                if '@graph' in blk and isinstance(blk['@graph'], list):
                    out.extend(blk['@graph'])
                else:
                    out.append(blk)
    return out


def _from_jsonld(jsonld):
    """Pull business profile fields from schema.org JSON-LD."""
    found = {}
    for item in jsonld:
        if not isinstance(item, dict):
            continue
        ty = item.get('@type', '')
        ty_l = (ty if isinstance(ty, str) else ' '.join(ty) if isinstance(ty, list) else '').lower()

        # Organization / LocalBusiness / various subtypes
        if any(k in ty_l for k in ('organization', 'localbusiness', 'restaurant', 'store',
                                    'professionalservice', 'corporation', 'business')):
            if 'name' not in found and item.get('name'):
                found['name'] = _clean(item['name'])
            if 'description' not in found and item.get('description'):
                found['description'] = _clean(item['description'])
            if 'telephone' in item and 'phone' not in found:
                found['phone'] = _clean(item['telephone'])
            if 'email' in item and 'email' not in found:
                found['email'] = _clean(item['email'])
            addr = item.get('address')
            if addr and 'address' not in found:
                if isinstance(addr, dict):
                    parts = [addr.get('streetAddress'), addr.get('addressLocality'),
                             addr.get('addressRegion'), addr.get('postalCode')]
                    found['address'] = ', '.join(p for p in parts if p)
                else:
                    found['address'] = _clean(addr)
            hours = item.get('openingHours') or item.get('openingHoursSpecification')
            if hours and 'hours' not in found:
                if isinstance(hours, list):
                    found['hours'] = '; '.join(str(h) for h in hours if h)
                else:
                    found['hours'] = _clean(hours)
            if 'sameAs' in item and 'social' not in found:
                sameas = item['sameAs']
                if isinstance(sameas, list):
                    found['social'] = sameas
        # FAQPage
        if 'faqpage' in ty_l:
            faqs = []
            for q in item.get('mainEntity', []) or []:
                if isinstance(q, dict):
                    question = _clean(q.get('name', ''))
                    answer_obj = q.get('acceptedAnswer', {})
                    answer = _clean(answer_obj.get('text', '')) if isinstance(answer_obj, dict) else ''
                    if question and answer:
                        faqs.append({'question': question, 'answer': answer})
            if faqs:
                found.setdefault('faqs', []).extend(faqs)
    return found


_GENERIC_NAMES = {'home', 'welcome', 'index', 'main', 'site', 'website', 'untitled', 'page'}


def _clean_name(raw):
    """Strip site-suffix patterns, generic words, weird chars."""
    if not raw:
        return ''
    v = _clean(raw)
    # Split on common delimiters and prefer the LONGER side (the business name
    # is usually longer than "Home" or "Welcome")
    parts = re.split(r'\s*[\|\-–—:]\s*', v)
    parts = [p for p in parts if p and p.lower() not in _GENERIC_NAMES]
    if not parts:
        return ''
    # Pick the longest non-generic chunk
    parts.sort(key=len, reverse=True)
    name = parts[0].strip()
    # Drop trailing site-suffix words
    name = re.sub(r'\s+(?:official\s+site|home\s*page|website)\s*$', '', name, flags=re.IGNORECASE).strip()
    if 2 < len(name) < 80 and name.lower() not in _GENERIC_NAMES:
        return name
    return ''


# STRICT copyright: © year(s) NAME — name stops at period, "all rights", or "Inc/LLC/Corp"
_COPYRIGHT_STRICT = re.compile(
    r'(?:©|\(c\)|copyright)\s*'
    r'(?:\d{4}(?:\s*[-–]\s*\d{2,4})?)?\s*'
    r'(?:[-–]\s*)?'
    r'([A-Z][A-Za-z0-9&\.\' \-]{2,79}?)'
    r'(?=\s*(?:\.|,|\s+all\s+rights\s+reserved|$))',
    re.IGNORECASE,
)

# Generic abbreviations that ARE NOT the real business name
_ABBREV_NAMES = {'scs', 'llc', 'inc', 'corp', 'ltd', 'co', 'plc', 'gmbh', 'sa'}


# ── Owner / leadership name extraction ─────────────────────────────────────

_OWNER_TITLES = (
    'owner', 'co-owner', 'co owner', 'founder', 'co-founder', 'co founder',
    'president', 'ceo', 'chief executive officer', 'managing director',
    'managing partner', 'principal', 'proprietor', 'director',
)

# "Owner Jana Higgins" / "Owner: Jana Higgins" / "Founded by Jana Higgins"
_OWNER_PATTERNS_TITLE_FIRST = re.compile(
    r'\b(?:'
    r'owner|co-?owner|founder|co-?founder|president|ceo|'
    r'chief\s+executive\s+officer|managing\s+director|managing\s+partner|'
    r'principal|proprietor'
    r')(?:\s+is|\s*:|\s*[-–]|\s+by)?\s+'
    r'([A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+){1,2})'  # First M. Last (1-3 words)
    r'\b',
    re.IGNORECASE,
)

# "Jana Higgins, Owner" / "Jana Higgins — Founder" / "Jana Higgins, CEO"
_OWNER_PATTERNS_TITLE_AFTER = re.compile(
    r'\b([A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+){1,2})\b'
    r'\s*[,\-–]\s*'
    r'(' + '|'.join(re.escape(t) for t in _OWNER_TITLES) + r')\b',
    re.IGNORECASE,
)

# "Meet our owner, Jane Smith" / "Our founder Jane Smith"
_OWNER_PATTERNS_MEET = re.compile(
    r'\b(?:meet\s+our|our|the)\s+'
    r'(?:' + '|'.join(re.escape(t) for t in _OWNER_TITLES) + r')'
    r'\s*[,:]?\s+'
    r'([A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+){1,2})\b',
    re.IGNORECASE,
)

# "Founded in 2010 by Jane Smith"
_OWNER_PATTERNS_FOUNDED = re.compile(
    r'\bfounded\s+(?:in\s+\d{4}\s+)?by\s+'
    r'([A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+){1,2})\b',
    re.IGNORECASE,
)

_NAME_BLACKLIST = {
    # Generic / pronoun-y phrases that LOOK like names but aren't
    'our team', 'our staff', 'our family', 'our company', 'our business',
    'all rights', 'rights reserved', 'click here', 'read more', 'learn more',
    'sign up', 'log in', 'home page', 'contact us', 'about us',
    'main street', 'first name', 'last name', 'full name',
    'united states', 'new york', 'los angeles', 'san francisco',
    'east coast', 'west coast',
}


_PRECEDING_NAME_COMMA = re.compile(r'[A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\s*,\s*$')


def _extract_owner_info(pages):
    """Find owner/founder/leadership names + titles from any page.
    Returns a list of {name, title, source_url} dicts, deduped by name.

    Patterns run in priority order — most reliable first. Comma-form
    "Name, Title" wins over "Title Name" form, because the latter is often a
    misread of a comma-separated staff list:
        "Frank Hawbolt, Owner Jana Higgins, Production Manager"
                       ^^^^^                ← "Owner" is Frank's title,
                                              not Jana's prefix.
    """
    found_by_name = {}
    captured_names = set()

    def _snippet(text, start, end, radius=60):
        """Return the raw text around a match — useful for owner confirmation."""
        s = max(0, start - radius)
        e = min(len(text), end + radius)
        out = text[s:e].strip()
        if s > 0: out = '...' + out
        if e < len(text): out = out + '...'
        return _clean(out)

    for page in pages or []:
        text = page.get('text', '') or ''
        url = page.get('url', '')

        # Pass 1 (HIGH): "Name, Title"
        for m in _OWNER_PATTERNS_TITLE_AFTER.finditer(text):
            name = _clean(m.group(1))
            title = _clean(m.group(2))
            if name.lower() not in captured_names:
                _add_owner(found_by_name, name, title, url,
                           confidence='high',
                           raw_snippet=_snippet(text, m.start(), m.end()))
                captured_names.add(name.lower())

        # Pass 2 (HIGH): "Meet our owner, Jane Smith"
        for m in _OWNER_PATTERNS_MEET.finditer(text):
            name = _clean(m.group(1))
            title_match = re.search(
                r'(?:' + '|'.join(re.escape(t) for t in _OWNER_TITLES) + r')',
                m.group(0), re.IGNORECASE
            )
            title = title_match.group(0) if title_match else 'owner'
            if name.lower() not in captured_names:
                _add_owner(found_by_name, name, title, url,
                           confidence='high',
                           raw_snippet=_snippet(text, m.start(), m.end()))
                captured_names.add(name.lower())

        # Pass 3 (HIGH): "Founded in 2010 by Jane Smith"
        for m in _OWNER_PATTERNS_FOUNDED.finditer(text):
            name = _clean(m.group(1))
            if name.lower() not in captured_names:
                _add_owner(found_by_name, name, 'founder', url,
                           confidence='high',
                           raw_snippet=_snippet(text, m.start(), m.end()))
                captured_names.add(name.lower())

        # Pass 4 (MEDIUM): "Owner Name" — title-first, skip if preceded by "Name, "
        for m in _OWNER_PATTERNS_TITLE_FIRST.finditer(text):
            preceding = text[max(0, m.start() - 60):m.start()]
            if _PRECEDING_NAME_COMMA.search(preceding):
                continue
            name = _clean(m.group(1))
            if name.lower() in captured_names:
                continue
            title = m.group(0).split(m.group(1))[0].strip(' :-—').strip()
            _add_owner(found_by_name, name, title, url,
                       confidence='medium',
                       raw_snippet=_snippet(text, m.start(), m.end()))
            captured_names.add(name.lower())

    return list(found_by_name.values())


# Words that signal "this is an organization, not a person"
_ORG_WORDS = {
    'corp', 'corporation', 'inc', 'incorporated', 'llc', 'ltd', 'limited',
    'company', 'group', 'agency', 'studio', 'partners', 'associates',
    'enterprises', 'systems', 'technologies', 'solutions', 'industries',
    'services', 'institute', 'foundation', 'association', 'society',
    'chapter', 'council', 'committee', 'department', 'division',
    'office', 'bureau', 'authority', 'commission', 'board',
    'international', 'national', 'regional', 'global', 'worldwide',
    'operations', 'manager', 'director',
    # State / place suffixes that flag orgs
    'nevada', 'california', 'florida', 'texas', 'oregon',
    # Acronymish words common in org names
    'agc', 'aia', 'naacp', 'nfl', 'mlb', 'usa', 'inc',
}


def _add_owner(store: dict, name: str, title: str, source_url: str,
               confidence: str = 'medium', raw_snippet: str = ''):
    """Internal: dedup-and-add an owner record. Filters out obvious orgs.
    Tracks confidence ('high'|'medium'|'low') + the raw text snippet where
    the match was found, so onboarding can show it to the owner for confirmation."""
    if not name:
        return
    low = name.lower()
    if low in _NAME_BLACKLIST:
        return
    words = name.split()
    if len(words) < 2 or len(words) > 4:
        return
    if any(w.lower().strip(',.') in _ORG_WORDS for w in words):
        return
    for w in words:
        clean = w.strip(',.')
        if len(clean) >= 2 and clean.isupper():
            return
    for w in words:
        clean = w.strip(',.')
        if len(clean) == 1 and clean.isupper() and not w.endswith('.'):
            return
    priority = ['owner', 'co-owner', 'co owner', 'founder', 'co-founder', 'co founder',
                'president', 'ceo', 'managing director', 'principal', 'proprietor']
    new_priority = next((i for i, p in enumerate(priority) if p in title.lower()), 999)
    existing = store.get(name.lower())
    if existing:
        existing_priority = next((i for i, p in enumerate(priority) if p in existing['title'].lower()), 999)
        if new_priority < existing_priority:
            existing['title'] = title.lower()
        # Always upgrade confidence to highest seen
        if confidence == 'high' or existing.get('confidence') == 'medium' and confidence == 'high':
            existing['confidence'] = 'high'
        if raw_snippet and not existing.get('raw_snippet'):
            existing['raw_snippet'] = raw_snippet
    else:
        store[name.lower()] = {
            'name': name,
            'title': title.lower(),
            'source_url': source_url,
            'confidence': confidence,
            'raw_snippet': raw_snippet,
        }


def _extract_copyright_names(pages):
    """Pull business name candidates from footer copyright lines.
    Strict pattern — skips obvious abbreviations like just 'SCS'."""
    candidates = []
    for page in pages or []:
        text = page.get('text', '') or ''
        for m in _COPYRIGHT_STRICT.finditer(text):
            name = _clean(m.group(1))
            name = re.sub(r'[.\s,\-–]+$', '', name)
            if not name or len(name) < 4:
                continue
            # Skip generic abbreviations ALONE
            if name.lower() in _ABBREV_NAMES:
                continue
            # Skip if it's all uppercase and short (likely abbrev)
            if name.isupper() and len(name) <= 4:
                continue
            candidates.append(name)
    return _unique(candidates)


# ── Body-text name discovery — find the FULL company name mentioned in content ─

# Common company-name suffix words
_NAME_SUFFIX_WORDS = {
    'source', 'group', 'services', 'company', 'co', 'corp', 'corporation',
    'inc', 'incorporated', 'llc', 'ltd', 'limited', 'agency', 'studio', 'shop',
    'works', 'partners', 'associates', 'enterprises', 'systems', 'technology',
    'technologies', 'solutions', 'industries', 'firm', 'consulting',
    'restaurant', 'cafe', 'bakery', 'salon', 'clinic', 'center', 'institute',
}


# Words that mark a phrase as nav/menu/CTA — NOT a business name
_NAV_PHRASE_WORDS = {
    'contact', 'subscribe', 'view', 'see', 'get', 'click', 'why', 'use', 'how',
    'home', 'menu', 'cart', 'login', 'signup', 'register', 'order', 'find',
    'learn', 'read', 'shop', 'browse', 'search', 'follow', 'join', 'become',
    'meet', 'discover', 'explore', 'check', 'try', 'request', 'download',
    'next', 'prev', 'previous', 'back', 'forward', 'more', 'all',
    'image', 'photo', 'video', 'logo',  # asset labels
}


def _extract_body_name_candidates(pages, full_text):
    """Find capitalized multi-word phrases that look like business names.
    Rejects nav-menu concatenations and CTAs."""
    candidates = {}
    pages_seen_in = {}  # phrase → set of urls (to detect "appears on every page = nav")

    pat = re.compile(
        r'\b('
        r"[A-Z][a-zA-Z]+(?:['’]s)?"                    # First word, optional 's
        r'(?:\s+[A-Z&][a-zA-Z&]+(?:[\.\-][A-Z]?[a-zA-Z]*)?)'  # 2nd word (required)
        r'(?:\s+[A-Z][a-zA-Z]+){0,3}'                  # 0-3 more words
        r')\b'
    )

    for page in pages or []:
        url = page.get('url', '')
        page_text = page.get('text', '') or ''
        for m in pat.finditer(page_text):
            phrase = m.group(1).strip()
            low = phrase.lower()
            words = low.split()

            # REJECT: starts with article/pronoun
            if words[0] in ('the','this','that','these','those','we','our',
                             'your','my','i','you','they','their','his','her'):
                continue
            # REJECT: contains any nav/CTA word
            if any(w.strip(",.'’") in _NAV_PHRASE_WORDS for w in words):
                continue
            # REJECT: known boilerplate
            if any(bad in low for bad in (
                'rights reserved', 'all rights', 'home page', 'site map',
                'privacy policy', 'terms of service', 'cookie policy',
                'log in', 'sign up',
            )):
                continue
            # REQUIRE: at least 2 words AND (has suffix OR 3+ words OR possessive)
            if len(words) < 2:
                continue
            has_suffix = any(w.strip(",.'’") in _NAME_SUFFIX_WORDS for w in words)
            is_long = len(words) >= 3
            has_possessive = "'s" in phrase or "’s" in phrase
            if not (has_suffix or is_long or has_possessive):
                continue

            candidates[phrase] = candidates.get(phrase, 0) + 1
            pages_seen_in.setdefault(phrase, set()).add(url)

    # Penalty for phrases that appear on every page (probably nav/footer)
    total_pages = max(1, len(pages or []))
    ranked = []
    for phrase, count in candidates.items():
        page_count = len(pages_seen_in.get(phrase, set()))
        ubiquity = page_count / total_pages
        if ubiquity > 0.75 and total_pages > 2:
            continue
        has_suffix = any(w.strip(",.'’") in _NAME_SUFFIX_WORDS for w in phrase.lower().split())
        score = count + (5 if has_suffix else 0) + (len(phrase.split()) - 2)
        ranked.append((score, phrase))
    ranked.sort(reverse=True)
    raw = [p for _, p in ranked]

    # ── Sub-phrase extraction: find 2-4 word clean sub-phrases that:
    #    (a) end with a _NAME_SUFFIX_WORDS suffix
    #    (b) start with a Title-Case word that's NOT a generic
    #    (c) appear as a substring in multiple raw candidates
    # This catches "Sierra Contractors Source" when the raw phrase is
    # "SUBSCRIPTION INFORMATION Sierra Contractors Source".
    sub_counts = {}
    for raw_phrase in raw:
        words = raw_phrase.split()
        # Try every 2-4 word window
        for size in (4, 3, 2):
            for i in range(len(words) - size + 1):
                window = words[i:i+size]
                first, last = window[0], window[-1]
                last_clean = last.strip(",.'’").lower()
                if last_clean not in _NAME_SUFFIX_WORDS:
                    continue
                if first.isupper() and len(first) > 4:   # ALL-CAPS like "SUBSCRIPTION" → skip
                    continue
                if not first[0].isupper():
                    continue
                if first.lower() in {'and', 'or', 'the', 'a', 'an', 'of', 'for', 'at', 'by', 'in'}:
                    continue
                sub = ' '.join(window)
                sub_counts[sub] = sub_counts.get(sub, 0) + 1

    # If any sub-phrase appears 2+ times, prefer it as the primary candidate.
    # Tie-break: longer phrase wins (more specific is better).
    repeated_subs = [(c, s) for s, c in sub_counts.items() if c >= 2]
    repeated_subs.sort(key=lambda cs: (-cs[0], -len(cs[1].split()), -len(cs[1])))

    # Subsumption: drop short sub-phrases when a longer form is present.
    # E.g., if "Sierra Contractors Source" is in the list, drop "Contractors Source".
    cleaner_raw = [s for _, s in repeated_subs]
    cleaner = []
    for phrase in cleaner_raw:
        is_subsumed = any(
            phrase != longer and phrase in longer
            for longer in cleaner_raw
            if len(longer.split()) > len(phrase.split())
        )
        if not is_subsumed:
            cleaner.append(phrase)

    # Final order: cleaner sub-phrases first, then raw
    return cleaner + [r for r in raw if r not in cleaner]


def _from_meta(pages):
    """Get name/description from <meta> tags + <title> + first H1."""
    found = {}
    for page in pages or []:
        meta = page.get('meta', {}) or {}
        title = page.get('title', '')
        headings = page.get('headings', []) or []

        if 'name' not in found:
            candidates = []
            # Try most specific sources first
            for key in ('og:site_name', 'application-name', 'twitter:site'):
                v = meta.get(key, '').strip()
                if v: candidates.append(v)
            for key in ('og:title', 'twitter:title'):
                v = meta.get(key, '').strip()
                if v: candidates.append(v)
            if title:
                candidates.append(title)
            # First H1 on homepage is often the business name
            for h in headings:
                if h.get('level') == 1:
                    t = _clean(h.get('text', ''))
                    if t: candidates.append(t)
                    break
            for cand in candidates:
                cleaned = _clean_name(cand)
                if cleaned:
                    found['name'] = cleaned
                    break

        if 'description' not in found:
            for key in ('description', 'og:description', 'twitter:description'):
                v = meta.get(key, '').strip()
                if v and len(v) > 30:
                    found['description'] = v
                    break
            # Fallback to longest H2 if no meta description
            if 'description' not in found:
                h2s = [_clean(h.get('text', '')) for h in headings if h.get('level') == 2]
                long_h2 = [h for h in h2s if 40 < len(h) < 300]
                if long_h2:
                    found['description'] = max(long_h2, key=len)
    return found


def _extract_phones(text):
    return _unique(re.findall(r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', text))


def _extract_emails(text):
    raw = re.findall(r'[\w\.+\-]+@[\w\.-]+\.[a-zA-Z]{2,}', text)
    # Filter out junk emails (1px tracking, sentry, etc.)
    return _unique([e for e in raw if not any(
        bad in e.lower() for bad in ('sentry.io', 'wixpress.com', 'example.com', 'noreply')
    )])


def _extract_addresses(text):
    """US-style street + city + state + zip extraction.
    Strict: requires ZIP code anchor so we don't run into navigation text."""
    state_pat = '|'.join(US_STATES.keys())
    # Anchored pattern — must end with state + 5-digit zip (with optional +4)
    # Street type is REQUIRED. City is required between street and state.
    pat = re.compile(
        r'\b(\d{2,6}\s+'                                       # street number
        r'(?:[A-Z][a-zA-Z0-9.\-]*\s+){1,5}'                    # street name (1-5 capitalized words)
        r'(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|'
        r'Lane|Ln|Way|Court|Ct|Place|Pl|Plaza|Pkwy|Parkway|'
        r'Highway|Hwy|Circle|Cir|Terrace|Ter|Trail|Trl)\.?'    # street type (required)
        r'(?:\s+(?:Suite|Ste|Unit|Apt|#)\s*\d+\w*)?'           # optional suite
        r',?\s+'
        r'([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)'              # city (1-3 capitalized words)
        r',?\s+'
        r'(' + state_pat + r')\s+'                              # state code
        r'(\d{5}(?:-\d{4})?))',                                 # zip (required anchor)
    )
    found = []
    for m in pat.finditer(text):
        full = _clean(m.group(0))
        if len(full) < 200:  # sanity cap
            found.append(full)
    return _unique(found)[:5]


def _extract_hours(text):
    """Find hours strings in the text using patterns."""
    found = []
    for pat in _HOURS_PATTERNS:
        for m in pat.findall(text):
            if isinstance(m, tuple):
                m = ' '.join(str(x) for x in m if x)
            found.append(_clean(m))
    return _unique(found)[:6]


def _classify_business_type(text, jsonld, pages=None):
    """Pick best business type from schema.org → meta description → keyword matches."""
    pages = pages or []
    # Schema.org first
    for item in jsonld or []:
        if isinstance(item, dict):
            ty = item.get('@type', '')
            ty_l = (ty if isinstance(ty, str) else
                    ' '.join(ty) if isinstance(ty, list) else '').lower()
            if 'restaurant' in ty_l: return 'Restaurant'
            if 'medicalbusiness' in ty_l: return 'Medical Office'
            if 'autorepair' in ty_l: return 'Auto Repair'
            if 'plumber' in ty_l: return 'Plumbing'
            if 'electrician' in ty_l: return 'Electrician'
            if 'beautysalon' in ty_l or 'hairsalon' in ty_l: return 'Salon / Spa'
            if 'lodgingbusiness' in ty_l or 'hotel' in ty_l: return 'Hotel / Lodging'
            if 'realestateagent' in ty_l: return 'Real Estate'
            if 'legalservice' in ty_l: return 'Law Firm'
            if 'gym' in ty_l or 'sportsactivitylocation' in ty_l: return 'Fitness / Gym'

    # Try meta description on first page (highly reliable for SaaS/AI)
    if pages:
        meta = pages[0].get('meta', {}) or {}
        title_low = (pages[0].get('title', '') or '').lower()
        desc_low = (meta.get('description', '') or meta.get('og:description', '')).lower()
        meta_text = (title_low + ' ' + desc_low).lower()
        # Strong meta-level signals
        if any(p in meta_text for p in ['ai-powered', 'artificial intelligence', 'personal ai',
                                         'ai companion', 'ai assistant', 'ai platform']):
            return 'AI / Tech'
        if any(p in meta_text for p in ['saas', 'cloud software', 'software platform',
                                         'subscribe to our service', 'free trial']):
            return 'SaaS / Software'

    # Keyword fallback — require AT LEAST 2 keyword matches per category
    # so a single nav word like "menu" doesn't misclassify the whole site
    low = text.lower()
    scored = []
    for label, keywords in BUSINESS_TYPE_KEYWORDS:
        # Count how many DIFFERENT keywords match
        matches = sum(1 for k in keywords if k in low)
        if matches >= 2:
            scored.append((matches, label))
    if scored:
        scored.sort(reverse=True)
        return scored[0][1]
    # Single-match categories — only return if the match is highly specific
    # (long phrases, not single common words)
    for label, keywords in BUSINESS_TYPE_KEYWORDS:
        for k in keywords:
            if len(k) >= 12 and k in low:  # only specific, multi-word phrases
                return label
    return 'General Business'


_NAV_NOISE = {
    # Generic nav
    'home', 'about', 'about us', 'contact', 'contact us', 'services', 'products',
    'menu', 'log in', 'login', 'sign up', 'signup', 'cart', 'search', 'blog',
    'next', 'previous', 'read more', 'learn more', 'go back', 'subscribe',
    'get started', 'see all', 'view all', 'click here', 'find out more',
    'our services', 'why use us', 'partnerships', 'advertising', 'print shop',
    # Common footer/legal links (NOT services)
    'privacy policy', 'terms of service', 'terms', 'terms and conditions', 'refund policy',
    'cookies', 'cookie policy', 'eula', 'license agreement', 'license', 'dmca',
    'gdpr', 'disclaimer', 'sitemap', 'careers', 'press', 'media',
    'faq', 'faqs', 'help', 'support', 'documentation',
    # Common SaaS marketing labels (these are sections, not services)
    'features', 'pricing', 'testimonials', 'reviews', 'case studies', 'integrations',
    # Common content boilerplate (TOS/Privacy section headings)
    'you own the software', 'you are at least 18 years of age',
    'you understand that this is a legally binding agreement',
    'you are authorized to enter into this agreement',
    'you are authorized to enter into this agreement on behalf of yourself or your organization',
}

# Phrases that indicate LEGAL boilerplate, not a service
_LEGAL_PHRASES = (
    'damages', 'liability', 'disclaim', 'warranty', 'warranties', 'governing law',
    'class action', 'arbitration', 'indemnif', 'license grant', 'prohibited use',
    'changes to terms', 'limitation of liab', 'no warranty', 'as-is', 'as is, where is',
    'consequential', 'incidental', 'punitive damages', 'loss of profits', 'loss of data',
    'medical, legal, financial', 'tax position', 'cpa certification', 'gdpr',
    'authorized to enter into', 'legally binding', '30-day money-back',
    'refund policy', 'third-party services', 'changes to these terms', 'class action waiver',
    'ai responses vary', 'taking or refraining', 'business interruption',
    'starting, stopping', 'using any orby-generated', 'relying on any',
    'acting on any symptom', 'investment or retirement',
)

_SERVICE_PAGE_HINTS = (
    '/service', '/product', '/menu', '/treatment', '/program', '/what-we-do',
    '/offering', '/specialt', '/expertise', '/practice-area', '/solution',
    '/work', '/portfolio', '/care',
)

# Pages we should NEVER pull services from
_NON_SERVICE_PAGE_HINTS = (
    '/legal', '/terms', '/tos', '/privacy', '/cookie', '/refund', '/return-policy',
    '/disclaimer', '/copyright', '/gdpr', '/eula', '/license', '/dmca',
)


def _looks_like_service(text):
    """Is this list-item text likely to be a service vs nav junk vs legal?"""
    t = _clean(text)
    low = t.lower()
    if 3 >= len(t) or len(t) >= 150:
        return False
    if low in _NAV_NOISE:
        return False
    word_count = len(t.split())
    if word_count > 18 or word_count < 1:
        return False
    if t.isupper() and word_count < 3:
        return False
    # Legal boilerplate is NOT a service
    if any(p in low for p in _LEGAL_PHRASES):
        return False
    # Sentences with verbs about consequences/disclaiming = legal
    if re.search(r'^(any|all|loss of|damages?|liability|use of|providing|relying)', low):
        return False
    return True


def _extract_services(pages, jsonld):
    """Pull service names from:
       1) JSON-LD hasOfferCatalog / makesOffer
       2) List items on service-ish URLs
       3) H2/H3 headings on service-ish URLs OR pages whose title mentions services
       4) List items on the homepage that look like services (NOT in _NAV_NOISE)"""
    services = set()
    # 1) JSON-LD offers
    for item in jsonld or []:
        if not isinstance(item, dict):
            continue
        offers = item.get('hasOfferCatalog') or item.get('makesOffer') or []
        if isinstance(offers, dict):
            offers = [offers]
        for off in offers:
            if isinstance(off, dict):
                name = off.get('name') or (off.get('itemOffered') or {}).get('name', '')
                if name:
                    services.add(_clean(name))

    # 2 + 3) Service-page list items + headings
    for page in pages or []:
        url = (page.get('url') or '').lower()
        title = (page.get('title') or '').lower()
        # Exclude legal/terms/privacy pages — they're not services
        if any(seg in url for seg in _NON_SERVICE_PAGE_HINTS):
            continue
        if any(seg in title for seg in ('terms', 'privacy', 'legal', 'refund', 'license agreement')):
            continue
        is_service_page = (
            any(seg in url for seg in _SERVICE_PAGE_HINTS) or
            any(seg in title for seg in ('service', 'product', 'what we do', 'offering', 'menu'))
        )
        if is_service_page:
            for li in page.get('list_items') or []:
                if _looks_like_service(li):
                    services.add(_clean(li))
            for h in page.get('headings') or []:
                if 2 <= h.get('level', 0) <= 3:
                    text = _clean(h.get('text', ''))
                    if _looks_like_service(text):
                        services.add(text)

    # 4) Homepage list items that look like services
    if pages:
        for li in pages[0].get('list_items') or []:
            if _looks_like_service(li):
                # Be stricter on homepage — must be 2+ words or have noun-ish quality
                if len(li.split()) >= 2 and not li.lower().startswith(('learn', 'read', 'click', 'view')):
                    services.add(_clean(li))

    return _unique(list(services))[:30]


def _extract_faqs(pages, jsonld):
    """Find FAQ Q&A pairs. JSON-LD FAQPage first, then heuristic from /faq pages."""
    faqs = []
    # JSON-LD
    jsonld_data = _from_jsonld(jsonld)
    if jsonld_data.get('faqs'):
        faqs.extend(jsonld_data['faqs'])
    # Heuristic: H3 followed by paragraph on /faq pages
    for page in pages or []:
        url = (page.get('url') or '').lower()
        if '/faq' not in url and 'faq' not in url:
            continue
        # Use list items + headings as questions
        for h in page.get('headings') or []:
            text = _clean(h.get('text', ''))
            if text.endswith('?') and 10 < len(text) < 250:
                faqs.append({'question': text, 'answer': '(see /faq page for full answer)'})
    # Dedupe
    seen, out = set(), []
    for f in faqs:
        key = f.get('question', '').lower().strip()
        if key and key not in seen:
            seen.add(key)
            out.append(f)
    return out[:20]


def _extract_service_area(text):
    """Find geographic indicators — city/state mentions."""
    cities_found = []
    common_cities = ['Reno', 'Sparks', 'Carson City', 'Las Vegas', 'Tahoe', 'Truckee',
                     'San Francisco', 'Los Angeles', 'Sacramento', 'Portland', 'Seattle',
                     'New York', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
                     'Boston', 'Dallas', 'Austin', 'Denver', 'Miami']
    for city in common_cities:
        if re.search(r'\b' + re.escape(city) + r'\b', text):
            cities_found.append(city)
    # Look for "serving X" / "X area" phrases
    for m in re.finditer(r'(?:serving|servicing|based in|located in)\s+(?:the\s+)?([A-Z][A-Za-z\s/]{3,40}(?:area|region|county|valley)?)',
                         text):
        area = _clean(m.group(1))
        if area:
            cities_found.append(area)
    return _unique(cities_found)[:5]


def _extract_social(pages):
    """Aggregate social links across all pages."""
    out = {}
    for page in pages or []:
        for net, url in (page.get('social_links') or {}).items():
            if net not in out:
                out[net] = url
    return out


def _domain_to_name(domain):
    """Last-resort business name guess from the domain."""
    if not domain:
        return ''
    base = domain.split('.')[0]
    base = base.replace('-', ' ').replace('_', ' ')
    return ' '.join(w.capitalize() for w in base.split())


# ── Main extractor ─────────────────────────────────────────────────────────

class BusinessDataExtractor:
    """Public API. Used by SiteScraper."""

    def extract(self, full_text: str, pages: list | None = None) -> dict:
        """Build a business_profile draft from scraped data.
        full_text is the combined text of all pages, pages is the per-page list."""
        pages = pages or []
        full_text = _clean(full_text)
        data = {}

        # 1) JSON-LD has the highest signal — use it first
        jsonld = _gather_jsonld(pages)
        jl = _from_jsonld(jsonld)
        if jl.get('name'):        data['name'] = jl['name']
        if jl.get('description'): data['description'] = jl['description']

        # 1b) Find the TRUE company name from:
        #     (a) footer copyright (strict pattern, skips abbreviations)
        #     (b) body-text frequency analysis (most-mentioned formal phrase)
        if 'name' not in data:
            copy_names = _extract_copyright_names(pages)        # e.g. "Joe's Plumbing, Inc"
            body_names = _extract_body_name_candidates(pages, full_text)  # frequency-ranked
            # Prefer copyright if present (most authoritative)
            # If copyright gave just an abbreviation, look in body for a full version
            chosen = None
            if copy_names:
                chosen = copy_names[0]
                # If copyright looks like an abbreviation (e.g. "SCS"), see if body
                # has a full multi-word form that starts the same way
                if len(chosen.split()) == 1 and len(chosen) <= 5:
                    for bn in body_names:
                        if bn.split()[0].lower().startswith(chosen.lower()[:3]):
                            chosen = bn
                            break
            elif body_names:
                chosen = body_names[0]
            if chosen:
                data['name'] = chosen
                data['legal_entity'] = chosen
                if body_names:
                    data['name_candidates'] = body_names[:5]

        contact = {}
        if jl.get('phone'):   contact['phones'] = [jl['phone']]
        if jl.get('email'):   contact['emails'] = [jl['email']]
        if jl.get('address'): contact['addresses'] = [jl['address']]

        if jl.get('hours'):   data['hours'] = jl['hours']
        if jl.get('social'):  data['social_links'] = jl['social']
        if jl.get('faqs'):    data['faqs'] = jl['faqs']

        # 2) Meta/<title> fills business name + description if JSON-LD didn't
        meta = _from_meta(pages)
        if 'name' not in data and meta.get('name'):
            data['name'] = meta['name']
        if 'description' not in data and meta.get('description'):
            data['description'] = meta['description']

        # 3) Contact details from full text (catches what JSON-LD misses)
        phones = _extract_phones(full_text)
        if phones:
            contact.setdefault('phones', [])
            for p in phones:
                if p not in contact['phones']:
                    contact['phones'].append(p)
        emails = _extract_emails(full_text)
        if emails:
            contact.setdefault('emails', [])
            for e in emails:
                if e not in contact['emails']:
                    contact['emails'].append(e)
        addresses = _extract_addresses(full_text)
        if addresses:
            contact.setdefault('addresses', [])
            for a in addresses:
                if a not in contact['addresses']:
                    contact['addresses'].append(a)
        if contact:
            data['contact'] = contact

        # 4) Hours from text patterns (catches what JSON-LD misses)
        if 'hours' not in data:
            hours = _extract_hours(full_text)
            if hours:
                data['hours'] = hours[0] if len(hours) == 1 else hours

        # 5) Services from /services pages + JSON-LD offers
        services = _extract_services(pages, jsonld)
        if services:
            data['services'] = services

        # 6) FAQs from /faq pages + JSON-LD FAQPage
        if 'faqs' not in data:
            faqs = _extract_faqs(pages, jsonld)
            if faqs:
                data['faqs'] = faqs

        # 7) Service area (geographic)
        service_area = _extract_service_area(full_text)
        if service_area:
            data['service_area'] = service_area

        # 8) Business type (industry classification)
        data['business_type'] = _classify_business_type(full_text, jsonld, pages)

        # 8b) Owner / leadership names + titles
        owners = _extract_owner_info(pages)
        if owners:
            # Sort: confidence (high first), then "owner"-title bias, then position
            def _owner_sort_key(o):
                conf_rank = {'high': 0, 'medium': 1, 'low': 2}.get(o.get('confidence', 'medium'), 1)
                owner_bias = 0 if 'owner' in o.get('title', '').lower() else 1
                return (conf_rank, owner_bias)
            owners_sorted = sorted(owners, key=_owner_sort_key)
            primary = owners_sorted[0]
            data['owner_name'] = primary['name']
            data['owner_title'] = primary['title']
            data['owner_confidence'] = primary.get('confidence', 'medium')
            data['owner_raw_snippet'] = primary.get('raw_snippet', '')
            data['leadership'] = owners_sorted

        # 9) Social links from all pages
        if 'social_links' not in data:
            social = _extract_social(pages)
            if social:
                data['social_links'] = social

        # 10) Fallback business name from domain if all else failed
        if 'name' not in data and pages:
            first_url = pages[0].get('url', '')
            host = urlparse(first_url).netloc.replace('www.', '')
            data['name'] = _domain_to_name(host)

        # 11) Page count + crawl stats
        data['_pages_scraped'] = len(pages)
        data['_internal_link_count'] = sum(len(p.get('internal_links') or []) for p in pages)

        return data
