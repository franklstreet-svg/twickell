from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

LOCALES_DIR = Path(__file__).resolve().parent / "locales"
_DEFAULT = "en"

_LANG_RE = re.compile(r"^[a-zA-Z]{2}(-[a-zA-Z]{2,8})?$")

_cache: dict[str, dict[str, str]] = {}

def _load(lang: str) -> dict[str, str]:
    lang = lang.lower()
    if lang in _cache:
        return _cache[lang]

    path = LOCALES_DIR / f"{lang}.json"
    if not path.exists():
        base = lang.split("-")[0]
        path2 = LOCALES_DIR / f"{base}.json"
        if path2.exists():
            lang = base
            path = path2
        else:
            lang = _DEFAULT
            path = LOCALES_DIR / f"{_DEFAULT}.json"

    data = json.loads(path.read_text(encoding="utf-8"))
    _cache[lang] = {str(k): str(v) for k, v in data.items()}
    return _cache[lang]

def pick_lang(query_lang: str | None, accept_language: str | None) -> str:
    if query_lang:
        q = query_lang.strip().lower()
        if _LANG_RE.match(q):
            return q
    if accept_language:
        parts = [p.strip() for p in accept_language.split(",") if p.strip()]
        if parts:
            first = parts[0].split(";")[0].strip().lower()
            if _LANG_RE.match(first):
                return first
    return _DEFAULT

def t(lang: str, key: str, **vars: Any) -> str:
    table = _load(lang)
    template = table.get(key, key)
    try:
        return template.format(**vars)
    except Exception:
        return template

def msg(lang: str, key: str, **vars: Any) -> dict[str, Any]:
    return {"message_key": key, "message": t(lang, key, **vars), "lang_used": lang}
