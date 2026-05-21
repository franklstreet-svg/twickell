import re
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse


class _HrefParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def _add_link(self, value: str):
        text = str(value or "").strip()
        if text:
            self.links.append(text)

    def _extract_js_links(self, text: str):
        source = str(text or "")
        patterns = (
            r"(?:window\.location(?:\.href|\.assign|\.replace)?|location\.href)\s*=\s*['\"]([^'\"]+)['\"]",
            r"(?:window\.location(?:\.href|\.assign|\.replace)?|location\.href)\s*\(\s*['\"]([^'\"]+)['\"]\s*\)",
            r"['\"](/[^'\"]+)['\"]",
        )
        for pattern in patterns:
            for match in re.findall(pattern, source, flags=re.I):
                self._add_link(match)

    def handle_starttag(self, tag, attrs):
        attr_map = dict(attrs)
        tag = tag.lower()

        for key in ("href", "action", "data-href", "formaction"):
            value = attr_map.get(key, "").strip()
            if value:
                self._add_link(value)

        if tag in {"a", "area", "link", "form", "iframe", "button", "input", "meta"}:
            onclick = attr_map.get("onclick", "")
            if onclick:
                self._extract_js_links(onclick)

        if tag in {"iframe", "frame"}:
            src = attr_map.get("src", "").strip()
            if src:
                self._add_link(src)

        if tag == "meta" and attr_map.get("http-equiv", "").strip().lower() == "refresh":
            content = attr_map.get("content", "")
            refresh_match = re.search(r"url\s*=\s*([^;]+)", content, flags=re.I)
            if refresh_match:
                self._add_link(refresh_match.group(1).strip().strip("'\""))


class LinkExtractor:
    def __init__(self):
        self.priority_keywords = [
            "service",
            "services",
            "about",
            "contact",
            "faq",
            "policy",
            "team",
            "staff",
            "location",
            "hours",
            "pricing",
            "book",
            "appointment",
            "shop",
            "product",
        ]

    def _normalize_url(self, base_url: str, href: str) -> str:
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            return ""

        path = parsed.path or "/"
        if path.endswith("/index.html") or path.endswith("/index.htm"):
            path = path[: -len("index.html")]
            if not path:
                path = "/"
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        normalized = parsed._replace(path=path, fragment="", params="").geturl()
        return normalized

    def _looks_like_asset(self, url: str) -> bool:
        path = urlparse(url).path.lower()
        return bool(re.search(r"\.(?:css|js|mjs|json|png|jpe?g|gif|webp|svg|ico|pdf|zip|tar|gz|mp4|mp3|wav)$", path))

    def extract_links(self, base_url: str, html: str) -> list[str]:
        parser = _HrefParser()
        parser.feed(html)

        cleaned = []
        base_domain = urlparse(base_url).netloc

        for href in parser.links:
            absolute = self._normalize_url(base_url, href)
            if not absolute:
                continue
            parsed = urlparse(absolute)

            if parsed.netloc != base_domain:
                continue

            if self._looks_like_asset(absolute):
                continue

            if absolute not in cleaned:
                cleaned.append(absolute)

        return cleaned

    def prioritize(self, links: list[str], limit: int = 12) -> list[str]:
        scored = []

        for link in links:
            text = link.lower()
            score = 0

            for keyword in self.priority_keywords:
                if keyword in text:
                    score += 1

            scored.append((score, link))

        scored.sort(key=lambda item: (-item[0], item[1]))

        ordered = [link for _, link in scored]
        return ordered[:limit]
