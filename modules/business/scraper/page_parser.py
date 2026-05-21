"""Page parser — extracts structured signals from a single HTML page.

Returns:
  url, text, text_preview, length, images,
  title, meta (dict), headings (list), list_items (list),
  jsonld (list of dicts), internal_links (list), social_links (dict)
"""
import json
import re
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse


_BLOCK_TAGS = {'script', 'style', 'noscript'}


class _StructuredParser(HTMLParser):
    """Pulls visible text, title, meta tags, headings, list items, and JSON-LD blocks."""

    _VISIBLE_ATTRS = ("alt", "title", "aria-label", "placeholder")

    def __init__(self):
        super().__init__()
        self.parts = []
        self.title = ''
        self.meta = {}             # name/property → content
        self.headings = []         # [(level, text), ...]
        self.list_items = []       # plain text bullets
        self.jsonld = []           # parsed JSON-LD blocks
        self.links = []            # (href, text)
        self._skip_depth = 0
        self._in_title = False
        self._in_heading = None    # 'h1' / 'h2' / 'h3'
        self._heading_buf = []
        self._in_li = False
        self._li_buf = []
        self._in_script_type = None  # for jsonld capture

    def handle_starttag(self, tag, attrs):
        tag_l = tag.lower()
        attr_map = {str(k).lower(): str(v) for k, v in attrs}

        if tag_l in _BLOCK_TAGS:
            if tag_l == 'script' and attr_map.get('type', '').lower() == 'application/ld+json':
                self._in_script_type = 'jsonld'
                # Don't skip — we want the content
                return
            self._skip_depth += 1
            return

        if tag_l == 'title':
            self._in_title = True
            return

        if tag_l == 'meta':
            name = attr_map.get('name', '') or attr_map.get('property', '') or attr_map.get('itemprop', '')
            content = attr_map.get('content', '')
            if name and content:
                self.meta[name.lower()] = content.strip()
            return

        if tag_l in ('h1', 'h2', 'h3'):
            self._in_heading = tag_l
            self._heading_buf = []
            return

        if tag_l == 'li':
            self._in_li = True
            self._li_buf = []
            return

        if tag_l == 'a':
            href = attr_map.get('href', '').strip()
            if href:
                self.links.append({'href': href, 'text': ''})  # text filled when we see the data
            return

        # Visible attribute text (alt/title/aria-label/placeholder)
        for key in self._VISIBLE_ATTRS:
            value = attr_map.get(key, '').strip()
            if value:
                self.parts.append(value)
        if tag_l in ('button', 'input'):
            value = attr_map.get('value', '').strip()
            input_type = attr_map.get('type', '').strip().lower()
            if value and (tag_l == 'button' or input_type in ('button', 'submit', 'reset', 'image')):
                self.parts.append(value)

    def handle_endtag(self, tag):
        tag_l = tag.lower()
        if tag_l in _BLOCK_TAGS:
            if self._in_script_type == 'jsonld':
                self._in_script_type = None
                return
            if self._skip_depth > 0:
                self._skip_depth -= 1
            return
        if tag_l == 'title':
            self._in_title = False
            return
        if tag_l in ('h1', 'h2', 'h3') and self._in_heading == tag_l:
            text = ' '.join(self._heading_buf).strip()
            if text:
                self.headings.append({'level': int(tag_l[1]), 'text': text})
            self._in_heading = None
            self._heading_buf = []
            return
        if tag_l == 'li' and self._in_li:
            text = ' '.join(self._li_buf).strip()
            if text and 1 < len(text) < 300:
                self.list_items.append(text)
            self._in_li = False
            self._li_buf = []
            return

    def handle_data(self, data):
        if self._in_script_type == 'jsonld':
            raw = (data or '').strip()
            if raw:
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        self.jsonld.extend(parsed)
                    else:
                        self.jsonld.append(parsed)
                except Exception:
                    pass
            return
        if self._skip_depth > 0:
            return
        text = (data or '').strip()
        if not text:
            return
        if self._in_title:
            self.title += text
            return
        if self._in_heading:
            self._heading_buf.append(text)
        if self._in_li:
            self._li_buf.append(text)
        # Update last link's text if we're inside an <a>
        if self.links and isinstance(self.links[-1].get('text'), str) and not self.links[-1]['text']:
            # only safe-ish heuristic — proper would track <a> open/close
            pass
        self.parts.append(text)


class PageParser:

    def extract_images(self, url, html):
        images = set()
        for src in re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', html, re.I):
            images.add(urljoin(url, src))
        for srcset in re.findall(r'srcset=["\']([^"\']+)["\']', html, re.I):
            for part in srcset.split(','):
                u = part.strip().split(' ')[0]
                if u:
                    images.add(urljoin(url, u))
        for bg in re.findall(r'url\(([^)]+)\)', html, re.I):
            bg = bg.replace('"', '').replace("'", '').strip()
            if bg.startswith('http') or bg.startswith('/'):
                images.add(urljoin(url, bg))
        return list(images)

    def extract_text(self, html):
        p = _StructuredParser()
        try:
            p.feed(html)
            p.close()
        except Exception:
            pass
        text = ' '.join(p.parts)
        return re.sub(r'\s+', ' ', text).strip()

    def parse(self, url, html):
        """Full structured parse — text + title + meta + headings + list_items + jsonld + links."""
        p = _StructuredParser()
        try:
            p.feed(html)
            p.close()
        except Exception:
            pass
        text = re.sub(r'\s+', ' ', ' '.join(p.parts)).strip()

        # Resolve link hrefs to absolute, classify internal vs social
        internal_links = []
        social_links = {}
        base_host = urlparse(url).netloc.lower().replace('www.', '')
        social_hosts = {
            'facebook.com': 'facebook', 'fb.com': 'facebook',
            'twitter.com': 'twitter', 'x.com': 'twitter',
            'instagram.com': 'instagram',
            'linkedin.com': 'linkedin',
            'youtube.com': 'youtube', 'youtu.be': 'youtube',
            'tiktok.com': 'tiktok',
            'yelp.com': 'yelp',
            'pinterest.com': 'pinterest',
        }
        for link in p.links:
            href = link.get('href', '').strip()
            if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:'):
                continue
            absu = urljoin(url, href)
            host = urlparse(absu).netloc.lower().replace('www.', '')
            for sh, name in social_hosts.items():
                if host.endswith(sh):
                    if name not in social_links:
                        social_links[name] = absu
                    break
            else:
                if host == base_host:
                    internal_links.append(absu)

        return {
            'url': url,
            'text': text,
            'text_preview': text[:1000],
            'length': len(text),
            'title': p.title.strip(),
            'meta': p.meta,
            'headings': p.headings,
            'list_items': p.list_items,
            'jsonld': p.jsonld,
            'internal_links': list(dict.fromkeys(internal_links))[:80],
            'social_links': social_links,
            'images': self.extract_images(url, html),
        }

    # Back-compat for site_scraper.py
    def summarize_page(self, url, html):
        return self.parse(url, html)
