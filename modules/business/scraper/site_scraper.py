from collections import deque
from urllib.parse import urlparse

from .http_client import SimpleHttpClient
from .link_extractor import LinkExtractor
from .page_parser import PageParser
from .extract_business_data import BusinessDataExtractor
from .save_to_website_data import build_scrape_result


class SiteScraper:
    def __init__(self, max_pages: int | None = 50):
        self.max_pages = max_pages
        self.http = SimpleHttpClient()
        self.links = LinkExtractor()
        self.parser = PageParser()
        self.extractor = BusinessDataExtractor()

    def _normalize_url(self, url: str) -> str:
        text = url.strip()
        if not text.startswith(("http://", "https://")):
            text = "https://" + text
        return text

    def _parent_directory_url(self, url: str) -> str | None:
        parsed = urlparse(url)
        path = parsed.path or "/"
        if path in {"", "/"}:
            return None
        if path.endswith("/"):
            return None
        if "/" not in path[1:]:
            return None
        parent = path.rsplit("/", 1)[0] + "/"
        return parsed._replace(path=parent, query="", fragment="", params="").geturl()

    def scrape(self, start_url: str) -> dict:
        root_url = self._normalize_url(start_url)

        first = self.http.fetch(root_url)
        if not first["ok"]:
            return {"ok": False, "error": first["error"]}

        pages = []
        combined_parts = []
        visited = set()
        queue = deque()

        root_page = self.parser.summarize_page(root_url, first["html"])
        root_discovered = self.links.extract_links(root_url, first["html"])
        root_page["ok"] = True
        root_page["status_code"] = first.get("status_code", 200)
        root_page["discovered_links"] = root_discovered
        root_page["crawl_depth"] = 0
        pages.append(root_page)
        if root_page.get("text"):
            combined_parts.append(root_page["text"])
        visited.add(root_url)

        for link in self.links.prioritize(root_discovered, limit=len(root_discovered) or 0):
            if link not in visited and link not in queue:
                queue.append(link)

        while queue and (self.max_pages is None or len(pages) < self.max_pages):
            url = queue.popleft()
            if url in visited:
                continue
            visited.add(url)

            result = self.http.fetch(url)
            if not result["ok"]:
                pages.append({
                    "url": url,
                    "ok": False,
                    "status_code": result.get("status_code", 0),
                    "error": result.get("error", ""),
                    "text": "",
                    "text_preview": "",
                    "length": 0,
                    "images": [],
                    "discovered_links": [],
                    "crawl_depth": len(pages),
                })
                continue

            page = self.parser.summarize_page(url, result["html"])
            discovered = self.links.extract_links(url, result["html"])
            page["ok"] = True
            page["status_code"] = result.get("status_code", 200)
            page["discovered_links"] = discovered
            page["crawl_depth"] = len(pages)
            pages.append(page)

            if page.get("text"):
                combined_parts.append(page["text"])

            parent_dir = self._parent_directory_url(url)
            if parent_dir and parent_dir not in visited and parent_dir not in queue:
                queue.appendleft(parent_dir)

            for link in self.links.prioritize(discovered, limit=len(discovered) or 0):
                if link not in visited and link not in queue:
                    queue.append(link)

        combined_text = "\n\n".join(combined_parts)
        # New extractor signature: takes text + pages (uses page-level structure too)
        try:
            extracted = self.extractor.extract(combined_text, pages)
        except TypeError:
            extracted = self.extractor.extract(combined_text)

        structured = build_scrape_result(extracted, pages)
        structured["crawl_stats"] = {
            "page_count": len(pages),
            "visited_count": len(visited),
            "max_pages": self.max_pages,
            "discovered_links_count": sum(len(p.get("discovered_links", [])) for p in pages if isinstance(p, dict)),
        }

        return {
            "ok": True,
            "domain": urlparse(root_url).netloc,
            "page_count": len(pages),
            "structured_data": structured,
            "pages": pages,
        }
