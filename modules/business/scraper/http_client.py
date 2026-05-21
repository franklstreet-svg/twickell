from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class SimpleHttpClient:
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.user_agent = (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )

    def fetch(self, url: str) -> dict:
        request = Request(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml",
            },
        )

        try:
            with urlopen(request, timeout=self.timeout) as response:
                content_type = response.headers.get("Content-Type", "")
                html = response.read().decode("utf-8", errors="ignore")
                return {
                    "ok": True,
                    "url": url,
                    "status_code": getattr(response, "status", 200),
                    "content_type": content_type,
                    "html": html,
                    "error": "",
                }
        except HTTPError as exc:
            return {
                "ok": False,
                "url": url,
                "status_code": exc.code,
                "content_type": "",
                "html": "",
                "error": f"HTTPError: {exc}",
            }
        except URLError as exc:
            return {
                "ok": False,
                "url": url,
                "status_code": 0,
                "content_type": "",
                "html": "",
                "error": f"URLError: {exc}",
            }
        except Exception as exc:
            return {
                "ok": False,
                "url": url,
                "status_code": 0,
                "content_type": "",
                "html": "",
                "error": f"Exception: {exc}",
            }

