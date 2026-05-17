MODULE_MANIFEST = {
    "id": "M007",
    "name": "Web Search",
    "category": "Core",
    "description": "Search the web and retrieve current information to answer user questions.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["web_search"],
    "min_bridge_version": "1.0.0"
}

"""Safe web search for Orby — DuckDuckGo web/news + Wikipedia. No API key needed."""
import logging
import requests

log = logging.getLogger(__name__)

_HEADERS = {'User-Agent': 'MyOrby/1.0 (personal AI assistant)'}
_SAFE = 'moderate'   # 'strict', 'moderate', or 'off'
_TIMEOUT = 10


def web_search(query: str, max_results: int = 5) -> str:
    """Search the web via DuckDuckGo. Returns a formatted snippet string."""
    try:
        from ddgs import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results, safesearch=_SAFE))
        if not results:
            return f"No results found for '{query}'."
        lines = [f"Web search results for: {query}\n"]
        for i, r in enumerate(results, 1):
            lines.append(f"{i}. {r['title']}")
            lines.append(f"   {r['body'][:200]}")
            lines.append(f"   Source: {r['href']}\n")
        return '\n'.join(lines)
    except Exception as e:
        log.warning('Web search failed: %s', e)
        return f"Web search unavailable right now: {e}"


def news_search(query: str, max_results: int = 5) -> str:
    """Search for recent news via DuckDuckGo News."""
    try:
        from ddgs import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.news(query, max_results=max_results, safesearch=_SAFE))
        if not results:
            return f"No recent news found for '{query}'."
        lines = [f"Latest news for: {query}\n"]
        for r in results:
            lines.append(f"• {r['title']}")
            lines.append(f"  {r.get('body', '')[:150]}")
            lines.append(f"  Source: {r['source']} — {r.get('date', '')}\n")
        return '\n'.join(lines)
    except Exception as e:
        log.warning('News search failed: %s', e)
        return f"News search unavailable right now: {e}"


def wikipedia_lookup(topic: str) -> str:
    """Get a Wikipedia summary for a topic."""
    try:
        # First find the right page title
        r = requests.get('https://en.wikipedia.org/w/api.php', params={
            'action': 'query', 'list': 'search',
            'srsearch': topic, 'format': 'json', 'srlimit': 1
        }, headers=_HEADERS, timeout=_TIMEOUT)
        r.raise_for_status()
        results = r.json().get('query', {}).get('search', [])
        if not results:
            return f"Nothing found on Wikipedia for '{topic}'."

        title = results[0]['title']
        r2 = requests.get(
            f'https://en.wikipedia.org/api/rest_v1/page/summary/{title.replace(" ", "_")}',
            headers=_HEADERS, timeout=_TIMEOUT)
        r2.raise_for_status()
        d = r2.json()
        extract = d.get('extract', '')
        if not extract:
            return f"Wikipedia page found for '{title}' but it has no summary."
        return f"From Wikipedia — {d.get('title', title)}:\n{extract[:600]}"
    except Exception as e:
        log.warning('Wikipedia lookup failed: %s', e)
        return f"Wikipedia lookup unavailable: {e}"


def smart_search(query: str) -> str:
    """
    Routes to the best source automatically:
    - News keywords → news_search
    - 'What is / Who is / Tell me about' → wikipedia_lookup first, then web if empty
    - Everything else → web_search
    """
    q_lower = query.lower()

    # News detection
    news_words = ('news', 'today', 'latest', 'breaking', 'current events',
                  'headlines', 'happening', 'update', 'recently')
    if any(w in q_lower for w in news_words):
        result = news_search(query)
        if 'unavailable' not in result and 'No recent' not in result:
            return result

    # Wikipedia-worthy queries (factual/definitional)
    wiki_words = ('what is', 'who is', 'what are', 'who was', 'tell me about',
                  'history of', 'definition of', 'meaning of', 'explain')
    if any(q_lower.startswith(w) or w in q_lower for w in wiki_words):
        wiki = wikipedia_lookup(query)
        if 'unavailable' not in wiki and 'Nothing found' not in wiki:
            return wiki

    # General web search
    return web_search(query)
