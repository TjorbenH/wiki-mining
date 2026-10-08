import logging
from dataclasses import dataclass
from urllib.parse import urljoin, urldefrag, urlparse, unquote

import aiohttp
import bs4

from ScraperWorker import ScraperWorker


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _WikiLayout:
    """ The parts of a MediaWiki's siteinfo needed to tell article URLs apart from everything else. """
    article_prefix: str             # path before the title, e.g. "/" (Halopedia) or "/wiki/" (Wikipedia)
    script_path: str                # path index.php / api.php / load.php live under, e.g. "" or "/w"
    excluded_namespaces: frozenset  # normalized names + aliases of every namespace that isn't followed


class MediaWikiScraperWorker(ScraperWorker):
    """ ScraperWorker for MediaWiki sites: only follows links to article pages.
    A URL is kept only if it
      - has no query string,
      - sits under the wiki's article path,
      - has a title whose namespace is a content namespace (main + whatever the wiki marks as content,
        e.g. Halopedia's H3:, Reach:, ...) or one of extra_namespaces,
      - isn't a link to a redirect page (MediaWiki marks those with class="mw-redirect").

    The namespace layout is read from each whitelisted domain's api.php (meta=siteinfo) once at startup,
    next to the whitelist itself. Every whitelisted domain must be a MediaWiki - startup fails otherwise.
    """

    def __init__(self, *args, extra_namespaces: frozenset[str] = frozenset(), skip_redirects: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self.extra_namespaces = frozenset(self._normalize_namespace(ns) for ns in extra_namespaces)
        self.skip_redirects = skip_redirects
        self._wiki_layouts: dict[str, _WikiLayout] = {}

    @staticmethod
    def _normalize_namespace(name: str) -> str:
        # namespace prefixes are case-insensitive and treat "_" and " " the same
        return name.replace("_", " ").strip().lower()

    async def _load_whitelists(self) -> None:
        await super()._load_whitelists()

        user_agent = (self._headers or {}).get("User-Agent", "*")
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds, connect=self.connect_timeout_seconds)
        layouts = {}
        async with aiohttp.ClientSession(headers=self._headers, timeout=timeout) as session:
            for domain, parser in self._domain_parsers.items():
                layouts[domain] = await self._fetch_wiki_layout(session, domain, parser, user_agent)
        self._wiki_layouts = layouts

    async def _fetch_wiki_layout(self, session: aiohttp.ClientSession, domain: str, parser, user_agent: str) -> _WikiLayout:
        """ Ask the wiki itself for its URL layout and namespaces. Tries https first, then http (like seed.py). """
        last_error = None
        for scheme in ("https", "http"):
            # api.php lives under the script path, which isn't known yet - try the two common locations
            for script_path in ("", "/w"):
                api_url = f"{scheme}://{domain}{script_path}/api.php"
                if not parser.can_fetch(user_agent, api_url):
                    last_error = f"{api_url} disallowed by robots.txt"
                    continue
                params = {
                    "action": "query",
                    "meta": "siteinfo",
                    "siprop": "general|namespaces|namespacealiases",
                    "format": "json",
                    "formatversion": "2",
                }
                try:
                    async with session.get(api_url, params=params) as response:
                        if response.status != 200:
                            last_error = f"HTTP {response.status} from {api_url}"
                            continue
                        query = (await response.json(content_type=None))["query"]
                except Exception as e:
                    last_error = f"{api_url}: {e!r}"
                    continue
                return self._parse_wiki_layout(domain, query)

        raise RuntimeError(
            f"Couldn't read MediaWiki siteinfo for whitelisted domain {domain!r} ({last_error}). "
            f"MediaWikiScraperWorker can only crawl MediaWiki sites."
        )

    def _parse_wiki_layout(self, domain: str, query: dict) -> _WikiLayout:
        general = query["general"]
        article_prefix = general["articlepath"].split("$1")[0]
        script_path = general.get("scriptpath", "")

        excluded_ids = set()
        followed = []
        names_by_id: dict[int, set[str]] = {}
        for ns in query["namespaces"].values():
            ns_id = ns["id"]
            names_by_id[ns_id] = {ns["name"], ns.get("canonical") or ns["name"]}
            if ns_id == 0:
                continue
            if ns.get("content") or any(self._normalize_namespace(n) in self.extra_namespaces for n in names_by_id[ns_id]):
                followed.append(ns["name"])
            else:
                excluded_ids.add(ns_id)
        for alias in query.get("namespacealiases", []):
            names_by_id.setdefault(alias["id"], set()).add(alias["alias"])

        excluded = {
            self._normalize_namespace(name)
            for ns_id in excluded_ids
            for name in names_by_id[ns_id]
        }

        logger.info(
            f"{domain}: MediaWiki {general.get('generator', '?')!r}, article path {article_prefix!r}, script path {script_path!r}, "
            f"following main namespace + {sorted(followed)}, excluding {len(excluded)} namespace names"
        )
        return _WikiLayout(article_prefix=article_prefix, script_path=script_path, excluded_namespaces=frozenset(excluded))

    def _is_article_url(self, url: str) -> bool:
        p = urlparse(url)
        layout = self._wiki_layouts.get((p.hostname or "").lower())
        if layout is None or p.query or p.params:
            return False
        if not p.path.startswith(layout.article_prefix):
            return False

        # with article path "/" index.php, api.php, load.php ... look like titles too
        if p.path.startswith(f"{layout.script_path}/"):
            first_segment = p.path[len(layout.script_path) + 1:].split("/", 1)[0]
            if first_segment.endswith(".php"):
                return False

        title = unquote(p.path[len(layout.article_prefix):])
        if not title.strip():
            return False  # domain root / bare article path, a redirect to the main page
        if ":" in title:
            # "Halo: Reach" is an article, "File:Halo.jpg" isn't - only a known namespace prefix counts
            prefix = self._normalize_namespace(title.split(":", 1)[0])
            if prefix in layout.excluded_namespaces:
                return False
        return True

    def _extract_urls(self, html: str, base_url: str) -> set[str]:
        """ Same as ScraperWorker._extract_urls, but skips links MediaWiki marks as pointing to a redirect page:
        a redirect serves its target's content under another URL, so following it only stores a duplicate.
        """
        if not self.skip_redirects:
            return super()._extract_urls(html, base_url)

        soup = bs4.BeautifulSoup(html, "lxml")
        links = set()

        for tag in soup.find_all("a", href=True):
            if "mw-redirect" in tag.get("class", []):
                continue
            href = tag["href"].strip()
            if base_url:
                href = urljoin(base_url, href)
            href, _ = urldefrag(href)
            if href:
                links.add(href)

        return links

    async def _filter_urls(self, urls: set[str]) -> set[str]:
        """ Whitelist + robots.txt (see ScraperWorker._filter_urls), then article URLs only. """
        urls = await super()._filter_urls(urls)
        kept = {url for url in urls if self._is_article_url(url)}
        logger.debug(f"Kept {len(kept)}/{len(urls)} whitelisted urls as article links")
        return kept
