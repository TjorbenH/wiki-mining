from ScraperWorker import ScraperWorker
from urllib.parse import urlparse
from typing import override

class WikiScraper(ScraperWorker):
    """ Instantiation of a Scraper Worker for Wiki-Style Pages
    In particular only links staying on the wiki domain will be scraped, we don't want the WWW.
    """
    @override
    def __init__(self, minio_client, minio_bucket, redis_client, pg_pool, hostname, crawl_stream, crawl_group, links_queue, domain, headers = None):
        super().__init__(minio_client, minio_bucket, redis_client, pg_pool, hostname, crawl_stream, crawl_group, links_queue, headers)
        self.domain = domain.lower().strip()

    @override
    async def _filter_urls(self, urls: set[str]) -> set[str]:
        def url_in_domain(url: str) -> bool:
            try:
                hostname = urlparse(url).hostname
            except ValueError:
                return False
            if not hostname:
                return False
            hostname = hostname.lower()
            return hostname == self.domain or hostname.endswith("." + self.domain)
        
        return {url for url in urls if url_in_domain(url)}