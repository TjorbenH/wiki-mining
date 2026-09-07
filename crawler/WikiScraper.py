from ScraperWorker import ScraperWorker
from urllib.parse import urlparse
from typing import override

class WikiScraper(ScraperWorker):
    """ Instantiation of a Scraper Worker for Wiki-Style Pages
    In particular only links staying on the wiki domain will be scraped, we don't want the WWW.
    """
    
    @override
    def __init__(
        self, 
        minio_client, 
        minio_bucket, 
        redis_client, 
        crawl_stream, 
        crawl_group, 
        dispatcher_stream, 
        dispatcher_group, 
        pg_pool, 
        hostname, 
        domain, 
        headers = None, 
        minio_upload_attempts = 3, 
        minio_retry_backoff_seconds = 0.5
        ):
        super().__init__(minio_client, 
                         minio_bucket, 
                         redis_client, 
                         crawl_stream, 
                         crawl_group, 
                         dispatcher_stream, 
                         dispatcher_group, 
                         pg_pool, hostname, 
                         headers, 
                         minio_upload_attempts, 
                         minio_retry_backoff_seconds)
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