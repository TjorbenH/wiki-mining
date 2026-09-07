import asyncio
import hashlib
import io
import json
import logging
from urllib.parse import urljoin, urlparse
 
import aiohttp
import asyncpg
import bs4
from minio import Minio
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from DataBaseClient import DataBaseClient


logger = logging.getLogger(__name__)

class ScraperWorker:
    """ ScaperWorker is at its core a resource handle for a number of IO-threads.
    These threads fetches URLs out of the Redis Queue, process the html and run an extraction scheme to find and store relevant new URLs to crawl.
    Start / Stop using the run() and stop() method.
    Subclass and override _process_html / _filter_urls to customize per-site behavior. 
    """
    
    def __init__(
        self, 
        minio_client: Minio, 
        minio_bucket: str, 
        redis_client: Redis,
        pg_pool: asyncpg.Pool,
        hostname: str,
        crawl_stream: str,
        crawl_group: str,
        links_queue: str,
        headers: dict = None,
        minio_upload_attempts: int = 3,
        minio_retry_backoff_seconds: float = 0.5
    ):  
        self.minio_client = minio_client
        self.minio_bucket = minio_bucket
        self.redis_client = redis_client
        self.database = DataBaseClient(pg_pool)
        # hostname identifies the dockercontainer this scraper runs in to avoid conflicts with the redis queue
        self.hostname = hostname
        self.crawl_stream = crawl_stream
        self.crawl_group = crawl_group 
        self.links_queue = links_queue
        self._headers = headers
        
        # Event to manage the worker threads
        self.stop_event = asyncio.Event()
        # Mutex to prevent calling run multiple times
        self.run_mutex = asyncio.Lock()
        
        # Session with custom header if a website requires identification for automated scapeing (wikipedia)
        self.session = None  
        
        # configs for the minio upload retries
        self.minio_upload_attempts = minio_upload_attempts
        self.minio_retry_backoff_seconds = minio_retry_backoff_seconds
    
    async def _ensure_stream_group(self):
        """ Make sure the needed redis crawl_stream is active and create it if needed."""
        try:
            # "$" = only new entries from now on; mkstream=True creates the stream if absent
            await self.redis_client.xgroup_create(name=self.crawl_stream, groupname=self.crawl_group, id="$", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error(f"Redis stream {self.crawl_stream} for group {self.crawl_group} doesn't exist and could't be created.")
                raise
    
    async def run(self, concurrency_limit: int = 10, rate_limit: int = 1) -> None:
        """ Start <concurrency_limit> many crawler tasks each running with a per item rate limit of <rate_limit>. """
        if self.run_mutex.locked():
            logger.error("Scapers are already running. No more run() calls permitted!")
            return
        
        async with self.run_mutex:
            self.stop_event.clear()
            # make sure the redis stream to get url data is active
            await self._ensure_stream_group()
            self.session = aiohttp.ClientSession(headers=self._headers)
            logger.info(f"Starting {concurrency_limit} worker threads ...")
            try:
                async with asyncio.TaskGroup() as tg:
                    for worker_id in range(concurrency_limit):
                        tg.create_task(self._worker_loop(worker_id, rate_limit))
            finally:
                await self.session.close()
                self.session = None
    
    def stop(self) -> None:
        """ Safely stop all running crawler threads. """
        if not self.run_mutex.locked():
            logger.error("Received stop signal without any scrapers running!")
            return
        logger.info("Recieved stop signal ...")
        self.stop_event.set()
        
    async def _worker_loop(self, worker_id: int, rate_limit: int) -> None:
        consumer_name = f"{self.hostname}-worker-{worker_id}"
        logger.debug(f"{consumer_name} started...")

        while not self.stop_event.is_set():
            payload = None
            msg_id = None
            try:
                popped = await self._pop_url(consumer_name)
                if popped is None:
                    continue
                msg_id, payload = popped

                logger.debug(f"{consumer_name} processing: {payload['url']}")

                await self._process_url(payload["id"], payload["url"])

            except Exception:
                url = payload["url"] if payload else "unknown (failed before/during pop)"
                logger.exception(f"{consumer_name} failed while processing {url!r}")

                await asyncio.sleep(rate_limit) # delay also when an error is caught
                continue
            finally:
                # acking the url regardless of success status is no issue since the crawling_status field in the DB tracks the status
                # leaving it unacked simply cloggs up the redis queue
                if msg_id is not None:
                    await self.redis_client.xack(self.crawl_stream, self.crawl_group, msg_id)

            await asyncio.sleep(rate_limit) # basic per item rate limiting
    
    async def _pop_url(self, consumer_name:str) -> tuple[str, dict] | None:
        """ Returns (message_id, {"id":..., "url":...}) or None if nothing arrived."""
        result = await self.redis_client.xreadgroup(
            groupname=self.crawl_group,
            consumername=consumer_name,
            streams={self.crawl_stream: ">"},  # ">" = only entries never delivered to this group
            count=1,
            block=1000,  # ms timeout to stop this from blocking and not responding to a stop sigal
        )

        if not result:
            return None

        _, entries = result[0]
        msg_id, fields = entries[0]
        try:
            return msg_id, {"id": int(fields["id"]), "url": fields["url"]}
        except (KeyError, ValueError) as e:
            logger.error(f"Malformed stream entry {msg_id!r} fields={fields!r}: {e}. Acking to prevent redelivery.")
            await self.redis_client.xack(self.crawl_stream, self.crawl_group, msg_id)
            return None
        
    async def _process_url(self, url_id: int, url: str) -> None:
        """ Run the processing pipline for a single url.
        Fetch the website -> process the html -> save the html -> discover and filter new urls -> write back urls to the dispatcher
        """
        await self.database.mark_in_progress(url_id)
        
        async def fail_stage(url_id: int, url: str, stage: str) -> None: # Helper Function to avoid redundant debug messages
            logger.error(f"[{stage}] failed for {url} (id={url_id})")
            await self.database.mark_failed(url_id)

        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.error(f"HTTP {response.status} for {url} (id={url_id})")
                    await self.database.mark_failed(url_id)
                    return
                html = await response.text()
        except Exception:
            await fail_stage(url_id, url, "http_fetch")
            raise

        try:
            processed_html = await self._process_html(html)
            html_bytes = processed_html.encode("utf-8")
            content_hash = hashlib.sha256(html_bytes).hexdigest()
        except Exception:
            await fail_stage(url_id, url, "html_process")
            raise

        try:
            storage_key = await self._save_to_minio(url, html_bytes)
            await self.database.mark_done(url_id, storage_key, content_hash)
        except Exception:
            await fail_stage(url_id, url, "minio_upload")
            raise

        try:
            raw_links = await asyncio.to_thread(self._extract_urls, html, url)
            new_links = await self._filter_urls(raw_links)
        except Exception:
            await fail_stage(url_id, url, "url_extraction")
            raise

        try:
            if new_links:
                payload = json.dumps({"origin_id": url_id, "urls": list(new_links)})
                await self.redis_client.rpush(self.links_queue, payload)
        except Exception:
            await fail_stage(url_id, url, "redis_writeback")
            raise

    async def _save_to_minio(self, url: str, html_bytes: bytes) -> str:
        """Uploads the given bytes to MinIO and returns the storage_key (object name) they were stored under.
        Retries a minio_upload_attempts times on upload failures.
        """
        MAX_STORAGE_KEY_PREFIX_BYTES = 200 # CAREFULL: magic number
        def truncate_utf8(s: str, max_bytes: int) -> str: 
            # Helper Function to truncate the object name to stay below minio's 255 byte name limit
            encoded = s.encode("utf-8")
            if len(encoded) <= max_bytes:
                return s
            return encoded[:max_bytes].decode("utf-8", errors="ignore")
        
        parsed = urlparse(url)
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:12]
        prefix = f"{parsed.netloc}{parsed.path}".strip("/").replace("/", "_")
        prefix = truncate_utf8(prefix, MAX_STORAGE_KEY_PREFIX_BYTES)
        storage_key = f"{prefix}_{url_hash}.html"

        def upload():
            # a fresh BytesIO per attempt, so a retry always sends the full body from byte 0
            self.minio_client.put_object(
                bucket_name=self.minio_bucket,
                object_name=storage_key,
                data=io.BytesIO(html_bytes),
                length=len(html_bytes),
                content_type="text/html"
            )

        for attempt in range(self.minio_upload_attempts):
            try:
                await asyncio.to_thread(upload)
                break
            except Exception:
                if attempt == self.minio_upload_attempts:
                    raise
                logger.warning(f"MinIO upload attempt {attempt}/{self.minio_upload_attempts} failed for {storage_key}, retrying...")
                await asyncio.sleep(self.minio_retry_backoff_seconds * attempt)

        logger.debug(f"Saved in MinIO: {storage_key}")
        return storage_key
        
    def _extract_urls(self, html: str, base_url: str) -> set[str]:
        """ Parse HTML and extract all hyperlink URLs."""    
        soup = bs4.BeautifulSoup(html, "lxml")
        links = set()
        
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            # resolve relative URLs
            if base_url:
                href = urljoin(base_url, href)
            links.add(href)
             
        return links
    
    async def _process_html(self, html:str) -> str:
        """ Interface: Overwrite to set specific processing rules for the html data (e.g. filter for certain fields ...)"""
        return html
    
    async def _filter_urls(self, urls: set[str]) ->set[str]:
        """ Interface: Overwrite to set specific crawl rules (e.g. certain top level domains, whitelist, blacklist ...)"""
        return urls