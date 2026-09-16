import asyncio
import hashlib
import io
import json
import logging
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
 
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
        crawl_stream: str,
        crawl_group: str,
        dispatcher_stream: str,
        dispatcher_group: str,
        pg_pool: asyncpg.Pool,
        hostname: str,
        headers: dict = None,
        minio_upload_attempts: int = 3,
        minio_retry_backoff_seconds: float = 0.5
    ):  
        self.minio_client = minio_client
        self.minio_bucket = minio_bucket
        
        self.redis_client = redis_client
        self.crawl_stream = crawl_stream
        self.crawl_group = crawl_group 
        self.dispatcher_stream = dispatcher_stream
        self.dispatcher_group = dispatcher_group
        
        self.database = DataBaseClient(pg_pool)
        
        # hostname identifies the dockercontainer this scraper runs in to avoid conflicts with the redis queue
        self.hostname = hostname
        
        self._headers = headers
        
        # configs for the minio upload retries
        self.minio_upload_attempts = minio_upload_attempts
        self.minio_retry_backoff_seconds = minio_retry_backoff_seconds
        
        # Event to manage the worker threads
        self.stop_event = asyncio.Event()
        # Mutex to prevent calling run multiple times
        self.run_mutex = asyncio.Lock()
        
        # Session with custom header if a website requires identification for automated scapeing (wikipedia)
        self.session = None  
        
        # whitelist for all crawlable domains and blacklist stemming from any robots.txt files
        self.domain_whitelist = set()
        self.domain_blacklist = set()
        # per-domain parsed robots.txt rules for whitelisted domains, used for path-level compliance checks
        self.robots_parsers: dict[str, RobotFileParser] = {}
    
    async def _ensure_stream_group(self, stream, group) -> None:
        try:
            # "$" = only new entries from now on; mkstream=True creates the stream if absent
            await self.redis_client.xgroup_create(name=stream, groupname=group, id="$", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error(f"Stream {stream} for group {group} doesn't exist and couldn't be created.")
                raise
            
    async def initalize_domain(self, domain: str) -> bool:
        """ Check for any restrictions to crawlers on the given domain (robots.txt), store them and whitelist the domain for crawling.
        Returns True if the domain is now whitelisted, False if robots.txt disallows the crawler entirely (domain is blacklisted instead).
        Safe to call before run() starts the worker session - uses its own short-lived session.
        """
        domain = domain.lower().strip()
        user_agent = (self._headers or {}).get("User-Agent", "*")

        # Fetch robots.txt
        robots_text = None
        no_robots_file = False
        access_denied = False
        async with aiohttp.ClientSession(headers=self._headers, timeout=aiohttp.ClientTimeout(total=10)) as session:
            for scheme in ("https", "http"):
                robots_url = f"{scheme}://{domain}/robots.txt"
                try:
                    async with session.get(robots_url) as response:
                        if response.status == 200:
                            robots_text = await response.text()
                            break
                        if response.status in (401, 403):
                            # Access to robots.txt itself refused -> domain gets blacklisted entirely
                            logger.warning(f"robots.txt at {robots_url} returned HTTP {response.status} (access denied)")
                            access_denied = True
                            break
                        if 400 <= response.status < 500:
                            # no robots.txt present -> no restrictions
                            logger.debug(f"robots.txt at {robots_url} returned HTTP {response.status}; treating as absent")
                            no_robots_file = True
                            break
                        logger.warning(f"robots.txt at {robots_url} returned HTTP {response.status}")
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.warning(f"Failed to fetch {robots_url}: {e}")

        if access_denied or (robots_text is None and not no_robots_file):
            # Either robots.txt access was explicitly refused, or its absence/presence couldn't be confirmed
            logger.error(f"Could not confirm crawl permission for {domain!r}; blacklisting domain.")
            self.domain_whitelist.discard(domain)
            self.robots_parsers.pop(domain, None)
            self.domain_blacklist.add(domain)
            return False

        # extract disallow entries into a per-domain robots parser
        parser = RobotFileParser()
        parser.parse(robots_text.splitlines() if robots_text is not None else [])

        if not parser.can_fetch(user_agent, f"https://{domain}/"):
            logger.warning(f"robots.txt for {domain!r} disallows {user_agent!r} entirely; blacklisting domain.")
            self.domain_whitelist.discard(domain)
            self.robots_parsers.pop(domain, None)
            self.domain_blacklist.add(domain)
            return False

        # store the domain in the crawlable domain whitelist
        self.robots_parsers[domain] = parser
        self.domain_whitelist.add(domain)
        self.domain_blacklist.discard(domain)
        logger.info(f"Domain whitelisted for crawling: {domain!r} ({'no robots.txt found' if robots_text is None else 'robots.txt parsed'})")
        return True

    async def run(self, concurrency_limit: int = 10, rate_limit: int = 1) -> None:
        """ Start <concurrency_limit> many crawler tasks each running with a per item rate limit of <rate_limit>. """
        if self.run_mutex.locked():
            logger.error("Scapers are already running. No more run() calls permitted!")
            return
        
        async with self.run_mutex:
            self.stop_event.clear()
            # ensure communication with the dispatcher is possible
            await self._ensure_stream_group(stream=self.crawl_stream, group=self.crawl_group)
            await self._ensure_stream_group(stream=self.dispatcher_stream, group=self.dispatcher_group)
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
                await self.redis_client.xadd(self.dispatcher_stream, {"origin_id": str(url_id), "urls": json.dumps(list(new_links))})
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
        """ Interface: Overwrite to set specific crawl rules (e.g. certain top level domains, whitelist, blacklist ...)
        By default the crawler remains on the set of whitelisted domains for which a compliance check was perfomed using setup().
        It's recommended to keep this behavior and to call super._filter_urls() in any subclass.
        """
        user_agent = (self._headers or {}).get("User-Agent", "*")
        filtered_urls = set()

        for url in urls:
            try:
                hostname = urlparse(url).hostname
            except ValueError:
                logger.debug(f"Dropping unparsable url: {url!r}")
                continue
            if not hostname:
                logger.debug(f"Dropping url with no hostname: {url!r}")
                continue
            hostname = hostname.lower()

            if any(hostname == blocked or hostname.endswith("." + blocked) for blocked in self.domain_blacklist):
                logger.debug(f"Dropping blacklisted url: {url!r}")
                continue

            # exact match or subdomain of a whitelisted domain (e.g. "en.wikipedia.org" under "wikipedia.org")
            whitelisted_domain = next(
                (allowed for allowed in self.domain_whitelist if hostname == allowed or hostname.endswith("." + allowed)),
                None,
            )
            if whitelisted_domain is None:
                logger.debug(f"Dropping non-whitelisted url: {url!r}")
                continue

            parser = self.robots_parsers.get(whitelisted_domain)
            if parser is None:
                # Whitelisted without cached robots.txt rules (e.g. added outside initalize_domain)
                logger.warning(f"No robots.txt rules cached for whitelisted domain {whitelisted_domain!r}; dropping {url!r}")
                continue

            if not parser.can_fetch(user_agent, url):
                logger.debug(f"Dropping url disallowed by robots.txt: {url!r}")
                continue

            filtered_urls.add(url)

        return filtered_urls