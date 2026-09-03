import asyncio
import io
import logging
from urllib.parse import urljoin, urlparse
import aiohttp
import bs4
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from minio import Minio
import json


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
        hostname: str = "unknown",
        headers: dict = None
    ):     
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        self.minio_client = minio_client
        self.minio_bucket = minio_bucket
        self.redis_client = redis_client
        # hostname identifies the dockercontainer this scraper runs in to avoid conflicts with the redis queue
        self.hostname = hostname 
        self._headers = headers
        
        # Event to manage the worker threads
        self.stop_event = asyncio.Event()
        # Mutex to prevent calling run multiple times
        self.run_mutex = asyncio.Lock()
        
        # Session with custom header if a website requires identification for automated scapeing (wikipedia)
        self.session = None  
    
    async def ensure_stream_group(self, redis: Redis, stream: str, group: str):
        try:
            # "$" = only new entries from now on; mkstream=True creates the stream if absent
            await redis.xgroup_create(name=stream, groupname=group, id="$", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error(f"Redis stream {stream} for group {group} doesn't exist and could't be created.")
                raise
    
    async def run(self, concurrency_limit: int = 10, rate_limit: int = 1) -> None:
        if self.run_mutex.locked():
            logger.error(f"Scapers are already running. No more run() calls permitted!")
            return
        
        async with self.run_mutex:
            self.stop_event.clear()
            # make sure the redis stream to get url data is active
            await self.ensure_stream_group(self.redis_client, "crawl_queue", "crawlers")
            self.session = aiohttp.ClientSession(headers=self._headers)
            logger.info(f"Starting {concurrency_limit} many worker threads ...")
            try:
                async with asyncio.TaskGroup() as tg:
                    for worker_id in range(concurrency_limit):
                        tg.create_task(self._worker_loop(worker_id, rate_limit))
            finally:
                await self.session.close()
                self.session = None
    
    def stop(self) -> None:
        if not self.run_mutex.locked():
            logger.error(f"Received stop signal without any scrapers running!")
            return
        logger.info(f"Recieved stop signal ...")
        self.stop_event.set()
        
    async def _worker_loop(self, worker_id: int, rate_limit: int) -> None:
        consumer_name = f"{self.hostname}-worker-{worker_id}"
        logger.debug(f"{consumer_name} started...")
        
        while not self.stop_event.is_set():
            try:
                popped = await self._pop_url(consumer_name)
                if popped is None:
                    continue
                msg_id, payload = popped
                
                logger.debug(f"{consumer_name} processing: {payload['url']}")
                
                await self._process_url(payload["id"], payload["url"])
                # ack once the url was processed successfully, prevents lost urls
                await self.redis_client.xack("crawl_queue", "crawlers", msg_id)
                
            except Exception as e:
                logger.error(f"{consumer_name} failed: {e}")
                await asyncio.sleep(rate_limit) # delay also when an error is caught
                continue
            
            await asyncio.sleep(rate_limit) # basic per item rate limiting
    
    async def _pop_url(self, consumer_name:str) -> tuple[str, dict] | None:   
        """Returns (message_id, {"id":..., "url":...}) or None if nothing arrived."""
        result = await self.redis_client.xreadgroup(
            groupname="crawlers",
            consumername=consumer_name,
            streams={"crawl_queue": ">"},  # ">" = only entries never delivered to this group
            count=1,
            block=1000,  # ms timeout to stop this from blocking and not responding to a stop sigal
        )
        
        if not result:
            return None
                        
        _, entries = result[0]
        msg_id, fields = entries[0]
        return msg_id, {"id": int(fields['id']), "url": fields['url']}
    
    #TODO i need to use the url_id for both the mino saving and writeback into the redis queue, so the dispatcher can enter them into the DB
    async def _process_url(self, url_id:int, url:str, ) -> None:
        async with self.session.get(url) as response:
            if response.status == 200:
                # url processing pipline
                html = await response.text()
                processed_html = await self._process_html(html)
                await self._save_to_minio(url, processed_html)
                                
                # url extraciton pipeline
                raw_links = await asyncio.to_thread(self._extract_urls, html, url)
                new_links = await self._filter_urls(raw_links)
                
                if new_links:
                    payload = json.dumps({"origin_id": url_id, "urls": list(new_links)})
                    await self.redis_client.rpush("unprocessed_links", payload)
                                    
            else:
                logger.error(f"Error {response.status} fetching {url}") 
        
    async def _save_to_minio(self, url: str, html_content: str) -> None:
        parsed = urlparse(url)
        safe_filename = f"{parsed.netloc}{parsed.path}".strip("/").replace("/", "_") + ".html"
        
        html_bytes = html_content.encode("utf-8")
        data_stream = io.BytesIO(html_bytes)
        
        def _upload():
            self.minio_client.put_object(
                bucket_name=self.minio_bucket,
                object_name=safe_filename,
                data=data_stream,
                length=len(html_bytes),
                content_type="text/html"
            )
        
        await asyncio.to_thread(_upload)
        logger.debug(f"Saved in MinIO: {safe_filename}")
        
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