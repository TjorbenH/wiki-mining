import asyncio
import json
import logging
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ResponseError


logger = logging.getLogger(__name__)

class QueueManager:
    """ Reads out potentially new URLs from the Crawlers over the Redis Queue in batches and checks them with the database state.
    Orchestrates URLs that have yet to be scraped into the Redis Queue for Crawlers and enters them into the database.
    step by step:
        1. read new urls from the redis queue
        2. check for exisitence in the DawData DB
            a) if an entry exists drop the url and continue 
            b) if no entry exists create a new entry with the id and link field 
        3. Enter all connections (origin_id, destination_id) that got discorvered into the Links DB
        4. Push new URLs to Scrape and their ID back to the crawlers over Redis

    QueueManager is the only component that assigns RawData ids or write to the Links table.
    Start/stop with run() and stop()
    """

    def __init__(
        self,
        redis_client: Redis,
        crawl_stream: str,
        crawl_group: str,
        dispatcher_stream: str,
        dispatcher_group: str,
        pg_pool: asyncpg.Pool,
        hostname: str,
        batch_size: int = 200
    ):
        self.redis_client = redis_client
        self.crawl_stream = crawl_stream
        self.crawl_group = crawl_group 
        self.dispatcher_stream = dispatcher_stream
        self.dispatcher_group = dispatcher_group
        
        self.pg_pool = pg_pool
        self.batch_size = batch_size
        
        # in theory more than one dispatch container can run, so we need to id it for the redis queue
        self.hostname = hostname
        self.consumer_name = f"{self.hostname}-dispatcher"

        # Event to manage the run loop
        self.stop_event = asyncio.Event()
        # Mutex to prevent calling run multiple times
        self.run_mutex = asyncio.Lock()

    async def _ensure_stream_group(self, stream, group) -> None: #TODO this is duplicate would be nice to have a util collection for dispatcher AND scraper
        try:
            # "$" = only new entries from now on; mkstream=True creates the stream if absent
            await self.redis_client.xgroup_create(name=stream, groupname=group, id="$", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error(f"Stream {stream} for group {group} doesn't exist and couldn't be created.")
                raise

    async def run(self, poll_interval: float = 1.0) -> None:
        """ Start the Queue Manager with a delay of <poll_interval> if the last batch was empty."""
        if self.run_mutex.locked():
            logger.error("QueueManager is already running. No more run() calls permitted!")
            return

        async with self.run_mutex:
            self.stop_event.clear()
            # ensure communication with the crawlers is possible
            await self._ensure_stream_group(stream=self.crawl_stream, group=self.crawl_group)
            await self._ensure_stream_group(stream=self.dispatcher_stream, group=self.dispatcher_group)
            logger.info("QueueManager started.")

            while not self.stop_event.is_set():
                try:
                    n_processed = await self._drain_and_dispatch()
                except Exception as e:
                    logger.exception(f"QueueManager cycle failed: {e}")
                    n_processed = 0

                if n_processed == 0:
                    # nothing to do -> backoff
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=poll_interval)
                    except asyncio.TimeoutError:
                        pass

            logger.info("QueueManager stopped.")

    def stop(self) -> None:
        """Safely stop the Queue Manager."""
        if not self.run_mutex.locked():
            logger.error("Received stop signal without QueueManager running!")
            return
        logger.info("Received stop signal ...")
        self.stop_event.set()
        
    def _canonicalize(self, url: str) -> str:
        """Normalize a URL so semantically identical URLs map to the same string"""
        try:
            p = urlparse(url)
            path = p.path.rstrip("/") or "/"
            query = urlencode(sorted(parse_qsl(p.query)))
            return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", query, ""))
        except Exception:
            return url
 
    async def _drain_batch(self) -> list[tuple[str, dict]]:
        """Read up to batch_size entries off the dispatcher stream."""
        result = await self.redis_client.xreadgroup(
            groupname=self.dispatcher_group,
            consumername=self.consumer_name,
            streams={self.dispatcher_stream: ">"},  # ">" = only entries never delivered to this group
            count=self.batch_size,
            block=1000,  # ms timeout to stop this from blocking and not responding to a stop signal
        )

        if not result:
            return []

        _, entries = result[0]
        batch = []
        for msg_id, fields in entries:
            try:
                batch.append((msg_id, {"origin_id": int(fields["origin_id"]), "urls": json.loads(fields["urls"])}))
            except (KeyError, ValueError, json.JSONDecodeError) as e:
                logger.error(f"Dropping malformed {self.dispatcher_stream} entry {msg_id!r} fields={fields!r}: {e}. Acking to prevent redelivery.")
                await self.redis_client.xack(self.dispatcher_stream, self.dispatcher_group, msg_id)
        return batch
    
    async def _drain_and_dispatch(self) -> int:
        """Drains potentially new URLs from the dispatcher stream, 
        enters them into the RawData DB and checks if they are genuinly new,
        if so dispatches the url back to the crawlers via the crawl_stream."""
        batch = await self._drain_batch()
        if not batch:
            return 0

        msg_ids = [msg_id for msg_id, _ in batch]
    
        async def ack(msg_ids):
            # helper function to ack the processing of a set of msg_ids to the dispatch stream
            if msg_ids:
                await self.redis_client.xack(self.dispatcher_stream, self.dispatcher_group, *msg_ids)

        # flatten origin_id:[outgoing_links] into pairs of (origin_id, link), canonicalizing each URL
        pairs: list[tuple[int, str]] = [(entry["origin_id"], self._canonicalize(url)) for _, entry in batch for url in entry.get("urls", [])]
        if not pairs:
            logger.warning(f"Drained {len(batch)} entries from {self.dispatcher_stream} but all had empty URL lists")
            await ack(msg_ids)
            return 0

        # dedupe if links were discovered from multiple origin ids
        urls = list(dict.fromkeys(url for _, url in pairs))
        origin_ids_in_batch = list({o for o, _ in pairs})

        try:
            async with self.pg_pool.acquire() as conn:
                async with conn.transaction():
                    # Insert all discovered Links into RawData
                    # Make note if a link already existed using the ON CONFLICT to set xmax = 0
                    rows = await conn.fetch(
                        """
                        INSERT INTO RawData (link)
                        SELECT unnest($1::text[])
                        ON CONFLICT (link) DO UPDATE SET link = EXCLUDED.link
                        RETURNING id, link, (xmax = 0) AS is_new;
                        """,
                        urls,
                    )
                    url_to_id = {row["link"]: row["id"] for row in rows}
                    new_rows = [row for row in rows if row["is_new"]]

                    # bulk edge insert
                    origin_ids = [origin_id for origin_id, url in pairs if url in url_to_id]
                    dest_ids = [url_to_id[url] for origin_id, url in pairs if url in url_to_id]
                    if origin_ids:
                        await conn.execute(
                            """
                            INSERT INTO Links (origin_id, destination_id)
                            SELECT * FROM unnest($1::bigint[], $2::bigint[])
                            ON CONFLICT DO NOTHING;
                            """,
                            origin_ids,
                            dest_ids,
                        )
        except Exception:
            logger.error(
                f"[db_write] failed for batch of {len(pairs)} links "
                f"(origins: {origin_ids_in_batch}, {len(urls)} unique URLs)"
            )
            raise

        # enter genuinely new links (xmax != 0) back into redis
        try:
            if new_rows:
                async with self.redis_client.pipeline(transaction=True) as pipe:
                    for row in new_rows:
                        pipe.xadd(self.crawl_stream, {"id": str(row["id"]), "url": row["link"]})
                    await pipe.execute()
        except Exception:
            logger.error(
                f"[redis_push] failed for {len(new_rows)} new URLs — "
                f"they are in the DB but were not queued onto {self.crawl_stream}"
            )
            raise

        # acking at this point is fine as all links are n
        # ow in the DB and back into the redis queue so nothing can get lost anymore
        await ack(msg_ids)

        logger.info(f"Batch: {len(pairs)} links processed, {len(new_rows)} URLs dispatched to {self.crawl_stream}.")
        return len(pairs)