import asyncio
import json
import logging

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ResponseError


logger = logging.getLogger(__name__)

#TODO set up the unprocessed_links redis queue as a stream as well otherwise batches get lost when the Manager breaks down. Not acceptatble.
class QueueManager:
    """ Reads out potentially new URLs from the Crawlers over the Redis Queue and checks them with the database state.
    Orchestrates URLs that have yet to be scraped into the Redis Queue for Crawlers and enters them into the database.
    step by step:
        1. read new urls from the redis queue
        2. check for exisitence in the DawData DB
            a) if an entry exists drop the url and continue 
            b) if no entry exists create a new entry with the id and link field 

    QueueManager is the only component that assigns RawData ids or write to the Links table.
    Start / stop with run() and stop()
    """

    def __init__(
        self,
        redis_client: Redis,
        pg_pool: asyncpg.Pool,
        crawl_stream: str,
        crawl_group: str,
        links_queue: str,
        batch_size: int = 200
    ):
        self.redis_client = redis_client
        self.pg_pool = pg_pool
        self.batch_size = batch_size
        self.crawl_stream = crawl_stream
        self.crawl_group = crawl_group
        self.links_queue = links_queue

        # Event to manage the run loop
        self.stop_event = asyncio.Event()
        # Mutex to prevent calling run multiple times
        self.run_mutex = asyncio.Lock()

    async def ensure_stream_group(self) -> None:
        try:
            # "$" = only new entries from now on; mkstream=True creates the stream if absent
            await self.redis_client.xgroup_create(name=self.crawl_stream, groupname=self.crawl_group, id="$", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error(f"Stream {self.crawl_stream} for group {self.crawl_group} doesn't exist and couldn't be created.")
                raise

    async def run(self, poll_interval: float = 1.0) -> None:
        if self.run_mutex.locked():
            logger.error("QueueManager is already running. No more run() calls permitted!")
            return

        async with self.run_mutex:
            self.stop_event.clear()
            # might be started before any crawlers so needs to ensure the group is active as well
            await self.ensure_stream_group()
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
        if not self.run_mutex.locked():
            logger.error("Received stop signal without QueueManager running!")
            return
        logger.info("Received stop signal ...")
        self.stop_event.set()
        
    async def _drain_batch(self) -> list[dict]:
        """Atomically pop up to batch_size raw JSON payloads off the list."""
        async with self.redis_client.pipeline(transaction=True) as pipe:
            pipe.lrange(self.links_queue, 0, self.batch_size - 1)
            pipe.ltrim(self.links_queue, self.batch_size, -1)
            raw_items, _ = await pipe.execute()

        batch = []
        for item in raw_items:
            try:
                batch.append(json.loads(item))
            except (json.JSONDecodeError, TypeError):
                logger.error(f"Dropping malformed unprocessed_links entry: {item!r}")
        return batch

    async def _drain_and_dispatch(self) -> int:
        batch = await self._drain_batch()
        if not batch:
            return 0

        # flatten {origin_id, urls: [...]} entries into (origin_id, url) pairs
        pairs: list[tuple[int, str]] = [(entry["origin_id"], url) for entry in batch for url in entry.get("urls", [])]
        if not pairs:
            return 0

        urls = [url for _, url in pairs]

        async with self.pg_pool.acquire() as conn:
            async with conn.transaction():
                # upsert-and-detect-new in one round trip: `xmax = 0` is true
                # only for rows that were actually inserted by this statement,
                # false for rows that hit the ON CONFLICT path.
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

                # bulk edge insert -- rely on the (origin_id, destination_id)
                # primary key so ON CONFLICT DO NOTHING absorbs duplicates for free
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

        if new_rows:
            async with self.redis_client.pipeline(transaction=True) as pipe:
                for row in new_rows:
                    pipe.xadd(self.crawl_stream, {"id": str(row["id"]), "url": row["link"]})
                await pipe.execute()

        logger.info(f"Batch: {len(pairs)} links seen, {len(new_rows)} new URLs dispatched to {self.crawl_stream}.")
        return len(pairs)