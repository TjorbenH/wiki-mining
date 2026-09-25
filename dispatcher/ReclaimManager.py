import asyncio
import logging
from collections.abc import Awaitable, Callable

import asyncpg
from redis.asyncio import Redis

from QueueManager import QueueManager


logger = logging.getLogger(__name__)

class ReclaimManager:
    """ Recovers work that QueueManager and the crawlers lost track of (usually due to failures):
        1. XAUTOCLAIMs dispatcher_stream entries stuck in the consumer group's pending list
           (a dispatcher consumer crashed, or errored, after XREADGROUP but before XACK)
           and replays them through QueueManager.process_batch().
        2. Sweeps RawData rows stuck at scraping_status='failed' and re-queues them onto
           crawl_stream with exponential backoff, up to a max attempt count.
        3. Marks scrapes that died mid-flight (crawler crash) as 'failed', so sweep 2 retries them.
    Start/stop with run() and stop()
    """

    def __init__(
        self,
        redis_client: Redis,
        dispatcher_stream: str,
        dispatcher_group: str,
        crawl_stream: str,
        crawl_group: str,
        queue_manager: QueueManager,
        pg_pool: asyncpg.Pool,
        hostname: str,
        retry_max_attempts: int = 5,
        # sensible defaults, not wired to env in main.py
        pending_min_idle_ms: int = 60_000,
        pending_batch_size: int = 200,
        pending_poll_interval: float = 30.0,
        retry_batch_size: int = 100,
        retry_base_delay_seconds: int = 60,
        retry_poll_interval: float = 60.0,
        # must exceed a live crawler's worst case per URL (dominated by aiohttp's default 300s total timeout), or slow scrapes get fetched twice
        stale_min_idle_ms: int = 600_000,
        stale_batch_size: int = 100,
        stale_poll_interval: float = 60.0,
    ):
        self.redis_client = redis_client
        self.dispatcher_stream = dispatcher_stream
        self.dispatcher_group = dispatcher_group
        self.crawl_stream = crawl_stream
        self.crawl_group = crawl_group

        self.queue_manager = queue_manager

        self.pg_pool = pg_pool

        # in theory more than one dispatch container can run, so we need to id it for the redis queue
        self.hostname = hostname
        self.consumer_name = f"{self.hostname}-reclaimer"

        self.pending_min_idle_ms = pending_min_idle_ms
        self.pending_batch_size = pending_batch_size
        self.pending_poll_interval = pending_poll_interval

        self.retry_batch_size = retry_batch_size
        self.retry_max_attempts = retry_max_attempts
        self.retry_base_delay_seconds = retry_base_delay_seconds
        self.retry_poll_interval = retry_poll_interval

        self.stale_min_idle_ms = stale_min_idle_ms
        self.stale_batch_size = stale_batch_size
        self.stale_poll_interval = stale_poll_interval

        # Event to manage the run loop
        self.stop_event = asyncio.Event()
        # Mutex to prevent calling run multiple times
        self.run_mutex = asyncio.Lock()

    async def run(self) -> None:
        """ Start all reclaim loops. Each backs off for its own poll_interval whenever a cycle finds nothing to do."""
        if self.run_mutex.locked():
            logger.error("ReclaimManager is already running. No more run() calls permitted!")
            return

        async with self.run_mutex:
            self.stop_event.clear()
            # ensure communication with the crawlers is possible
            await self.queue_manager.ensure_stream_group(stream=self.dispatcher_stream, group=self.dispatcher_group)
            await self.queue_manager.ensure_stream_group(stream=self.crawl_stream, group=self.crawl_group)
            logger.info("ReclaimManager started.")

            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._reclaim_loop(self.pending_poll_interval, processor=self._reclaim_pending_batch, loop_name="pending"))
                tg.create_task(self._reclaim_loop(self.retry_poll_interval, processor=self._retry_failed_batch, loop_name="failed"))
                tg.create_task(self._reclaim_loop(self.stale_poll_interval, processor=self._reclaim_stale_batch, loop_name="stale"))

            logger.info("ReclaimManager stopped.")

    def stop(self) -> None:
        """Safely stop the Reclaim Manager."""
        if not self.run_mutex.locked():
            logger.error("Received stop signal without ReclaimManager running!")
            return
        logger.info("Received stop signal ...")
        self.stop_event.set()

    async def _reclaim_loop(self, poll_interval: float, processor: Callable[[], Awaitable[int]], loop_name: str) -> None:
        while not self.stop_event.is_set():
            try:
                n_processed = await processor()
            except Exception as e:
                logger.exception(f"ReclaimManager {loop_name}-reclaim cycle failed: {e}")
                n_processed = 0

            if n_processed == 0:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    pass

    async def _reclaim_stale_batch(self) -> int:
        """Marks scrapes that died mid-flight as 'failed' (attempts + 1).
        Two sources:
            1. crawl_stream entries left unacked past stale_min_idle_ms (crawler died between XREADGROUP and XACK).
               Their row may still be 'queued' if it died before mark_in_progress, so the DB sweep alone misses them.
            2. RawData rows stuck at 'in_progress' past stale_min_idle_ms whose message was acked anyway
        stale_min_idle_ms must exceed the longest a live crawler can take for one URL, or slow scrapes get fetched twice.
        """
        _, claimed, _ = await self.redis_client.xautoclaim(
            name=self.crawl_stream,
            groupname=self.crawl_group,
            consumername=self.consumer_name,
            min_idle_time=self.stale_min_idle_ms,
            start_id="0-0",
            count=self.stale_batch_size,
        )

        claimed_msg_ids = [msg_id for msg_id, _ in claimed]
        claimed_row_ids = []
        for msg_id, fields in claimed:
            try:
                claimed_row_ids.append(int(fields["id"]))
            except (KeyError, TypeError, ValueError):
                logger.error(f"Malformed stale {self.crawl_stream} entry {msg_id!r} fields={fields!r}. Acking without a DB update.")

        # the updated_at filter skips rows touched since the crawler died (e.g. already failed and requeued)
        # so a delayed or repeated XACK below can't fail the same scrape twice
        async with self.pg_pool.acquire() as conn:
            if claimed_row_ids:
                await conn.execute(
                    """
                    UPDATE RawData
                    SET scraping_status = 'failed', attempts = attempts + 1, updated_at = now()
                    WHERE id = ANY($1::bigint[])
                      AND scraping_status IN ('queued', 'in_progress')
                      AND updated_at < now() - ($2::numeric) * interval '1 second';
                    """,
                    claimed_row_ids,
                    self.stale_min_idle_ms / 1000,
                )

            # only ack once the rows are 'failed' -> if the update raised, the entries stay pending for the next cycle
            if claimed_msg_ids:
                await self.redis_client.xack(self.crawl_stream, self.crawl_group, *claimed_msg_ids)

            swept = await conn.fetch(
                """
                UPDATE RawData
                SET scraping_status = 'failed', attempts = attempts + 1, updated_at = now()
                WHERE id IN (
                    SELECT id FROM RawData
                    WHERE scraping_status = 'in_progress'
                      AND updated_at < now() - ($1::numeric) * interval '1 second'
                    ORDER BY updated_at
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id;
                """,
                self.stale_min_idle_ms / 1000,
                self.stale_batch_size,
            )

        if claimed_msg_ids or swept:
            logger.info(
                f"Reclaimed stale scrapes: {len(claimed_msg_ids)} unacked {self.crawl_stream} entries, "
                f"{len(swept)} rows stuck 'in_progress' > {self.stale_min_idle_ms}ms. Marked 'failed' for retry."
            )

        return len(claimed_msg_ids) + len(swept)

    async def _reclaim_pending_batch(self) -> int:
        """Claims dispatcher_stream entries that have sat unacked past pending_min_idle_ms and replays them through QueueManager.process_batch()."""
        _, claimed, deleted = await self.redis_client.xautoclaim(
            name=self.dispatcher_stream,
            groupname=self.dispatcher_group,
            consumername=self.consumer_name,
            min_idle_time=self.pending_min_idle_ms,
            start_id="0-0",
            count=self.pending_batch_size,
        )

        if deleted:
            logger.warning(f"XAUTOCLAIM reported {len(deleted)} deleted ids on {self.dispatcher_stream} (unexpected, stream isn't trimmed)")

        if not claimed:
            return 0

        batch = await self.queue_manager._parse_entries(claimed)
        n_processed = await self.queue_manager._process_batch(batch) if batch else 0

        if n_processed:
            logger.info(f"Reclaimed {len(claimed)} pending {self.dispatcher_stream} entries idle > {self.pending_min_idle_ms}ms")

        return n_processed

    async def _retry_failed_batch(self) -> int:
        """Requeues RawData rows stuck at scraping_status='failed' back onto crawl_stream,
        with exponential backoff (base_delay * 2^attempts) up to retry_max_attempts"""
        async with self.pg_pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    SELECT id, link, attempts FROM RawData
                    WHERE scraping_status = 'failed'
                      AND attempts < $1
                      AND updated_at < now() - (($2::numeric) * power(2, attempts)) * interval '1 second'
                    ORDER BY updated_at
                    LIMIT $3
                    FOR UPDATE SKIP LOCKED;
                    """,
                    self.retry_max_attempts,
                    self.retry_base_delay_seconds,
                    self.retry_batch_size,
                )

                if not rows:
                    return 0

                # push to the queue before marking the row queued in case this fails the row is still failed -> self healing
                try:
                    async with self.redis_client.pipeline(transaction=True) as pipe:
                        for row in rows:
                            pipe.xadd(self.crawl_stream, {"id": str(row["id"]), "url": row["link"]})
                        await pipe.execute()
                except Exception:
                    logger.error(f"[redis_push] failed while retrying {len(rows)} failed URLs — leaving them 'failed' for the next sweep")
                    raise

                ids = [row["id"] for row in rows]
                await conn.execute(
                    "UPDATE RawData SET scraping_status = 'queued', updated_at = now() WHERE id = ANY($1::bigint[]);",
                    ids,
                )

        logger.info(f"Retried {len(rows)} failed URLs onto {self.crawl_stream}.")
        return len(rows)
