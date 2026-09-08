import asyncio
import logging

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
    Start/stop with run() and stop()
    """

    def __init__(
        self,
        redis_client: Redis,
        dispatcher_stream: str,
        dispatcher_group: str,
        crawl_stream: str,
        queue_manager: QueueManager,
        pg_pool: asyncpg.Pool,
        hostname: str,
        pending_min_idle_ms: int = 60_000,
        pending_batch_size: int = 200,
        retry_batch_size: int = 100,
        retry_max_attempts: int = 5,
        retry_base_delay_seconds: int = 60,
    ):
        self.redis_client = redis_client
        self.dispatcher_stream = dispatcher_stream
        self.dispatcher_group = dispatcher_group
        self.crawl_stream = crawl_stream
        
        self.queue_manager = queue_manager

        self.pg_pool = pg_pool

        # in theory more than one dispatch container can run, so we need to id it for the redis queue
        self.hostname = hostname
        self.consumer_name = f"{self.hostname}-reclaimer"

        self.pending_min_idle_ms = pending_min_idle_ms
        self.pending_batch_size = pending_batch_size
        self.retry_batch_size = retry_batch_size
        self.retry_max_attempts = retry_max_attempts
        self.retry_base_delay_seconds = retry_base_delay_seconds

        # Event to manage the run loop
        self.stop_event = asyncio.Event()
        # Mutex to prevent calling run multiple times
        self.run_mutex = asyncio.Lock()

    async def run(self, pending_poll_interval: float = 30.0, retry_poll_interval: float = 60.0) -> None:
        """ Start both reclaim loops. Each backs off for its own poll_interval whenever a cycle finds nothing to do."""
        if self.run_mutex.locked():
            logger.error("ReclaimManager is already running. No more run() calls permitted!")
            return

        async with self.run_mutex:
            self.stop_event.clear()
            # ensure communication with the crawlers is possible
            await self.queue_manager.ensure_stream_group(stream=self.dispatcher_stream, group=self.dispatcher_group)
            logger.info("ReclaimManager started.")

            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._pending_reclaim_loop(pending_poll_interval))
                tg.create_task(self._failed_retry_loop(retry_poll_interval))

            logger.info("ReclaimManager stopped.")

    def stop(self) -> None:
        """Safely stop the Reclaim Manager."""
        if not self.run_mutex.locked():
            logger.error("Received stop signal without ReclaimManager running!")
            return
        logger.info("Received stop signal ...")
        self.stop_event.set()

    async def _pending_reclaim_loop(self, poll_interval: float) -> None:
        while not self.stop_event.is_set():
            try:
                n_processed = await self._reclaim_pending_batch()
            except Exception as e:
                logger.exception(f"ReclaimManager pending-reclaim cycle failed: {e}")
                n_processed = 0

            if n_processed == 0:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    pass

    async def _failed_retry_loop(self, poll_interval: float) -> None:
        while not self.stop_event.is_set():
            try:
                n_processed = await self._retry_failed_batch()
            except Exception as e:
                logger.exception(f"ReclaimManager failed-retry cycle failed: {e}")
                n_processed = 0

            if n_processed == 0:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    pass

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
