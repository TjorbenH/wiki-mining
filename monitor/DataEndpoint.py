import asyncio
import logging
import time
from datetime import datetime, timezone

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ResponseError


logger = logging.getLogger(__name__)

SCRAPING_STATUSES = ("queued", "in_progress", "done", "failed")


def _iso(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, timezone.utc).isoformat()


class DataEndpoint:
    """ Periodically fetch and cache the state of the system (DB, Redis queue etc.)
        Provide raw stats as well as completion detection.
    Start/stop with run() and stop()
    """

    def __init__(
        self,
        redis_client: Redis,
        pg_pool: asyncpg.Pool,
        crawl_stream: str,
        crawl_group: str,
        dispatcher_stream: str,
        dispatcher_group: str,
        retry_max_attempts: int = 5,
        poll_interval: float = 10.0,
        history_minutes: int = 60,
        completion_confirm_s: float = 60.0,
        stall_after_s: float = 600.0,
    ):
        self.redis_client = redis_client
        self.pg_pool = pg_pool
        
        self.crawl_stream = crawl_stream
        self.crawl_group = crawl_group
        self.dispatcher_stream = dispatcher_stream
        self.dispatcher_group = dispatcher_group

        self.retry_max_attempts = retry_max_attempts
        self.poll_interval = poll_interval # delay between to polls
        self.history_minutes = history_minutes # how many per minute series are stored
        self.completion_confirm_s = completion_confirm_s # how long the completed state has to be held for it to actually count
        self.stall_after_s = stall_after_s # how long a crawl can have no progress before being called stalled

        self.snapshot: dict = {}
        self.last_error: str | None = None
        self.last_cycle_at: float | None = None

        # completion detection state, carried between polls
        self._progress_fingerprint: tuple | None = None
        self._last_progress_mono = time.monotonic()
        self._last_progress_wall = time.time()
        self._idle_since: tuple[float, tuple] | None = None  # (monotonic, entries_added of both streams)

        # Event to manage the run loop
        self.stop_event = asyncio.Event()
        # Mutex to prevent calling run multiple times
        self.run_mutex = asyncio.Lock()

    async def run(self) -> None:
        """ Poll Postgres and Redis every poll_interval seconds until stop() is called.
        A failed poll keeps the previous snapshot and records the error in last_error.
        """
        if self.run_mutex.locked():
            logger.error("DataEndpoint is already running. No more run() calls permitted!")
            return

        async with self.run_mutex:
            self.stop_event.clear()
            # counts as a cycle, so the healthcheck has a reference point before the first poll finishes
            self.last_cycle_at = time.monotonic()
            logger.info("DataEndpoint started.")

            while not self.stop_event.is_set():
                try:
                    self.snapshot = await self._collect()
                    self.last_error = None
                except Exception as e:
                    logger.exception(f"DataEndpoint poll cycle failed: {e}")
                    self.last_error = f"{type(e).__name__}: {e}"
                self.last_cycle_at = time.monotonic()

                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=self.poll_interval)
                except asyncio.TimeoutError:
                    pass

            logger.info("DataEndpoint stopped.")

    def stop(self) -> None:
        if not self.run_mutex.locked():
            logger.error("Received stop signal without DataEndpoint running!")
            return
        logger.info("Received stop signal ...")
        self.stop_event.set()

    def is_healthy(self) -> bool:
        """ True while the poll loop is still cycling. Database/Redis hickups don't matter here. """
        if self.last_cycle_at is None:
            return False
        return time.monotonic() - self.last_cycle_at < 3 * self.poll_interval + 30

    def get_stats(self) -> dict:
        """ Latest snapshot plus the error from the last cycle, if it failed (the snapshot is then stale)."""
        return {**self.snapshot, "last_error": self.last_error}

    async def _collect(self) -> dict:
        """ One poll: read both streams and the DB, then evaluate completion on the result."""
        now = time.time()
        streams = {
            "crawl": await self._stream_stats(self.crawl_stream, self.crawl_group),
            "dispatcher": await self._stream_stats(self.dispatcher_stream, self.dispatcher_group),
        }
        rawdata, scraped_per_minute = await self._db_stats()

        snapshot = {
            "collected_at": _iso(now),
            "config": {
                "poll_interval_s": self.poll_interval,
                "retry_max_attempts": self.retry_max_attempts,
                "history_minutes": self.history_minutes,
            },
            "streams": streams,
            "rawdata": rawdata,
            "per_minute": self._per_minute(now, scraped_per_minute),
        }
        snapshot["completion"] = self._evaluate_completion(snapshot)
        return snapshot

    async def _stream_stats(self, stream: str, group: str) -> dict:
        """ Length and counters of a stream plus its consumer group and that group's consumers."""
        try:
            info = await self.redis_client.xinfo_stream(stream)
        except ResponseError as e:
            if "no such key" not in str(e).lower():
                raise
            return {"name": stream, "exists": False, "group": None}

        groups = await self.redis_client.xinfo_groups(stream)
        g = next((g for g in groups if g["name"] == group), None)
        group_stats = None
        if g is not None:
            consumers = await self.redis_client.xinfo_consumers(stream, group)
            entries_read = g.get("entries-read")
            if entries_read is None and g["last-delivered-id"] == "0-0":
                # Redis reports nil until the group's first read
                entries_read = 0
            group_stats = {
                "name": group,
                # delivered to a consumer but not acked yet
                "pending": g["pending"],
                # entries never delivered to this group
                "lag": g.get("lag"),
                "entries_read": entries_read,
                # XAUTOCLAIM moves entries between consumers without re-reading them, so this holds
                "acked": entries_read - g["pending"] if entries_read is not None else None,
                "last_delivered_id": g["last-delivered-id"],
                "consumers": [
                    {
                        "name": c["name"],
                        "pending": c["pending"],
                        "idle_ms": c["idle"],
                        "inactive_ms": c.get("inactive"),
                    }
                    for c in consumers
                ],
            }

        return {
            "name": stream,
            "exists": True,
            "length": info.get("length"),
            "entries_added": info.get("entries-added"),
            "last_generated_id": info.get("last-generated-id"),
            "group": group_stats,
        }

    async def _db_stats(self) -> tuple[dict, dict[int, int]]:
        """ RawData counts by status and pages scraped per minute.
            Returns (rawdata dict for the snapshot, {epoch_minute: pages scraped})."""
        async with self.pg_pool.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                status_rows = await conn.fetch(
                    """
                    SELECT scraping_status, count(*) AS n, count(*) FILTER (WHERE attempts < $1) AS retryable
                    FROM RawData
                    GROUP BY scraping_status;
                    """,
                    self.retry_max_attempts,
                )
                # for the scraped per minute stats
                scraped_rows = await conn.fetch(
                    """
                    SELECT floor(extract(epoch FROM updated_at) / 60)::bigint AS minute, count(*) AS n
                    FROM RawData
                    WHERE scraping_status = 'done' AND updated_at >= now() - make_interval(mins => $1)
                    GROUP BY 1;
                    """,
                    self.history_minutes,
                )

        status = {s: 0 for s in SCRAPING_STATUSES}
        failed_retryable = 0
        for row in status_rows:
            status[row["scraping_status"]] = row["n"]
            if row["scraping_status"] == "failed":
                failed_retryable = row["retryable"]

        rawdata = {
            "total": sum(status.values()),
            "status": status,
            "failed_retryable": failed_retryable,
            "failed_exhausted": status["failed"] - failed_retryable,
        }
        return rawdata, {row["minute"]: row["n"] for row in scraped_rows}

    def _per_minute(self, now: float, scraped: dict[int, int]) -> list[dict]:
        """ Pages scraped per minute over the last history_minutes, with empty minutes filled in as 0.
            Oldest first; the last entry is the current, still partial minute."""
        current = int(now // 60)
        return [
            {"minute": _iso(minute * 60), "scraped": scraped.get(minute, 0)}
            for minute in range(current - self.history_minutes + 1, current + 1)
        ]

    @staticmethod
    def _group_drained(stream: dict) -> bool:
        """ True if the stream exists and its group has nothing undelivered (lag) or unacked (pending)."""
        group = stream.get("group")
        return bool(stream["exists"] and group and group["pending"] == 0 and group["lag"] == 0)

    def _evaluate_completion(self, snapshot: dict) -> dict:
        """ running / complete / stalled (no progress for stall_after_s seconds) / not_started.
        Completed state needs to be held for completion_confirm_s seconds to avoid false positives. 
        """
        now_mono = time.monotonic()
        crawl, dispatcher = snapshot["streams"]["crawl"], snapshot["streams"]["dispatcher"]
        rawdata = snapshot["rawdata"]

        conditions = {
            "streams_drained": self._group_drained(crawl) and self._group_drained(dispatcher),
            "no_open_rows": rawdata["status"]["queued"] == 0 and rawdata["status"]["in_progress"] == 0,
            "no_retryable_failures": rawdata["failed_retryable"] == 0,
        }

        entries_added = (crawl.get("entries_added"), dispatcher.get("entries_added"))
        fingerprint = (
            entries_added,
            tuple((s.get("group") or {}).get("entries_read") for s in (crawl, dispatcher)),
            tuple(rawdata["status"].values()),
            rawdata["failed_retryable"],
        )
        if fingerprint != self._progress_fingerprint:
            self._progress_fingerprint = fingerprint
            self._last_progress_mono = now_mono
            self._last_progress_wall = time.time()

        idle = all(conditions.values())
        if not idle:
            self._idle_since = None
        elif self._idle_since is None or self._idle_since[1] != entries_added:
            self._idle_since = (now_mono, entries_added)

        if rawdata["total"] == 0:
            state = "not_started"
        elif idle and now_mono - self._idle_since[0] >= self.completion_confirm_s:
            state = "complete"
        elif now_mono - self._last_progress_mono >= self.stall_after_s:
            state = "stalled"
        else:
            state = "running"

        return {
            "state": state,
            "conditions": conditions,
            "last_progress_at": _iso(self._last_progress_wall),
            "stall_after_s": self.stall_after_s,
        }
