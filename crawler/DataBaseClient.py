import asyncpg

class DataBaseClient:
    """ Helper Class to neatly package all interactions between the ScraperWorker and the PostgreSQL DB."""
    
    def __init__(self, pg_pool: asyncpg.Pool):
        self.pg_pool = pg_pool

    async def mark_in_progress(self, url_id: int) -> None:
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                "UPDATE RawData SET scraping_status = 'in_progress', updated_at = now() WHERE id = $1;",
                url_id,
            )
 
    async def mark_done(self, url_id: int, storage_key: str, content_hash: str) -> None:
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE RawData
                SET storage_key = $1, content_hash = $2, scraping_status = 'done', updated_at = now()
                WHERE id = $3;
                """,
                storage_key, content_hash, url_id,
            )
 
    async def mark_failed(self, url_id: int) -> None:
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE RawData
                SET scraping_status = 'failed', attempts = attempts + 1, updated_at = now()
                WHERE id = $1;
                """,
                url_id,
            )

    async def get_domain_whitelist(self) -> dict[str, str | None]:
        """Returns {domain: robots_txt} for every domain seed.py has whitelisted."""
        async with self.pg_pool.acquire() as conn:
            rows = await conn.fetch("SELECT domain, robots_txt FROM DomainsWhitelist;")
        return {row["domain"]: row["robots_txt"] for row in rows}