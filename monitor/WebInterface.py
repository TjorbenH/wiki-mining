import asyncio
import logging
from pathlib import Path

from aiohttp import web

from DataEndpoint import DataEndpoint


logger = logging.getLogger(__name__)

# single self-contained page (inline CSS/JS, no external requests); polls /api/stats
INDEX_HTML = Path(__file__).parent / "static" / "index.html"

class WebInterface:
    """ HTTP server for the monitor. Serves the DataEndpoint's latest snapshot.
            /           web page
            /api/stats  JSON snapshot
            /health     200 while the monitor's poll loop is alive, 503 otherwise (used by the compose healthcheck)
    Start/stop with run() and stop()
    """

    def __init__(
        self,
        data_endpoint: DataEndpoint,
        host: str = "0.0.0.0",
        # main.py passes MONITOR_PORT, which compose also uses for the ports mapping and healthcheck
        port: int = 5000,
    ):
        self.data_endpoint = data_endpoint
        self.host = host
        self.port = port

        self.app = web.Application()
        self.app.router.add_get("/", self._index)
        self.app.router.add_get("/api/stats", self._stats)
        self.app.router.add_get("/health", self._health)

        # Event to manage the run loop
        self.stop_event = asyncio.Event()
        # Mutex to prevent calling run multiple times
        self.run_mutex = asyncio.Lock()

    async def run(self) -> None:
        if self.run_mutex.locked():
            logger.error("WebInterface is already running. No more run() calls permitted!")
            return

        async with self.run_mutex:
            self.stop_event.clear()
            runner = web.AppRunner(self.app, access_log=None)
            await runner.setup()
            try:
                await web.TCPSite(runner, self.host, self.port).start()
                logger.info(f"WebInterface up on {self.host}:{self.port}.")
                await self.stop_event.wait()
            finally:
                await runner.cleanup()
            logger.info("WebInterface stopped.")

    def stop(self) -> None:
        if not self.run_mutex.locked():
            logger.error("Received stop signal without WebInterface running!")
            return
        logger.info("Received stop signal ...")
        self.stop_event.set()

    async def _index(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(INDEX_HTML)

    async def _stats(self, request: web.Request) -> web.Response:
        return web.json_response(self.data_endpoint.get_stats())

    async def _health(self, request: web.Request) -> web.Response:
        healthy = self.data_endpoint.is_healthy()
        return web.json_response({"healthy": healthy}, status=200 if healthy else 503)
