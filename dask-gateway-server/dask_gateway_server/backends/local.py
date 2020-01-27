import asyncio

from .db_base import DatabaseBackend


class LocalBackend(DatabaseBackend):
    async def handle_cluster_start(self, cluster):
        yield {"hello": 1}
        await asyncio.sleep(2)
        yield {"hello": 2}

    async def handle_cluster_stop(self, cluster):
        self.log.info("handle_cluster_stop")
        await asyncio.sleep(2)


class UnsafeLocalBackend(LocalBackend):
    pass
