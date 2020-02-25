import asyncio

from traitlets import Dict, Unicode, Any, Int, Instance
from traitlets.config import LoggingConfigurable

from kubernetes_asyncio import watch, client

from ...utils import cancel_task


# Monkeypatch kubernetes_asyncio to cleanup resources better
client.rest.RESTClientObject.__del__ = lambda self: None


class Watch(watch.Watch):
    def __init__(self, api_client=None, return_type=None):
        self._raw_return_type = return_type
        self._stop = False
        self._api_client = api_client or client.ApiClient()
        self.resource_version = 0
        self.resp = None


DELETED = type(
    "DELETED", (object,), dict.fromkeys(["__repr__", "__reduce__"], lambda s: "DELETED")
)()


class Reflector(LoggingConfigurable):
    kind = Unicode()
    method = Any()
    method_kwargs = Dict()
    timeout_seconds = Int(10)
    api_client = Instance(client.ApiClient)
    cache = Dict()
    queue = Any(None)
    initialized = Instance(asyncio.Event, args=())

    def get(self, name, default=None):
        return self.cache.get(name, default)

    def pop(self, name, default=None):
        self.cache.pop(name, default)

    def put(self, name, value):
        self.cache[name] = value

    async def handle(self, obj, event_type="INITIAL"):
        name = obj["metadata"]["name"]
        self.log.debug("Received %s event for %s[%s]", event_type, self.kind, name)
        if event_type in {"INITIAL", "ADDED", "MODIFIED"}:
            self.cache[name] = obj
        elif event_type == "DELETED":
            self.cache[name] = DELETED
        if self.queue is not None:
            await self.queue.put((self.kind, name))

    async def start(self):
        if not hasattr(self, "_task"):
            self._task = asyncio.ensure_future(self.run())
        await self.initialized.wait()

    async def stop(self):
        if hasattr(self, "_task"):
            await cancel_task(self._task)
            del self._task

    async def run(self):
        self.log.debug("Starting %s watch stream...", self.kind)

        watch = Watch(self.api_client)

        while True:
            try:
                initial = await self.method(**self.method_kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error in %s watch stream, retrying...", self.kind, exc_info=exc
                )

            initial = self.api_client.sanitize_for_serialization(initial)
            for obj in initial["items"]:
                await self.handle(obj)
            self.initialized.set()
            resource_version = initial["metadata"]["resourceVersion"]

            try:
                while True:
                    kwargs = {
                        "resource_version": resource_version,
                        "timeout_seconds": self.timeout_seconds,
                    }
                    kwargs.update(self.method_kwargs)
                    async with watch.stream(self.method, **kwargs) as stream:
                        async for ev in stream:
                            ev = self.api_client.sanitize_for_serialization(ev)
                            await self.handle(ev["object"], event_type=ev["type"])
                            resource_version = stream.resource_version
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error in %s watch stream, retrying...", self.kind, exc_info=exc
                )
        self.log.debug("%s watch stream stopped", self.kind)
