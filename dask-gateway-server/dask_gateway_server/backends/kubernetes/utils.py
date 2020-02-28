import asyncio

from traitlets import Dict, Unicode, Any, Int, Instance
from traitlets.config import LoggingConfigurable

from kubernetes_asyncio import watch, client

from ...traitlets import Callable
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


class Informer(LoggingConfigurable):
    name = Unicode()
    client = Any(None)
    method = Unicode()
    method_kwargs = Dict()
    timeout_seconds = Int(10)
    on_update = Callable()
    on_delete = Callable()

    def get(self, name, default=None):
        return self.cache.get(name, default)

    def drop(self, name):
        self.cache.pop(name, None)

    def put(self, obj):
        name = obj["metadata"]["name"]
        namespace = obj["metadata"]["namespace"]
        self.update_cache(f"{namespace}.{name}", obj)

    def update_cache(self, name, obj, deleted=False):
        if deleted:
            self.cache.pop(name)
        else:
            self.cache[name] = obj

    async def handle(self, obj, event_type="ADDED"):
        namespace = obj["metadata"]["namespace"]
        name = obj["metadata"]["name"]
        key = f"{namespace}.{name}"
        self.log.debug("Event - %s %s %s", event_type, self.name, key)

        old = self.cache.get(name)
        if event_type in {"ADDED", "MODIFIED"}:
            self.update_cache(key, obj)
            try:
                await self.on_update(obj, old=old)
            except Exception:
                self.log.warning("Error in %s informer", self.name, exc_info=True)
        elif event_type == "DELETED":
            self.update_cache(key, obj, deleted=True)
            try:
                await self.on_delete(obj)
            except Exception:
                self.log.warning("Error in %s informer", self.name, exc_info=True)

    async def start(self):
        if not hasattr(self, "_task"):
            self.cache = {}
            self.initialized = asyncio.Event()
            self._task = asyncio.ensure_future(self.run())
        await self.initialized.wait()

    async def stop(self):
        if hasattr(self, "_task"):
            await cancel_task(self._task)
            del self._task

    async def run(self):
        self.log.debug("Starting %s watch stream...", self.name)

        watch = Watch(self.client.api_client)
        method = getattr(self.client, self.method)

        while True:
            try:
                initial = await method(**self.method_kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error in %s watch stream, retrying...", self.name, exc_info=exc
                )
                continue

            initial = self.client.api_client.sanitize_for_serialization(initial)
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
                    async with watch.stream(method, **kwargs) as stream:
                        async for ev in stream:
                            ev = self.client.api_client.sanitize_for_serialization(ev)
                            await self.handle(ev["object"], event_type=ev["type"])
                            resource_version = stream.resource_version
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error in %s watch stream, retrying...", self.name, exc_info=exc
                )
        self.log.debug("%s watch stream stopped", self.name)


class Reflector(Informer):
    queue = Instance(asyncio.Queue)

    def update_cache(self, name, obj, deleted=False):
        self.cache[name] = (obj, deleted)

    async def on_update(self, obj, old=None):
        namespace = obj["metadata"]["namespace"]
        name = obj["metadata"]["name"]
        await self.queue.put(f"{namespace}.{name}")

    on_delete = on_update


def merge_json_objects(a, b):
    """Merge two JSON objects recursively.

    - If a dict, keys are merged, preferring ``b``'s values
    - If a list, values from ``b`` are appended to ``a``

    Copying is minimized. No input collection will be mutated, but a deep copy
    is not performed.

    Parameters
    ----------
    a, b : dict
        JSON objects to be merged.

    Returns
    -------
    merged : dict
    """
    if b:
        # Use a shallow copy here to avoid needlessly copying
        a = a.copy()
        for key, b_val in b.items():
            if key in a:
                a_val = a[key]
                if isinstance(a_val, dict) and isinstance(b_val, dict):
                    a[key] = merge_json_objects(a_val, b_val)
                elif isinstance(a_val, list) and isinstance(b_val, list):
                    a[key] = a_val + b_val
                else:
                    a[key] = b_val
            else:
                a[key] = b_val
    return a
