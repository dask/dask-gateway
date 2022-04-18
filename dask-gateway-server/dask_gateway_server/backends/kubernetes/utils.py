import asyncio
import functools
import json
from datetime import datetime, timezone

from kubernetes_asyncio import client
from traitlets import Any, Dict, Int, Unicode
from traitlets.config import LoggingConfigurable

from ...traitlets import Callable
from ...utils import cancel_task

# Monkeypatch kubernetes_asyncio to cleanup resources better
client.rest.RESTClientObject.__del__ = lambda self: None


def parse_k8s_timestamp(ts):
    t = datetime.fromisoformat(ts[:-1])
    return int(t.replace(tzinfo=timezone.utc).timestamp() * 1000)


def k8s_timestamp():
    t = datetime.now(timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


class RateLimitedClient:
    """A rate limited wrapper around a kubernetes client"""

    def __init__(self, client, rate_limiter):
        self.rate_limiter = rate_limiter
        self.client = client
        self._cache = {}

    @property
    def api_client(self):
        return self.client.api_client

    def __getattr__(self, name):
        if not name.startswith("_") and hasattr(self.client, name):
            if name not in self._cache:
                method = getattr(self.client, name)

                @functools.wraps(method)
                async def func(*args, **kwargs):
                    await self.rate_limiter.acquire()
                    return await method(*args, **kwargs)

                self._cache[name] = func
            return self._cache[name]
        raise AttributeError(name)


async def watch(method, *args, **kwargs):
    """Handle a single watch call ona resource"""
    kwargs["watch"] = True
    kwargs["_preload_content"] = False

    resp = await method(*args, **kwargs)
    async with resp:
        while True:
            line = await resp.content.readline()
            line = line.decode("utf8")
            if not line:
                break
            yield json.loads(line)


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

    def get_key(self, obj):
        namespace = obj["metadata"]["namespace"]
        name = obj["metadata"]["name"]
        return f"{namespace}.{name}"

    def handle_initial(self, objs):
        obj_map = {self.get_key(i): i for i in objs}
        if self.cache:
            deleted = set(self.cache).difference(obj_map)
        else:
            deleted = ()
        self.log.debug(
            "Relisted %s informer - %d deletes, %d updates",
            self.name,
            len(deleted),
            len(objs),
        )
        for key in deleted:
            obj = self.cache.pop(key)
            try:
                self.on_delete(obj)
            except Exception:
                self.log.warning("Error in %s informer", self.name, exc_info=True)
        for key, obj in obj_map.items():
            old = self.cache.get(key)
            self.cache[key] = obj
            try:
                self.on_update(obj, old=old)
            except Exception:
                self.log.warning("Error in %s informer", self.name, exc_info=True)

    def handle(self, obj, event_type="ADDED"):
        key = self.get_key(obj)
        self.log.debug("Event - %s %s %s", event_type, self.name, key)

        old = self.cache.get(key)
        if event_type in {"ADDED", "MODIFIED"}:
            self.cache[key] = obj
            try:
                self.on_update(obj, old=old)
            except Exception:
                self.log.warning("Error in %s informer", self.name, exc_info=True)
        elif event_type == "DELETED":
            self.cache.pop(key, None)
            try:
                self.on_delete(obj)
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
        self.log.debug("Starting %s informer...", self.name)

        method = getattr(self.client, self.method)

        while True:
            try:
                initial = await method(**self.method_kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error in %s informer, retrying...", self.name, exc_info=exc
                )
                continue

            initial = self.client.api_client.sanitize_for_serialization(initial)
            self.handle_initial(initial["items"])
            self.initialized.set()
            res_ver = initial["metadata"]["resourceVersion"]

            try:
                while True:
                    relist = False
                    kwargs = {
                        "resource_version": res_ver,
                        "timeout_seconds": self.timeout_seconds,
                    }
                    kwargs.update(self.method_kwargs)
                    async for ev in watch(method, **kwargs):
                        typ = ev["type"]
                        obj = ev["object"]
                        if typ.lower() == "error":
                            code = obj.get("code", 0)
                            if code == 410:
                                self.log.debug(
                                    "Too old resourceVersion in %s informer, relisting",
                                    self.name,
                                )
                            else:
                                self.log.warning(
                                    (
                                        "Unexpected error in %s informer, relisting: "
                                        "message=%r, code=%d"
                                    ),
                                    obj.get("message"),
                                    code,
                                )
                            relist = True
                            break
                        try:
                            res_ver = ev["object"]["metadata"]["resourceVersion"]
                        except KeyError:
                            pass
                        self.handle(obj, typ)
                    if relist:
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error in %s informer, retrying...", self.name, exc_info=exc
                )
        self.log.debug("%s informer stopped", self.name)


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
