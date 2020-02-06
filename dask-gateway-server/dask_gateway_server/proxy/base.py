import asyncio
import json
from contextlib import contextmanager

from aiohttp import web


class Proxy(object):
    def __init__(self, capacity=10):
        self.routes = {}
        self.offset = 0
        self.events = []
        self.capacity = capacity
        self._watchers = set()
        self._next_id = 1

    def _get_id(self):
        o = self._next_id
        self._next_id += 1
        return o

    def append_event(self, kind, route):
        event = {"id": self._get_id(), "type": kind, "route": route}

        if len(self.events) >= self.capacity:
            n_dropped = self.capacity // 2
            self.events = self.events[n_dropped:]
            self.offset += n_dropped

        self.events.append(event)

        for q in self._watchers:
            q.put_nowait([event])

    def put_route(self, kind, sni=None, path=None, target=None):
        if target is None:
            raise ValueError("must specify `target`")

        if kind == "SNI":
            if sni is None:
                raise ValueError("must specify `sni`")
            route = {"kind": kind, "sni": sni, "target": target}
            self.routes[(kind, sni)] = route
        elif kind == "PATH":
            if path is None:
                raise ValueError("must specify `path`")
            route = {"kind": kind, "path": path, "target": target}
            self.routes[(kind, path)] = route
        else:
            raise ValueError(f"Unknown route kind {kind}")
        self.append_event("PUT", route)

    def delete_route(self, kind, sni=None, path=None):
        if kind == "SNI":
            if sni is None:
                raise ValueError("must specify `sni`")
            route = self.routes.pop((kind, sni), None)
            if route is not None:
                route = {"kind": kind, "sni": sni}
        elif kind == "PATH":
            if path is None:
                raise ValueError("must specify `path`")
            route = self.routes.pop((kind, path), None)
            if route is not None:
                route = {"kind": kind, "path": path}
        else:
            raise ValueError(f"Unknown route kind {kind}")

        if route is not None:
            self.append_event("DELETE", route)

    def events_after(self, last_id=0):
        index = last_id - self.offset
        if index >= 0:
            events = self.events[index:]
            return events
        else:
            # We've since dropped these events, client needs to reset
            return None

    def get_routes(self):
        routes = list(self.routes.values())
        return {"routes": routes, "id": self._next_id - 1}

    @contextmanager
    def watch_after(self, last_id=0):
        events = self.events_after(last_id)
        if events is None:
            raise web.HTTPGone()

        queue = asyncio.Queue()
        if events:
            queue.put_nowait(events)
        self._watchers.add(queue)
        yield queue
        self._watchers.discard(queue)


async def put(request):
    msg = await request.json()
    request.app["proxy"].put_route(**msg)
    return web.Response()


async def delete(request):
    msg = await request.json()
    request.app["proxy"].delete_route(**msg)
    return web.Response()


async def get(request):
    proxy = request.app["proxy"]
    watch = request.query.get("watch", False)

    if watch:
        last_id = request.query.get("last_id", 0)
        try:
            last_id = int(last_id)
        except Exception:
            raise web.HTTPUnprocessableEntity(reason="invalid `last_id`: %s" % last_id)

        with proxy.watch_after(last_id) as queue:
            response = web.StreamResponse(headers={"Content-Type": "application/json"})
            await response.prepare(request)
            while True:
                events = await queue.get()
                msg = {"events": events}
                print(msg)
                await response.write(json.dumps(msg).encode() + b"\n")
        await response.write_eof()
        return response
    else:
        msg = proxy.get_routes()
        return web.json_response(msg)


app = web.Application()
app["proxy"] = Proxy()

app.add_routes(
    [web.put("/routes", put), web.delete("/routes", delete), web.get("/routes", get)]
)

web.run_app(app)
