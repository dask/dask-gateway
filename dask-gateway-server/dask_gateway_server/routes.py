from functools import wraps

from aiohttp import web


default_routes = web.RouteTableDef()


def authenticated(handler):
    """Require this request handler to be authenticated"""

    @wraps(handler)
    async def inner(request):
        user = await request.app["gateway"].authenticate(request)
        request["user"] = user
        return await handler(request)

    return inner


@default_routes.get("/api/health")
async def health(request):
    health = await request.app["gateway"].health()
    if health["status"] == "fail":
        status = 503
    else:
        status = 200
    return web.json_response(health, status=status)


@default_routes.get("/api/hello")
@authenticated
async def hello(request):
    return web.json_response({"user": request["user"].name})
