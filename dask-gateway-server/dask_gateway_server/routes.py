from functools import wraps

from aiohttp import web


default_routes = web.RouteTableDef()


def authenticated(handler):
    """Require this request handler to be authenticated"""

    @wraps(handler)
    async def inner(request):
        auth = request.app["authenticator"]
        return await auth.authenticate_and_handle(request, handler)

    return inner


@default_routes.get("/api/health")
async def health(request):
    health = await request.app["gateway"].health()
    if health["status"] == "fail":
        status = 503
    else:
        status = 200
    return web.json_response(health, status=status)


@default_routes.get("/api/clusters/")
@authenticated
async def clusters(request):
    return web.json_response({})
