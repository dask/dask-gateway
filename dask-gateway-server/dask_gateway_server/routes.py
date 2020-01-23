from aiohttp import web

from . import models


@web.middleware
async def error_middleware(request, handler):
    try:
        return await handler(request)
    except web.HTTPException as exc:
        if exc.status not in {401, 403}:
            return web.json_response({"error": exc.reason}, status=exc.status)
        raise


@web.middleware
async def auth_middleware(request, handler):
    if getattr(handler, "_gateway_auth", False):
        auth = request.app["authenticator"]
        return await auth.authenticate_and_handle(request, handler)
    else:
        return await handler(request)


default_middleware = [error_middleware, auth_middleware]

default_routes = web.RouteTableDef()


def authenticated(handler):
    """Require this request handler to be authenticated"""
    handler._gateway_auth = True
    return handler


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
    user = request["user"]
    backend = request.app["backend"]
    status = request.query.get("status")
    if status is None:
        statuses = None
    else:
        try:
            statuses = [models.ClusterStatus.from_name(k) for k in status.split(",")]
        except Exception as exc:
            raise web.HTTPUnprocessableEntity(reason=str(exc))
    await backend.list_clusters(user=user, statuses=statuses)
    return web.json_response({})
