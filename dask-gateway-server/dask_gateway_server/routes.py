from functools import wraps

from aiohttp import web

from . import models


def _wrap_error_handler(handler):
    @wraps(handler)
    async def inner(request):
        try:
            return await handler(request)
        except web.HTTPException as exc:
            headers = exc.headers
            headers.popall("Content-Type", None)
            return web.json_response(
                {"error": exc.reason}, headers=headers, status=exc.status
            )

    return inner


def _wrap_authenticated(handler):
    @wraps(handler)
    async def inner(request):
        auth = request.app["authenticator"]
        return await auth.authenticate_and_handle(request, handler)

    return inner


def api_handler(handler=None, authenticated=True):
    if handler is None:
        return lambda handler: api_handler(handler, authenticated=authenticated)

    if authenticated:
        handler = _wrap_authenticated(handler)
    return _wrap_error_handler(handler)


default_routes = web.RouteTableDef()


@default_routes.get("/api/health")
@api_handler(authenticated=False)
async def health(request):
    health = await request.app["gateway"].health()
    if health["status"] == "fail":
        status = 503
    else:
        status = 200
    return web.json_response(health, status=status)


@default_routes.get("/api/clusters/")
@api_handler
async def list_clusters(request):
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
    clusters = await backend.list_clusters(user=user, statuses=statuses)
    return web.json_response({c.name: c.to_dict() for c in clusters})


@default_routes.post("/api/clusters/")
@api_handler
async def create_cluster(request):
    user = request["user"]
    backend = request.app["backend"]
    msg = await request.json()
    cluster_options = msg.get("cluster_options") or {}

    try:
        cluster_name = await backend.start_cluster(user, cluster_options)
    except Exception as exc:
        reason = str(exc)
        request.app["log"].warning(
            "Error creating new cluster for user %s: %s", user.name, reason
        )
        raise web.HTTPUnprocessableEntity(reason=reason)

    return web.json_response({"name": cluster_name}, status=201)


@default_routes.get("/api/clusters/{cluster_name}")
@api_handler
async def get_cluster(request):
    user = request["user"]
    cluster_name = request.match_info["cluster_name"]
    backend = request.app["backend"]
    cluster = await backend.get_cluster(cluster_name)
    if cluster is None:
        raise web.HTTPNotFound(reason=f"Cluster {cluster_name} not found")
    if not user.has_permissions(cluster):
        raise web.HTTPForbidden(
            reason=f"User {user.name} lacks permissions to view cluster {cluster_name}"
        )
    return web.json_response(cluster.to_dict())


@default_routes.delete("/api/clusters/{cluster_name}")
@api_handler
async def delete_cluster(request):
    user = request["user"]
    cluster_name = request.match_info["cluster_name"]
    backend = request.app["backend"]
    cluster = await backend.get_cluster(cluster_name)
    if cluster is not None:
        if not user.has_permissions(cluster):
            raise web.HTTPForbidden(
                reason=f"User {user.name} lacks permissions to stop cluster {cluster_name}"
            )
        await backend.stop_cluster(cluster_name)
    return web.Response(status=204)
