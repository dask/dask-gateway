from functools import wraps, partial

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


def _wrap_token_authenticated(handler):
    @wraps(handler)
    async def inner(request):
        backend = request.app["backend"]

        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": "token"})

        auth_type, auth_token = auth_header.split(" ", 1)
        if auth_type != "token":
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": "token"})

        cluster_name = request.match_info["cluster_name"]
        cluster = await backend.get_cluster(cluster_name)
        if cluster is None:
            raise web.HTTPNotFound(reason=f"Cluster {cluster_name} not found")

        if cluster.token != auth_token:
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": "token"})

        return await handler(request)

    return inner


def api_handler(handler=None, user_authenticated=False, token_authenticated=False):
    if handler is None:
        return partial(
            api_handler,
            user_authenticated=user_authenticated,
            token_authenticated=token_authenticated,
        )

    if user_authenticated:
        handler = _wrap_authenticated(handler)
    elif token_authenticated:
        handler = _wrap_token_authenticated(handler)

    return _wrap_error_handler(handler)


default_routes = web.RouteTableDef()


@default_routes.get("/api/health")
@api_handler
async def health(request):
    health = await request.app["gateway"].health()
    if health["status"] == "fail":
        status = 503
    else:
        status = 200
    return web.json_response(health, status=status)


@default_routes.get("/api/clusters/")
@api_handler(user_authenticated=True)
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
@api_handler(user_authenticated=True)
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
@api_handler(user_authenticated=True)
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
@api_handler(user_authenticated=True)
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


@default_routes.post("/api/clusters/{cluster_name}/scale")
@api_handler(user_authenticated=True)
async def scale_cluster(request):
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

    msg = await request.json()
    count = msg.get("count", None)
    if not isinstance(count, int) or count < 0:
        raise web.HTTPUnprocessableEntity(
            reason=f"Scale expects a non-negative integer, got {count}"
        )

    try:
        await backend.forward_message_to_scheduler(
            cluster, {"op": "scale", "count": count}
        )
    except Exception as exc:
        raise web.HTTPConflict(reason=str(exc))
    return web.Response()


@default_routes.post("/api/clusters/{cluster_name}/heartbeat")
@api_handler(token_authenticated=True)
async def handle_heartbeat(request):
    backend = request.app["backend"]
    cluster_name = request.match_info["cluster_name"]
    msg = await request.json()
    await backend.on_cluster_heartbeat(cluster_name, msg)
    return web.Response()


@default_routes.get("/api/clusters/{cluster_name}/addresses")
@api_handler(token_authenticated=True)
async def handle_addresses(request):
    cluster_name = request.match_info["cluster_name"]
    backend = request.app["backend"]
    cluster = await backend.get_cluster(cluster_name)
    if cluster is None:
        raise web.HTTPNotFound(reason=f"Cluster {cluster_name} not found")
    return web.json_response(
        {
            "api_address": cluster.api_address,
            "dashboard_address": cluster.dashboard_address,
            "scheduler_address": cluster.scheduler_address,
        }
    )
