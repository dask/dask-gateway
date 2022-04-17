from functools import partial, wraps

from aiohttp import web

from . import models
from .backends.base import PublicException


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


def _wrap_authenticated(handler, user=False, token=False):
    if not user and not token:
        return handler

    @wraps(handler)
    async def inner(request):
        if token:
            backend = request.app["backend"]
            auth_header = request.headers.get("Authorization")
            if auth_header:
                auth_type, auth_token = auth_header.split(" ", 1)
                if auth_type == "token":
                    cluster_name = request.match_info["cluster_name"]
                    cluster = await backend.get_cluster(cluster_name)
                    if cluster is not None and cluster.token == auth_token:
                        # Use `none` to indicate api token authenticated
                        request["user"] = None
                        return await handler(request)
            if not user:
                raise web.HTTPUnauthorized(headers={"WWW-Authenticate": "token"})

        if user:
            auth = request.app["authenticator"]
            return await auth.authenticate_and_handle(request, handler)

    return inner


def api_handler(handler=None, user_authenticated=False, token_authenticated=False):
    if handler is None:
        return partial(
            api_handler,
            user_authenticated=user_authenticated,
            token_authenticated=token_authenticated,
        )

    handler = _wrap_authenticated(
        handler, user=user_authenticated, token=token_authenticated
    )

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


@default_routes.get("/api/version")
@api_handler
async def version(request):
    version = request.app["gateway"].version_info()
    return web.json_response(version, status=200)


@default_routes.get("/api/v1/options")
@api_handler(user_authenticated=True)
async def cluster_options(request):
    user = request["user"]
    backend = request.app["backend"]
    cluster_options = await backend.get_cluster_options(user)
    spec = cluster_options.get_specification()
    return web.json_response({"cluster_options": spec})


@default_routes.get("/api/v1/clusters/")
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
    clusters = await backend.list_clusters(username=user.name, statuses=statuses)
    return web.json_response({c.name: c.to_dict() for c in clusters})


@default_routes.post("/api/v1/clusters/")
@api_handler(user_authenticated=True)
async def create_cluster(request):
    user = request["user"]
    backend = request.app["backend"]
    msg = await request.json()
    cluster_options = msg.get("cluster_options") or {}

    try:
        cluster_name = await backend.start_cluster(user, cluster_options)
    except PublicException as exc:
        reason = str(exc)
        request.app["log"].info(
            "Error creating new cluster for user %s: %s", user.name, reason
        )
        raise web.HTTPUnprocessableEntity(reason=reason)
    return web.json_response({"name": cluster_name}, status=201)


def _parse_query_flag(val):
    if val is None:
        return False
    elif val == "":
        return True
    else:
        try:
            return bool(int(val))
        except Exception:
            return False


@default_routes.get("/api/v1/clusters/{cluster_name}")
@api_handler(user_authenticated=True)
async def get_cluster(request):
    user = request["user"]
    cluster_name = request.match_info["cluster_name"]
    backend = request.app["backend"]
    wait = request.query.get("wait")
    wait = _parse_query_flag(wait)
    cluster = await backend.get_cluster(cluster_name, wait=wait)
    if cluster is None:
        raise web.HTTPNotFound(reason=f"Cluster {cluster_name} not found")
    if not user.has_permissions(cluster):
        raise web.HTTPForbidden(
            reason=f"User {user.name} lacks permissions to view cluster {cluster_name}"
        )
    return web.json_response(cluster.to_dict())


@default_routes.delete("/api/v1/clusters/{cluster_name}")
@api_handler(user_authenticated=True, token_authenticated=True)
async def delete_cluster(request):
    user = request["user"]
    cluster_name = request.match_info["cluster_name"]
    backend = request.app["backend"]
    cluster = await backend.get_cluster(cluster_name)
    if cluster is not None:
        if user is not None and not user.has_permissions(cluster):
            raise web.HTTPForbidden(
                reason=f"User {user.name} lacks permissions to stop cluster {cluster_name}"
            )
        await backend.stop_cluster(cluster_name)
    return web.Response(status=204)


@default_routes.post("/api/v1/clusters/{cluster_name}/scale")
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

    max_workers = cluster.config.get("cluster_max_workers")
    resp_msg = None
    if max_workers is not None and count > max_workers:
        resp_msg = (
            f"Scale request of {count} workers would exceed resource limit of "
            f"{max_workers} workers. Scaling to {max_workers} instead."
        )
        count = max_workers

    try:
        await backend.forward_message_to_scheduler(
            cluster, {"op": "scale", "count": count}
        )
    except PublicException as exc:
        raise web.HTTPConflict(reason=str(exc))

    return web.json_response({"ok": not resp_msg, "msg": resp_msg})


@default_routes.post("/api/v1/clusters/{cluster_name}/adapt")
@api_handler(user_authenticated=True)
async def adapt_cluster(request):
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
    minimum = msg.get("minimum", None)
    maximum = msg.get("maximum", None)
    active = msg.get("active", True)

    max_workers = cluster.config.get("cluster_max_workers")
    resp_msg = None
    if max_workers is not None:
        if maximum is None:
            maximum = max_workers
        if minimum is None:
            minimum = 0
        if maximum > max_workers or minimum > max_workers:
            orig_max = maximum
            orig_min = minimum
            maximum = min(max_workers, maximum)
            minimum = min(max_workers, minimum)
            resp_msg = (
                f"Adapt with `maximum={orig_max}, minimum={orig_min}` workers "
                f"would exceed resource limit of {max_workers} workers. Using "
                f"`maximum={maximum}, minimum={minimum}` instead."
            )

    try:
        await backend.forward_message_to_scheduler(
            cluster,
            {"op": "adapt", "minimum": minimum, "maximum": maximum, "active": active},
        )
    except PublicException as exc:
        raise web.HTTPConflict(reason=str(exc))

    return web.json_response({"ok": not resp_msg, "msg": resp_msg})


@default_routes.post("/api/v1/clusters/{cluster_name}/heartbeat")
@api_handler(token_authenticated=True)
async def handle_heartbeat(request):
    backend = request.app["backend"]
    cluster_name = request.match_info["cluster_name"]
    msg = await request.json()
    try:
        await backend.on_cluster_heartbeat(cluster_name, msg)
    except PublicException as exc:
        raise web.HTTPConflict(reason=str(exc))
    return web.Response()
