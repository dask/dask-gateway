from aiohttp import web


default_routes = web.RouteTableDef()


@default_routes.get("/api/health")
async def health(request):
    health = await request.app["gateway"].health()
    if health["status"] == "fail":
        status = 503
    else:
        status = 200
    return web.json_response(health, status=status)
