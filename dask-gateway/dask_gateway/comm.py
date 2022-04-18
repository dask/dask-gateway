import ssl
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

from distributed.comm.core import Connector
from distributed.comm.registry import Backend, backends
from distributed.comm.tcp import (
    MAX_BUFFER_SIZE,
    TLS,
    convert_stream_closed_error,
    get_stream_address,
)
from distributed.utils import ensure_ip, get_ip
from tornado import netutil
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient


def parse_gateway_address(address):
    if not address.startswith("gateway://"):
        address = "gateway://" + address
    parsed = urlparse(address)
    if not parsed.path:
        raise ValueError("Gateway address %r missing path component" % address)
    path = parsed.path.strip("/")
    return parsed.hostname, parsed.port, path


class GatewayConnector(Connector):
    _executor = ThreadPoolExecutor(2)
    _resolver = netutil.ExecutorResolver(close_executor=False, executor=_executor)
    client = TCPClient(resolver=_resolver)

    async def connect(self, address, deserialize=True, **connection_args):
        ip, port, path = parse_gateway_address(address)
        sni = "daskgateway-" + path
        ctx = connection_args.get("ssl_context")
        if not isinstance(ctx, ssl.SSLContext):
            raise TypeError(
                "Gateway expects a `ssl_context` argument of type "
                "ssl.SSLContext, instead got %s" % ctx
            )

        try:
            plain_stream = await self.client.connect(
                ip, port, max_buffer_size=MAX_BUFFER_SIZE
            )
            stream = await plain_stream.start_tls(
                False, ssl_options=ctx, server_hostname=sni
            )
            if stream.closed() and stream.error:
                raise StreamClosedError(stream.error)

        except StreamClosedError as e:
            # The socket connect() call failed
            convert_stream_closed_error(self, e)

        local_address = "tls://" + get_stream_address(stream)
        peer_address = "gateway://" + address
        return TLS(stream, local_address, peer_address, deserialize)


class GatewayBackend(Backend):
    # I/O
    def get_connector(self):
        return GatewayConnector()

    def get_listener(self, *args, **kwargs):
        raise NotImplementedError("Listening on a gateway connection")

    # Address handling
    def get_address_host(self, loc):
        return parse_gateway_address(loc)[0]

    def get_address_host_port(self, loc):
        return parse_gateway_address(loc)[:2]

    def resolve_address(self, loc):
        host, port, path = parse_gateway_address(loc)
        host = ensure_ip(host)
        return "%s:%d/%s" % (host, port, path)

    def get_local_address_for(self, loc):
        host, port, path = parse_gateway_address(loc)
        host = ensure_ip(host)
        host = get_ip(host)
        return "%s:%d/%s" % (host, port, path)


backends["gateway"] = GatewayBackend()
