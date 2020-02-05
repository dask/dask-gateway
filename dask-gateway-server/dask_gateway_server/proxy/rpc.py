import asyncio
import itertools
import json
import struct


uint32 = struct.Struct("!L")


class RPCError(Exception):
    pass


class AuthError(Exception):
    pass


async def new_channel(addr, token, *, loop=None, **kwargs):
    """Create a new channel.

    Parameters
    ----------
    addr : tuple or str
        The address to connect to.
    token : str
        The auth token to use.
    loop : AbstractEventLoop, optional
        The event loop to use.
    **kwargs
        All remaining arguments are forwarded to ``loop.create_connection``.

    Returns
    -------
    channel : Channel
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    def factory():
        return ChannelProtocol(loop=loop, token=token)

    if isinstance(addr, tuple):
        connect = loop.create_connection
        args = (factory,) + addr
    elif isinstance(addr, str):
        connect = loop.create_unix_connection
        args = (factory, addr)
    else:
        raise ValueError("Unknown address type: %s" % addr)

    _, p = await connect(*args, **kwargs)

    return p.channel


class ChannelProtocol(asyncio.Protocol):
    def __init__(self, token=None, *, loop=None):
        super().__init__()
        self.token = token
        self.transport = None
        self.channel = None
        self._buffer = bytearray()
        self._read_size = None
        self._loop = loop
        self._paused = False
        self._drain_waiter = None
        self._connection_lost = None
        self._authenticated = False

    def connection_made(self, transport):
        self.transport = transport
        self.channel = Channel(self, transport, self.token, loop=self._loop)

    def connection_lost(self, exc=None):
        if exc is None:
            exc = ConnectionResetError("Connection closed")
        self.channel._set_exception(exc)
        self._connection_lost = exc

        if self._paused:
            waiter = self._drain_waiter
            if waiter is not None:
                self._drain_waiter = None
                if not waiter.done():
                    waiter.set_exception(exc)

    def data_received(self, data):
        self._buffer.extend(data)
        while True:
            if not self._authenticated:
                if len(self._buffer) >= uint32.size:
                    data = self._buffer[: uint32.size]
                    self._buffer = self._buffer[uint32.size :]
                    ok = uint32.unpack_from(data)[0]
                    if ok:
                        self.channel._auth_future.set_result(None)
                        self._authenticated = True
                    else:
                        self.channel._set_exception(
                            AuthError("Failed to authenticate with proxy")
                        )
                else:
                    break

            if self._read_size is None and len(self._buffer) >= uint32.size:
                self._read_size = uint32.unpack_from(self._buffer)[0] + uint32.size
            if self._read_size and len(self._buffer) >= self._read_size:
                data = self._buffer[uint32.size : self._read_size]
                self._buffer = self._buffer[self._read_size :]
                self._read_size = None
                msg = json.loads(data)
                self.channel._append_msg(msg)
            else:
                break

    def eof_received(self):
        self.channel._set_exception(ConnectionResetError())

    def pause_writing(self):
        self._paused = True

    def resume_writing(self):
        self._paused = False

        waiter = self._drain_waiter
        if waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    async def drain(self):
        if self._paused and not self._connection_lost:
            self._drain_waiter = self._loop.create_future()
            await self._drain_waiter


class Channel(object):
    """A communication channel between two endpoints.

    Use ``new_channel`` to create a channel.
    """

    def __init__(self, protocol, transport, token, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop
        self.token = token

        self._id_iter = itertools.count()
        self._active_reqs = {}
        self._waiter = None
        self._exception = None
        self._auth_future = loop.create_future()

    async def __aenter__(self):
        return self

    async def __aexit__(self, typ, value, traceback):
        await self.close()
        if isinstance(value, ConnectionResetError):
            return True

    async def authenticate(self):
        self._transport.write(self.token.encode())
        await self._auth_future

    async def call(self, method, **kwargs):
        """Send a request and wait for a response."""
        if self._exception is not None:
            raise self._exception

        msg_id = next(self._id_iter)
        msg = {"id": msg_id, "method": method, "params": kwargs}
        data = json.dumps(msg).encode()
        len_bytes = uint32.pack(len(data))
        self._transport.write(len_bytes + data)

        reply = self._active_reqs[msg_id] = self._loop.create_future()
        await self._protocol.drain()
        return await reply

    def _close(self):
        if self._transport is not None:
            transport = self._transport
            self._transport = None
            return transport.close()

    async def close(self):
        """Close the channel and release all resources.

        It is invalid to use this channel after closing.

        This method is idempotent.
        """
        self._close()
        try:
            futs = self._active_reqs.values()
            await asyncio.gather(*futs, return_exceptions=True)
        except asyncio.CancelledError:
            pass

    def _append_msg(self, msg):
        id = msg.get("id")
        if id is None:
            return
        fut = self._active_reqs.pop(id)
        if not fut.done():
            result = msg.get("result")
            if result is not None:
                fut.set_result(result)
            else:
                error = msg.get("error", "Invalid response")
                fut.set_exception(RPCError(error))

    def _set_exception(self, exc):
        self._exception = exc

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_exception(exc)

        for msg in self._active_reqs.values():
            if not msg.done():
                msg.set_exception(exc)

        if not self._auth_future.done():
            self._auth_future.set_exception(exc)

        self._close()
