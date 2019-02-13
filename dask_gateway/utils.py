import socket


def random_port():
    """Get a single random port."""
    with socket.socket() as sock:
        sock.bind(('', 0))
        return sock.getsockname()[1]
