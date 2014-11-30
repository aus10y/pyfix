import logging
import socket

asyncio_logger = logging.getLogger('asyncio')
asyncio_logger.setLevel(logging.ERROR)

API_address = 'localhost'
API_port = 8888

CONNECTION_args = (API_address, API_port)

SOCKET_FAMILY = socket.AF_INET
SOCKET_TYPE = socket.SOCK_STREAM

SOCKET_args = (SOCKET_FAMILY, SOCKET_TYPE)