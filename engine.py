"""
This is the FIX engine.

The engine handles communication between an exchange and a local process. The
communicates to local processes via a socket server.

"""
from __init__ import API_address, API_port

import os
import ssl
import asyncio
import logging
import logging.handlers

from FIXclient import FIXclient
from APIserver import APIserver


#------------------------------------------------------------------------------
# Set up logging

if not os.path.exists('./logs/'):
    os.mkdir('./logs/')

LOG_FILENAME = './logs/FIX_Engine.log'

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

file_handler = logging.handlers.RotatingFileHandler(LOG_FILENAME,
                                               maxBytes=1048576,
                                               backupCount=9,
                                               )
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)

formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console)

#------------------------------------------------------------------------------
# Start your engines

loop = asyncio.get_event_loop()

api_server_coro = loop.create_server(
        APIserver,
        API_address,
        API_port,
        )

server = loop.run_until_complete(api_server_coro)

logger.info('Serving API on {}'.format(server.sockets[0].getsockname()))

try:
    loop.run_forever()
except KeyboardInterrupt:
    print('\n',end='')
    logger.info("Caught KeyboardInterrupt")
except Exception as e:
    logger.exception()
finally:
    server.close()
    loop.close()
    logger.info("Exiting\n\n")