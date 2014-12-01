import __init__

import ast
import asyncio
import logging
import time
import ssl

from .FIXclient import FIXclient
#import test

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#------------------------------------------------------------------------------

class APIserver(asyncio.Protocol):
    
    def __init__(self, FIX_handler=None, engine_handler=None):
        super().__init__()
        
        self.loop = asyncio.get_event_loop()
        
        self.connected = False
        self.transport = None
        
        self.FIX_transport = None
        self.FIX_protocol = None
        
        self._in_queue = asyncio.Queue()
        self._out_queue = asyncio.Queue()
        self._in_put_nowait = self._in_queue.put_nowait
        self._out_put_nowait = self._out_queue.put_nowait
        
        self.commands = {}
        self.commands['FIX_connect'] = self.FIX_connect
        self.commands['FIX_disconnect'] = self.FIX_disconnect
        self.commands['FIX'] = None
        
    
    def connection_made(self, transport):
        logger.info('API: Connection established...')
        
        self.connected = True
        self.transport = transport
        
        asyncio.async(self.handle_incoming())
        asyncio.async(self.handle_outgoing())
    
    
    def connection_lost(self, exc):
        logger.info('API: Connection lost...')
        
        self.connected = False
        
        self._in_put_nowait(None)
        self._out_put_nowait(None)
    
    
    def data_received(self, data):
        logger.debug('API: Data received: {}'.format(data.decode('utf-8')))
        
        self.put_incoming(data)
    
    
    def eof_received(self):
        logger.info('API: EOF received...')
        return None
    
    
    def put_incoming(self, msg):
        self._in_put_nowait(msg)
    
    
    def put_outgoing(self, msg):
        self._out_put_nowait(msg)
    
    
    @asyncio.coroutine
    def handle_incoming(self):
        logger.debug('API: Incoming handler started...')
        while self.connected:
            data = yield from self._in_queue.get()
            
            if data is None: continue
            
            command = ast.literal_eval(data.decode('utf-8'))
            
            logger.debug("API: Handling command '{}'".format(command['type']))
            
            try:
                handler = self.commands[command['type']]
                try:
                    kwargs = command['kwargs']
                    try:
                        yield from handler(kwargs)
                    except KeyError as e:
                        logger.error('API: ...Error while handling')
                        logger.exception(e)
                except KeyError as e:
                    logger.error('API: ...kwargs not found')
                    logger.exception(e)
            except KeyError as e:
                logger.error('API: ...Command not found')
                logger.exception(e)
        logger.debug('API: Incoming handler stopped...')
    
    
    @asyncio.coroutine
    def handle_outgoing(self):
        logger.debug('API: Outgoing handler started...')
        while self.connected:
            data = yield from self._out_queue.get()
            
            if data is None: continue
            
            logger.debug('API: Handling outgoing data')
            
            self.transport.write(data)
            
            logger.info('API: Sent: {}'.format(data.decode('utf-8')))
        logger.debug('API: Outgoing handler stopped...')
    
    
    @asyncio.coroutine
    def FIX_connect(self, settings, *args, **kwargs):
        if settings['SSL']:
            sslcontext = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        else:
            sslcontext = None
        
        try:
            client = FIXclient(
                    HeartBtInt=settings['HeartBtInt'],
                    SenderCompID=settings['SenderCompID'],
                    TargetCompID=settings['TargetCompID']
                    )
            
            logger.debug('API: Creating FIX connection...')
            
            self.FIX_transport, self.FIX_protocol = yield from \
                    self.loop.create_connection(
                            lambda: client,
                            host=settings['host'],
                            port=settings['port'],
                            ssl=sslcontext
                            )
            
            self.commands['FIX'] = self.FIX_protocol.put_outgoing_async
            self.FIX_protocol.api_out = self.put_outgoing
            
        except Exception as e:
            logger.error('API: Problem creating FIX connection')
            logger.exception(e)
    
    
    @asyncio.coroutine
    def FIX_disconnect(self, *args, **kwargs):
        try:
            self.FIX_transport.close()
        except AttributeError as e:
            logger.debug("API: FIX connection already closed")
        
        self.FIX_transport = None
        self.FIX_protocol = None
    

#------------------------------------------------------------------------------
