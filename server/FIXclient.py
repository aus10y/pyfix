#import __init__

import ast
import json
import asyncio
import logging
from datetime import datetime

from util import TAGS, MSGS
from util import parse, compose

#from message import Message

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#------------------------------------------------------------------------------

class FIXclient(asyncio.Protocol):
    
    def __init__(self, HeartBtInt, SenderCompID, TargetCompID):
        
        super().__init__()
        
        self.loop = asyncio.get_event_loop()
        self.transport = None
        self.connected = False
        
        self._in_queue = asyncio.Queue()
        self._out_queue = asyncio.Queue()
        self._in_put_nowait = self._in_queue.put_nowait
        self._out_put_nowait = self._out_queue.put_nowait
        
        self._local_MsgSeqNum = 0
        self._host_MsgSeqNum = 0
        
        self.HeartBtInt = HeartBtInt
        self.SenderCompID = SenderCompID
        self.TargetCompID = TargetCompID
        self.api_connected = None
        self.api_out = lambda dummy: None
        
        self._recv_hbt = None
        self._send_hbt = None
        self._last_send_time = None
        self._last_recv_time = None
        
        self._sent_TestReqID = None
        self._recv_TestReqID = None
    
    
    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        logger.info('FIX: Connection established with {}'.format(peername))
        
        self.transport = transport
        self.connected = True
        
        self._recv_hbt = self.loop.call_later(self.HeartBtInt + 1, self.test_request)
        self._send_hbt = self.loop.call_later(self.HeartBtInt - 1, self.local_heartbeat)
        self._last_send_time = datetime.now()
        self._last_recv_time = datetime.now()
        
        asyncio.async(self.handle_incoming_data())
        asyncio.async(self.handle_outgoing_data())
        
    
    def connection_lost(self, exc):
        logger.info('FIX: Connection lost...')
        
        self.connected = False
        
        self._send_hbt.cancel()
        self._recv_hbt.cancel()
        
        self._in_put_nowait(None)
        self._out_put_nowait((None, None))
        
        # Try to reconnect if in case of accidental disconnect
        pass
    
    
    def data_received(self, msg):
        logger.debug('FIX: Data received : {}'.format(msg.decode("utf-8")))
        
        self.reset_host_heartbeat()
        
        self.put_incoming(msg)
    
    
    def eof_received(self):
        logger.info('FIX: EOF received...')
        return None
    
    
    def MsgSeqNum(self):
        self._local_MsgSeqNum +=1
        return str(self._local_MsgSeqNum).encode('utf-8')
    
    
    def reset_host_heartbeat(self):
        self._recv_hbt.cancel()
        self._recv_hbt = self.loop.call_later(self.HeartBtInt + 1, self.test_request)
    
    
    def reset_local_heartbeat(self):
        self._send_hbt.cancel()
        self._send_hbt = self.loop.call_later(self.HeartBtInt, self.local_heartbeat)
    
    
    def put_incoming(self, msg):
        self._in_put_nowait(msg)
    
    
    def put_outgoing(self, msg_data, msg_name='message'):
       self._out_put_nowait((msg_data, msg_name))
    
    
    @asyncio.coroutine
    def put_incoming_async(self, msg):
        yield from self._in_queue.put(msg)
    
    
    @asyncio.coroutine
    def put_outgoing_async(self, msg_data, msg_name='message'):
        yield from self._out_queue.put((msg_data, msg_name))
    
    
    @asyncio.coroutine
    def handle_incoming_data(self):
        logger.debug('FIX: Incoming handler started...')
        under_resend = False
        while self.connected:
            msg = yield from self._in_queue.get()
            
            if msg is None: continue
            
            if not parse.header_is_present(msg):
                logger.info("FIX: Message is malformed, improper header")
                # do stuff
                continue
            
            if not parse.checksum_is_present(msg):
                logger.info("FIX: No checksum present, will try to build message from pieces")
                while True:
                    next_msg = yield from self._in_queue.get()
                    if parse.header_is_present(next_msg):
                        # next_msg is most likely it's own message,
                        # msg was most likely a malformed message
                        logger.info("FIX: Previous message was in error")
                        
                        # do stuff here
                        pass
                        
                        break
                        
                    else:
                        # Since no header is present, we will assume 'next_msg'
                        # is part of 'msg'. They will be combined and tested.
                        msg = msg + next_msg
                        if parse.checksum_is_present(msg):
                            # We will take the message to be complete
                            break
                        else:
                            # We'll try again!
                            continue
            
            
            
            # Validate the first three TAGS of the message
            if not parse.verify_initial_fields(msg):
                logger.info('FIX: Invalid FIX message; Error in first three TAGS')
                if not under_resend:
                    #under_resend = True
                    #self.request_resend()
                    pass
                continue
            
            
            # Validate the CheckSum
            if not parse.verify_checksum(msg):
                logger.info('FIX: Invalid checksum')
                if not under_resend:
                    #under_resend = True
                    #self.request_resend()
                    pass
                continue
            
            # Validate the BodyLength
            if not parse.verify_bodylength(msg):
                logger.info('FIX: Invalid BodyLength')
                if not under_resend:
                    #under_resend = True
                    #self.request_resend()
                    pass
                continue
            
            # Decompile the message to a dict with Field Names as the keys
            try:
                msg_dict = parse.decompile(msg)
            except parse.ParseError as e:
                logger.info(e)
                if not under_resend:
                    #under_resend = True
                    #self.request_resend()
                    pass
                continue
            
            # Check to see if the MsgSeqNum is as expected
            if int(msg_dict[b'MsgSeqNum']) != self._host_MsgSeqNum + 1:
                logger.info('FIX: MsgSeqNum out of expected order')
                if not under_resend:
                    #under_resend = True
                    #self.request_resend()
                    pass
                continue
            
            # Increment the host MsgSeqNum counter
            self._host_MsgSeqNum +=1
            
            # Ensure the Resend Request flag 'under_resend' is reset
            under_resend = False
            
            # Check if the message is administrative in nature
            #raise NotImplementedError
            
            # If not administrative, send to client application
            self.api_out(msg)
            
        logger.debug('FIX: Incoming handler stopped...')
    
    
    @asyncio.coroutine
    def handle_outgoing_data(self):
        logger.debug('FIX: Outgoing handler started...')
        while self.connected:
            msg_data, msg_name = yield from self._out_queue.get()
            
            if msg_data is None: continue
            
            logger.debug('FIX: Handling outgoing data')
            
            # XXX consider testing for MsgSeqNum so that application can pass it's own
            # msg_data['MsgSeqNum'] = self.MsgSeqNum()
            
            try:
                FIX_message = compose.compile_message(msg_data, self.MsgSeqNum())
            except compose.CompileError as e:
                logger.debug('FIX: Outgoing CompileError...')
                logger.exception(e)
            self.transport.write(FIX_message)
            self.reset_local_heartbeat()
            logger.info('FIX: Sent {}: {}'.format(msg_name, FIX_message.decode('utf-8')))
        
        logger.debug('FIX: Outgoing handler stopped...')
    
    
    #@asyncio.coroutine
    def local_heartbeat(self):
        HeartBeat = []
        HeartBeat.append(b'35=0\x01')   # MsgType, HeartBeat
        HeartBeat.append(b'49=' + self.SenderCompID + b'\x01')  # SenderCompID
        HeartBeat.append(b'56=' + self.TargetCompID + b'\x01')  # TargetCompID
        HeartBeat.append(b'52=' + compose.SendingTime() + b'\x01')  # SendingTime
        
        self.put_outgoing(HeartBeat, msg_name='HeartBeat')
    
    
    #@asyncio.coroutine
    def test_request(self):
        self._sent_TestReqID = compose.SendingTime()
        TestReq = []
        TestReq.append(b'35=1\x01')   # MsgType, TestReq
        TestReq.append(b'49=' + self.SenderCompID + b'\x01')  # SenderCompID
        TestReq.append(b'56=' + self.TargetCompID + b'\x01')  # TargetCompID
        TestReq.append(b'52=' + compose.SendingTime() + b'\x01')  # SendingTime
        TestReq.append(b'112=' + self._sent_TestReqID + b'\x01')  # TestReqID
        
        self.put_outgoing(TestReq, msg_name='Test Request')
    
    
    #@asyncio.coroutine
    def request_resend(self, EndSeqNo=b'0'):
        ResendReq = []
        ResendReq.append(b'35=1\x01')   # MsgType, TestReq
        ResendReq.append(b'49=' + self.SenderCompID + b'\x01')  # SenderCompID
        ResendReq.append(b'56=' + self.TargetCompID + b'\x01')  # TargetCompID
        ResendReq.append(b'7=' + str(self._host_MsgSeqNum + 1).encode('utf-8') + b'\x01')   # BeginSeqNo
        ResendReq.append(b'16=' + EndSeqNo + b'\x01')
        
        self.put_outgoing(ResendReq, msg_name='Resend Request')

#------------------------------------------------------------------------------

