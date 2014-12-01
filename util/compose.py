"""
Utilities for FIX messaging.

Every message begins with BeginString and BodyLength, in that order.

The BodyLength field is defined as the number of bytes following the trailing
BodyLength delimiter, up to and including the delimiter preceeding the
checksum field.

The CheckSum is calculated by first, summing the binary value of each character
(byte) from the start of the entire FIX message (up to and including the
delimiter preceeding the CheckSum field), then taking the result of sum modulo
256.

For the purposes of this module, the FIX header will be defined as only the
first two fields, BeginString and BodyLength. The Body will be everything that
follows the BodyLength trailing delimiter, up to and including the delimiter
preceeding the CheckSum field.

Order for composing the FIX message:
*Compile the body fields
*Find the BodyLength value
*Add the header to the body
*Calculate the CheckSum value
*Add the CheckSum field to the existing message

"""
import time
import asyncio
import logging
import functools
from datetime import datetime, timedelta

from . import SOH, TAGS, MSGS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logger.addHandler(console)


#------------------------------------------------------------------------------

test_message = {
        'BeginString': b'FIX.4.4',
        'BodyLength':None,
        'MsgType': b'A',
        'MsgSeqNum': b'1',
        'SenderCompID': b'T-bitmaker',
        'TargetCompID': b'COINSETTERFIX',
        'EncryptMethod': b'0',
        'HeartBtInt': b'30',
        'ResetSeqNumFlag': b'Y',
        'Username': b'bitmaker',
        'Password': b'password',
        }

#------------------------------------------------------------------------------

def SendingTime():
	sending_time = datetime.utcnow().\
	        strftime("%Y%m%d-%H:%M:%S.%f")[:-3].encode('utf-8')
	
	return sending_time

#------------------------------------------------------------------------------

def find_checksum_as_bytes(message):
    checksum = sum([i for i in message]) % 256
    
    checksum_str = str(checksum).zfill(3)
    checksum_as_bytes = checksum_str.encode('utf-8')
    
    return checksum_as_bytes

#------------------------------------------------------------------------------

def compile_field(name=None, value=None):
    field = TAGS[name] + b'=' + value + SOH
    
    return field

#------------------------------------------------------------------------------

def get_body_fields(**kwargs):
    body_fields = {}
    
    for k, v in kwargs.items():
        if  (k == 'BeginString' or k == b'BeginString') or \
            (k == 'BodyLength' or k == b'BodyLength'):
            pass
        else:
            body_fields[k] = v
    
    return body_fields

#------------------------------------------------------------------------------

def get_header_fields(**kwargs):
    pass

#------------------------------------------------------------------------------

def compile_body(**kwargs):
    body = b''
    for field, value in kwargs.items():
        try:
            body = body + TAGS[field] + b'=' + value + SOH
        except KeyError as e:
            pass
    
    return body

#------------------------------------------------------------------------------

def compile_header(**kwargs):
    header = b''
    tip = b''
    
    try:
        tip = tip + compile_field('BeginString', kwargs['BeginString'])
    except KeyError as e:
        tip = tip + compile_field('BeginString', b'FIX.4.4')
    
    tip = tip + compile_field('BodyLength', b'')

#------------------------------------------------------------------------------
"""
The following decorator does two things:
1. Applies rate limiting.
2. Applies coroutine functionality if not already present in the decorated
   function.
"""
def rate_limit(rate, period=None):
    class Limiter:
        def __init__(self, f):
            functools.update_wrapper(self, f)
            
            self.__func = f
            self._is_coroutine = True
            self.__calling_func = None
            
            try:
                if self.__func._is_coroutine:
                    
                    def temp(self, *args, **kwargs):
                        elapsed_time = datetime.now() - self._last_time_called
                        
                        if elapsed_time < self._min_interval:
                            time_to_wait = self._min_interval - elapsed_time
                            yield from asyncio.sleep(time_to_wait.total_seconds())
                        
                        func_return = yield from self.__func(*args, **kwargs)
                        self._last_time_called = datetime.now()
                        
                        return func_return
                        
                    self.__calling_func = temp
                    
                else:
                    raise AttributeError
                
            except AttributeError as e:
                
                def temp(self, *args, **kwargs):
                    elapsed_time = datetime.now() - self._last_time_called
                    
                    if elapsed_time < self._min_interval:
                        time_to_wait = self._min_interval - elapsed_time
                        yield from asyncio.sleep(time_to_wait.total_seconds())
                    
                    func_return = self.__func(*args, **kwargs)
                    self._last_time_called = datetime.now()
                    
                    return func_return
                
                self.__calling_func = temp
            
            self._rate = rate
            self._period = period
            
            if not period:
                self._min_interval = timedelta(seconds=rate)
            else:
                self._min_interval = timedelta(seconds=(period / rate))
            
            self._last_time_called = datetime.now()
        
        @asyncio.coroutine
        def __call__(self, *args, **kwargs):
            return self.__calling_func(self, *args, **kwargs)
    
    return Limiter

#------------------------------------------------------------------------------

class CompileError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def compile_message_original(**kwargs):
    try:
        all_fields = frozenset(kwargs)
        cumm_fields = set()
        
        FIX_fields = []
        
        # Assemble Header
        FIX_fields.append(compile_field('BeginString', kwargs['BeginString']))
        cumm_fields.add('BeginString')
        
        FIX_fields.append('BodyLength will go here')
        cumm_fields.add('BodyLength')
        
        FIX_fields.append(compile_field('MsgType', kwargs['MsgType']))
        cumm_fields.add('MsgType')
        
        FIX_fields.append(compile_field('SenderCompID', kwargs['SenderCompID']))
        cumm_fields.add('SenderCompID')
        
        FIX_fields.append(compile_field('TargetCompID', kwargs['TargetCompID']))
        cumm_fields.add('TargetCompID')
        
        FIX_fields.append(compile_field('MsgSeqNum', kwargs['MsgSeqNum']))
        cumm_fields.add('MsgSeqNum')
        
        try:
            FIX_fields.append(compile_field('SendingTime', kwargs['SendingTime']))
        except KeyError as e:
            FIX_fields.append(compile_field('SendingTime', SendingTime()))
        cumm_fields.add('SendingTime')
        
        # Assemble Body
        body_fields = all_fields - cumm_fields
        for field in body_fields:
            FIX_fields.append(compile_field(field, kwargs[field]))
        
        # Calculate BodyLength
        BodyLength = 0
        for field in FIX_fields[2:]:    # Exclude BeginString and BodyLength
            BodyLength = BodyLength + len(field)
        BodyLength = str(BodyLength).encode('utf-8')
        FIX_fields[1] = compile_field('BodyLength', BodyLength)
        
        # Compile the FIX_fields entries in a message
        FIX_message = b''
        for field in FIX_fields:
            FIX_message = FIX_message + field
        
        # Calculate the CheckSum
        CheckSum = find_checksum_as_bytes(FIX_message)
        
        # Add the CheckSum field to the FIX message
        FIX_message = FIX_message + compile_field('CheckSum', CheckSum)
        
        return FIX_message
    
    except KeyError as e:
        raise ParseError("{}, {}".format(type(e), e.args))


def compile_message(fields, MsgSeqNum):
    try:
        message = b''
        partial_message = b''
        
        header_start = b'8=FIX.4.4\x019='
        
        partial_message = partial_message + fields.pop(0)
        
        MsgSeqNum_field = b'34=' + MsgSeqNum + b'\x01'
        
        partial_message += MsgSeqNum_field
        
        for field in fields:
            partial_message += field
        
        BodyLength = len(partial_message)
        
        header = header_start + str(BodyLength).encode('utf-8') + b'\x01'
        
        message = header + partial_message
        
        # Calculate the CheckSum
        CheckSum = find_checksum_as_bytes(message)
        
        # Add the CheckSum field to the FIX message
        FIX_message = message + b'10=' + CheckSum + b'\x01'
        
        return FIX_message
    
    except Exception as e:
        raise ParseError("{}, {}".format(type(e), e.args))

#------------------------------------------------------------------------------
