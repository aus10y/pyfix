from . import SOH, TAGS, MSGS

import logging
#import functools
import asyncio
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logger.addHandler(console)

#------------------------------------------------------------------------------

def verify_checksum(message):
    try:
        checksum_field = message[-7:-1]
        tag, checksum = checksum_field.split(b'=')
        
        return (sum(message[:-7]) % 256) == int(checksum)
    except ValueError:
        raise ParseError("Invalid checksum")

#------------------------------------------------------------------------------

def checksum_is_present(message):
    checksum_field = message[-7:-1]
    
    try:
        tag, checksum = checksum_field.split(b'=')
    except ValueError as e:
        return False
    
    if tag == b'10':
        return True
    else:
        return False

#------------------------------------------------------------------------------

def verify_bodylength(message):
    try:
        first_index = message.index(b'\x01') + 1
        second_index = first_index + message[first_index:].index(b'\x01') + 1
        
        # '-7' so that the checksum field is not counted in bodylength
        calc_length = len(message[second_index:-7])
        
        body_length_index = first_index + message[first_index:].index(b'=') + 1
        
        recv_length = int(message[body_length_index:second_index - 1])
        
        return recv_length == calc_length
    except ValueError:
        return False

#------------------------------------------------------------------------------

def verify_initial_fields(message):
    # The first three fields must always be the same
    # Not currently checking the contents of each field, only the tag
    
    # Check for BeginString tag
    if message[0] != ord(b'8'):
        return False
    
    # Check for BodyLength tag
    first_index = message.index(b'\x01') + 1
    if message[first_index] != ord(b'9'):
        return False
    
    # Check for MsgType tag
    second_index = first_index + message[first_index:].index(b'\x01') + 1
    if message[second_index: second_index + 2] != b'35':
        return False
    
    return True

#------------------------------------------------------------------------------

def initial_fields(message):
    # Parse and return a tuple containing the values of the first three fields
    # Throw a ParseError in the event of failure
    
    first_three_tags = ()
    first_three_values = ()
    
    field_index = 0
    for i, byte in enumerate(message):
        if byte == ord(b'\x01'):
            try:
                tag, value = message[field_index:i].split(b'=')
            except (ValueError, KeyError) as e:
                raise ParseError("{}, {}".format(type(e), e.args))
            first_three_tags = first_three_tags + (tag,)
            first_three_values = first_three_values + (value,)
            field_index = i + 1
            if len(first_three_tags) == 3:
                break
    
    if len(first_three_tags) != 3:
        raise ParseError("Unable to find first three fields")
    
    if (
            first_three_tags[0] != b'8' or \
            first_three_tags[1] != b'9' or \
            first_three_tags[2] != b'35'
        ):
        raise ParseError("Invalid initial tags: {}".format(first_three_tags))
    
    return first_three_values


#------------------------------------------------------------------------------

def sans_first_two_fields(message):
    num_found = 0
    index = 0
    
    for i, byte in enumerate(message):
        if byte == ord(b'\x01'):
            num_found+=1
            if num_found == 2:
                index = i + 1
    
    return message[index:]
    

#------------------------------------------------------------------------------

class ParseError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def decompile(message):
    sections = message.split(SOH)
    if len(sections) == 1:
        raise ParseError('Malformed FIX message')
    
    fields = {}
    
    try:
        for section in sections[:-1]:
            tag, data = section.split(b'=')
            fields[TAGS[tag]] = data
        
        return fields
    except (ValueError, KeyError) as e:
        raise ParseError("{}, {}".format(type(e), e.args))

#------------------------------------------------------------------------------

def is_complete(message):
    try:
        BeginString, BodyLength, MsgType = initial_fields(message)
        
        if not checksum_is_present(message):
            return False
        
        return True
        
    except ParseError as e:
        return False

#------------------------------------------------------------------------------

def header_is_present(message):
    # make beginning field version agnostic
    beginning = b'8=FIX.'
    if beginning == message[:6]:
        return True
    else:
        return False

#------------------------------------------------------------------------------

