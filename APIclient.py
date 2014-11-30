"""
The FIX engine API client module.

This module provides methods for external modules to produce exchange specific
methods.


Command Structure:
{"type":<type>, "command": <command name>, "kwargs":{...}}

Valid types: "FIX", "engine"


Don't send BeginString or BodyLength.


"""
import socket

from __init__ import SOCKET_args, CONNECTION_args

def init_client():
    #socket.setdefaulttimeout(0)
    sock = socket.socket(*SOCKET_args)
    #sock.setblocking(False)
    #sock.settimeout(0)
    #sock = socket.create_connection(CONNECTION_args, timeout=0)
    sock.connect(CONNECTION_args)
    sock.settimeout(0)
    
    return sock

def build_command(command, data):
    msg_dict = {}
    msg_dict['type'] = command
    msg_dict['kwargs'] = data
    
    msg_str = str(msg_dict)
    msg = msg_str.encode('utf-8')
    
    return msg
    

def command(socket, command, data):
    cmd = build_command(command, data)
    
    try:
        socket.send(cmd)
        
        return True
    except:
        return False

