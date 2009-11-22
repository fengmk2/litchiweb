#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Implement wrapper socket with non-blocking I/O base on the normal socket.

Using the 'Coroutine Trampolining' magic.
"""
from socket import socket, AF_INET, SOCK_STREAM

from litchi.systemcall import ReadWait, WriteWait


_socketmethods = (
    'bind', 'connect', 'connect_ex', 'fileno', 'listen',
    'getpeername', 'getsockname', 'getsockopt', 'setsockopt',
    'sendall', 'setblocking',
    'settimeout', 'gettimeout', 'shutdown',
    'close', 'dup', 'makefile', )

class Socket(object):
    
    def __init__(self, family=AF_INET, type=SOCK_STREAM, proto=0, _sock=None):
        if _sock is not None:
            self.sock = _sock
        else:
            self.sock = socket(family, type, proto)
        self.sock.setblocking(False)
        
    def accept(self):
        yield ReadWait(self.sock)
        client, addr = self.sock.accept()
        yield Socket(_sock=client), addr
        
    def send(self, buffer):
        while buffer:
            yield WriteWait(self.sock)
            len = self.sock.send(buffer)
            buffer = buffer[len:]
            
    def recv(self, maxbytes):
        yield ReadWait(self.sock)
        yield self.sock.recv(maxbytes)
        
    family = property(lambda self: self._sock.family, doc="the socket family")
    type = property(lambda self: self._sock.type, doc="the socket type")
    proto = property(lambda self: self._sock.proto, doc="the socket protocol")

    _s = ("def %s(self, *args): return self.sock.%s(*args)\n\n"
          "%s.__doc__ = socket.%s.__doc__\n")
    for _m in _socketmethods:
        exec _s % (_m, _m, _m, _m)
    del _m, _s