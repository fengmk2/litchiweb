#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Implement wrapper socket with non-blocking I/O base on the normal socket.

Using the 'Coroutine Trampolining' magic.
"""
from socket import socket, AF_INET, SOCK_STREAM

from litchi.systemcall import ReadWait, WriteWait, Sleep


_socketmethods = (
    'bind', 'connect', 'connect_ex', 'fileno', 'listen',
    'getpeername', 'getsockname', 'getsockopt', 'setsockopt',
    'sendall', 'setblocking',
    'settimeout', 'gettimeout', 'shutdown',
    'dup', 'makefile', 'close')

class Socket(object):
    """A non-blocking socket warp class. It only support TCP, not work at UDP currently."""
    def __init__(self, family=AF_INET, type=SOCK_STREAM, proto=0, _sock=None):
        if _sock is not None:
            self.sock = _sock
        else:
            self.sock = socket(family, type, proto)
        self.sock.setblocking(0)
        self._read_buffer = ''
        
    def accept(self):
        yield ReadWait(self.sock)
        client, addr = self.sock.accept()
        yield Socket(_sock=client), addr
        
    def send(self, buffer):
        while buffer:
            yield WriteWait(self.sock)
            len = self.sock.send(buffer)
            buffer = buffer[len:]
    
    def read_until(self, delimiter):
        """yield the result until socket read the given delimiter."""
        while True:
            yield self.recv()
            loc = self._read_buffer.find(delimiter)
            if loc != -1:
                yield self._consume(loc + len(delimiter))
                break
            
    def read_bytes(self, num_bytes):
        while True:
            yield self.recv()
            if len(self._read_buffer) >= num_bytes:
                yield self._consume(num_bytes)
                break
        
    def _consume(self, loc):
        result = self._read_buffer[:loc]
        self._read_buffer = self._read_buffer[loc:]
        return result
    
    def recv(self, buffersize=65535):
        while True:
#            print 'recv', self
            yield ReadWait(self.sock)
            self._read_buffer += self.sock.recv(buffersize)
            if self._read_buffer:
                yield self._read_buffer
            yield Sleep(0)
            
    def __repr__(self):
        try:
            fd = self.fileno()
        except Exception, e:
            fd = e
        return '<Socket %s>' % fd
    
#    def recvfrom(self, buffersize=65535, flags=0):
#        if self.sock.type == SOCK_STREAM:
#            yield self.recv(buffersize), None
#        yield ReadWait(self.sock)
#        data, address = self.sock.recvfrom(buffersize)
#        yield data, address
    
    def __del__(self):
        try:
            self.sock.close()
        except:
            # close() may fail if __init__ didn't complete
            pass
    
    family = property(lambda self: self._sock.family, doc="the socket family")
    type = property(lambda self: self._sock.type, doc="the socket type")
    proto = property(lambda self: self._sock.proto, doc="the socket protocol")

    _s = ("def %s(self, *args): return self.sock.%s(*args)\n\n"
          "%s.__doc__ = socket.%s.__doc__\n")
    for _m in _socketmethods:
        exec _s % (_m, _m, _m, _m)
    del _m, _s