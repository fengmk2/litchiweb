# -*- coding: utf-8 -*-
"""
Connector/Python, native MySQL driver written in Python.
Copyright 2009 Sun Microsystems, Inc. All rights reserved. Use is subject to license terms.

Modified by MK2(fengmk2@gmail.com)

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""

import os
import socket

from mysql.connector.connection import MySQLBaseConnection
from mysql.connector import errors

from litchi.socketwrap import Socket
from litchi.db.mysql.protocol import MySQLAsyncProtocol

class MySQLBaseAsyncConnection(MySQLBaseConnection):
    """Base class for MySQL Async Connections subclasses.
    
    Should not be used directly but overloaded, changing the
    open_connection part. Examples over subclasses are
      MySQLAsyncTCPConnection
      MySQLAsyncUNIXConnection
    """
    def __init__(self, prtcls=None):
        self.sock = None # holds the socket connection
        self.connection_timeout = None
        self.protocol = None
        self.socket_flags = 0
        try:
            self.protocol = prtcls(self)
        except:
#            self.protocol = protocol.MySQLProtocol(self)
            self.protocol = MySQLAsyncProtocol(self)
        self._set_socket_flags()

    def send(self, buf):
        """
        Send packets using the socket to the server.
        """
#        pktlen = len(buf)
        try:
            yield self.sock.send(buf)
#            while pktlen:
#                pktlen -= self.sock.send(buf)
        except Exception, e:
            raise errors.OperationalError('%s' % e)

    def recv(self):
        """
        Receive packets using the socket from the server.
        """
        try:
            # self.sock.recv(4, self.socket_flags)
            header = yield self.sock.read_bytes(4, self.socket_flags)
            (pktsize, pktnr) = self.protocol.handle_header(header)
#            buf = header + self.sock.recv(pktsize, self.socket_flags)
            buf = header + (yield self.sock.read_bytes(pktsize, self.socket_flags))
            self.protocol.is_error(buf)
        except:
            raise
        
        # return (buf, pktsize, pktnr)
        yield (buf, pktsize, pktnr)

    def set_protocol(self, prtcls):
        try:
            self.protocol = prtcls(self, self.protocol.handshake)
        except:
#            self.protocol = protocol.MySQLProtocol(self)
            self.protocol = MySQLAsyncProtocol(self)
    
    def set_connection_timeout(self, timeout):
        self.connection_timeout = timeout

    def _set_socket_flags(self, flags=None):
        self.socket_flags = 0
        if flags is None:
            if os.name == 'nt':
                flags = 0
            else:
                flags = socket.MSG_WAITALL
                
        if flags is not None:
            self.socket_flags = flags
    

class MySQLAsyncUnixConnection(MySQLBaseConnection):
    """Opens a connection through the UNIX socket of the MySQL Server."""
    
    def __init__(self, prtcls=None,unix_socket='/tmp/mysql.sock'):
        MySQLBaseConnection.__init__(self, prtcls=prtcls)
        self.unix_socket = unix_socket
        self.socket_flags = socket.MSG_WAITALL
        
    def open_connection(self):
        """Opens a UNIX socket and checks the MySQL handshake."""
        try:
#            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock = Socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.settimeout(self.connection_timeout)
            self.sock.connect(self.unix_socket)
        except StandardError, e:
            raise errors.OperationalError('%s' % e)
        
        # buf = self.recv()[0]
        buf = (yield self.recv())[0]
        self.protocol.handle_handshake(buf)

class MySQLAsyncTCPConnection(MySQLBaseAsyncConnection):
    """Opens an Async TCP connection to the MySQL Server."""
    
    def __init__(self, prtcls=None, host='127.0.0.1', port=3306):
        super(MySQLAsyncTCPConnection, self).__init__(prtcls=prtcls)
        self.server_host = host
        self.server_port = port
        
    def open_connection(self):
        """Opens a TCP Connection and checks the MySQL handshake."""
        try:
#             self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock = Socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.connection_timeout)
#            self.sock.setblocking(1)
            sock.connect( (self.server_host, self.server_port) )
#            self.sock = Socket(_sock=sock)
            self.sock = sock
#            self.sock.setblocking(0)
        except StandardError, e:
            raise errors.OperationalError('%s' % e)
            
        # buf = self.recv()[0]
        buf = (yield self.recv())[0]
        self.protocol.handle_handshake(buf)

        
