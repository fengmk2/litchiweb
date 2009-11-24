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

from mysql.connector import constants
from mysql.connector import conversion
from mysql.connector.errors import InterfaceError
from mysql.connector import errors
from mysql.connector.mysql import MySQLBase

from litchi.db.mysql.connection import MySQLAsyncUnixConnection, \
    MySQLAsyncTCPConnection
from litchi.db.mysql import cursor
from litchi.db.mysql.protocol import MySQLAsyncProtocol


class AsyncMySQLBase(MySQLBase):
    """MySQLBase"""

    def _set_connection(self, prtcls=None):
        """Automatically chooses based on configuration which connection type to setup."""
        if self.unix_socket and os.name != 'nt':
#            self.conn = MySQLUnixConnection(prtcls=prtcls,
#                unix_socket=self.unix_socket)
            self.conn = MySQLAsyncUnixConnection(prtcls=prtcls,
                unix_socket=self.unix_socket)
        else:
#            self.conn = MySQLTCPConnection(prtcls=prtcls,
#                host=self.server_host, port=self.server_port)
            self.conn = MySQLAsyncTCPConnection(prtcls=prtcls,
                host=self.server_host, port=self.server_port)
        self.conn.set_connection_timeout(self.connection_timeout)
        
    def _open_connection(self):
        """Opens the connection and sets the appropriated protocol."""
        # We don't know yet the MySQL version we connect too
        self._set_connection()
        try:
#            self.conn.open_connection()
            yield self.conn.open_connection()
            version = self.conn.protocol.server_version
            if version < (4,1):
                raise InterfaceError("MySQL Version %s is not supported." % version)
            else:
#                self.conn.set_protocol(protocol.MySQLProtocol)
                self.conn.set_protocol(MySQLAsyncProtocol)
            self.protocol = self.conn.protocol
#            self.protocol.do_auth(username=self.username, password=self.password,
#                database=self.database)
            yield self.protocol.do_auth(username=self.username, password=self.password,
                database=self.database)
        except:
            raise
        
    def _post_connection(self):
        """Should be called after a connection was established"""
        self.get_characterset_info()
        self.set_converter_class(conversion.MySQLConverter)
        
        try:
#            self.set_charset(self.charset_name)
            yield self.set_charset(self.charset_name)
#            self.set_autocommit(self.autocommit)
            yield self.set_autocommit(self.autocommit)
        except:
            raise
            
    def is_connected(self):
        """
        Check whether we are connected to the MySQL server.
        """
#        return self.protocol.cmd_ping()
        result = yield self.protocol.cmd_ping()
        yield result
    ping = is_connected

    def disconnect(self):
        """
        Disconnect from the MySQL server.
        """
        if not self.conn:
#            return
            yield
            
        if self.conn.sock is not None:
#            self.protocol.cmd_quit()
            yield self.protocol.cmd_quit()
            try:
                self.conn.close_connection()
            except:
                pass
        self.protocol = None
        self.conn = None
    
    def set_charset(self, name):
        """
        Set the character set used for the connection. This is the recommended
        way of change it per connection basis. It does execute SET NAMES
        internally, but it's good not to use this command directly, since we
        are setting some other members accordingly.
        """
        if name not in constants.CharacterSet.get_supported():
            raise errors.ProgrammingError, "Character set '%s' not supported." % name
#            return
            yield
        try:
            info = constants.CharacterSet.get_charset_info(name)
        except errors.ProgrammingError, e:
            raise
        
        try:
#            self.protocol.cmd_query("SET NAMES '%s'" % name)
            yield self.protocol.cmd_query("SET NAMES '%s'" % name)
        except:
            raise
        else:
            (self.charset, self.charset_name, self.collation_name) = info
            self.converter.set_charset(self.charset_name)
    
    def set_autocommit(self, switch):
        """
        Set auto commit on or off. The argument 'switch' must be a boolean type.
        """
        if not isinstance(switch, bool):
            raise ValueError, "The switch argument must be boolean."
        
        s = 'OFF'
        if switch:
            s = 'ON'
        
        try:
#            self.protocol.cmd_query("SET AUTOCOMMIT = %s" % s)
            yield self.protocol.cmd_query("SET AUTOCOMMIT = %s" % s)
        except:
            raise
        else:
            self.autocommit = switch

class AsyncMySQL(AsyncMySQLBase):
    """
    Class implementing Python DB API v2.0.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the MySQL object. Calls connect() to open the connection
        when an instance is created.
        """
#        MySQLBase.__init__(self)
        super(AsyncMySQL, self).__init__()
        self.cursors = []
        self.affected_rows = 0
        self.server_status = 0
        self.warning_count = 0
        self.field_count = 0
        self.insert_id = 0
        self.info_msg = ''
        
#        self.connect(*args, **kwargs)
#        yield self.connect(*args, **kwargs)
            
    def connect(self, dsn='', user='', password='', host='127.0.0.1',
            port=3306, db=None, database=None, use_unicode=True, charset='utf8', get_warnings=False,
            autocommit=False, unix_socket=None,
            connection_timeout=None, client_flags=None, buffered=False):
        """
        Establishes a connection to the MySQL Server. Called also when instansiating
        a new MySQLConnection object through the __init__ method.

        Possible parameters are:

        dsn
            (not used)
        user
            The username used to authenticate with the MySQL Server.

        password
            The password to authenticate the user with the MySQL Server.

        host
            The hostname or the IP address of the MySQL Server we are connecting with.
            (default 127.0.0.1)

        port
            TCP port to use for connecting to the MySQL Server.
            (default 3306)

        database
        db
            Initial database to use once we are connected with the MySQL Server.
            The db argument is synonym, but database takes precedence.

        use_unicode
            If set to true, string values received from MySQL will be returned
            as Unicode strings.
            Default: True

        charset
            Which character shall we use for sending data to MySQL. One can still
            override this by using the SET NAMES command directly, but this is
            discouraged. Instead, use the set_charset() method if you
            want to change it.
            Default: Whatever the MySQL server has default.

        get_warnings
            If set to true, whenever a query gives a warning, a SHOW WARNINGS will
            be done to fetch them. They will be available as MySQLCursor.warnings.
            The default is to ignore these warnings, for debugging it's good to
            enable it though, or use strict mode in MySQL to make most of these
            warnings errors.
            Default: False

        autocommit
            Auto commit is OFF by default, which is required by the Python Db API
            2.0 specification.
            Default: False

        unix_socket
            Full path to the MySQL Server UNIX socket. By default TCP connection will
            be used using the address specified by the host argument.
        
        connection_timeout
            Timeout for the TCP and UNIX socket connection.
        
        client_flags
            Allows to set flags for the connection. Check following for possible flags:
             >>> from mysql.connector.constants import ClientFlag
             >>> print '\n'.join(ClientFlag.get_full_info())
        
        buffered
            When set to True .execute() will fetch the rows immediatly.
            
        """
        # db is not part of Db API v2.0, but MySQLdb supports it.
        if db and not database:
            database = db

        self.set_host(host)
        self.set_port(port)
        self.set_database(database)
        self.set_getwarnings(get_warnings)
        self.set_unixsocket(unix_socket)
        self.set_connection_timeout(connection_timeout)
        self.set_client_flags(client_flags)
        self.set_buffered(buffered)

        if user or password:
            self.set_login(user, password)

#        self.disconnect()
        yield self.disconnect()
#        self._open_connection()
        yield self._open_connection()
#        self._post_connection()
        yield self._post_connection()
    
    def close(self):
        del self.cursors[:]
#        self.disconnect()
        yield self.disconnect()
    
    def remove_cursor(self, c):
        try:
            self.cursors.remove(c)
        except ValueError:
            raise errors.ProgrammingError(
                "Cursor could not be removed.")
    
    def register_cursor(self, c):
        try:
            self.cursors.append(c)
        except:
            raise
    
    def cursor(self):
        if self.buffered:
#            c = (cursor.MySQLCursorBuffered)(self)
            c = (cursor.AsyncMySQLCursorBuffered)(self)
        else:
#            c = (cursor.MySQLCursor)(self)
            c = (cursor.AsyncMySQLCursor)(self)
        
        self.register_cursor(c)
        return c

    def commit(self):
        """Shortcut for executing COMMIT."""
#        self.protocol.cmd_query("COMMIT")
        yield self.protocol.cmd_query("COMMIT")

    def rollback(self):
        """Shortcut for executing ROLLBACK"""
#        self.protocol.cmd_query("ROLLBACK")
        yield self.protocol.cmd_query("ROLLBACK")

    
