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

from decimal import Decimal

from mysql.connector.constants import *
from mysql.connector import errors
from mysql.connector import utils
from mysql.connector import protocol

class AsyncMySQLProtocol(protocol.MySQLProtocol):
    """Class handling the MySQL Protocol.
    
    MySQL v4.1 Client/Server Protocol is currently supported.
    """

    def do_auth(self,  username=None, password=None, database=None,
        client_flags=None):
        """
        Make and send the authentication using information found in the
        handshake packet.
        """
        if not client_flags:
            client_flags = ClientFlag.get_default()
        
        auth = protocol.Auth(client_flags=client_flags,
            pktnr=self.handshake.pktnr+1)
        auth.create(username=username, password=password,
            seed=self.handshake.info['seed'], database=database)

#        self.conn.send(auth.get())
        yield self.conn.send(auth.get())
#        buf = self.conn.recv()[0]
        buf = (yield self.conn.recv())[0]
        if self.is_eof(buf):
            raise errors.InterfaceError("Found EOF after Auth, expecting OK. Using old passwords?")
        
        connect_with_db = client_flags & ClientFlag.CONNECT_WITH_DB
        if self.is_ok(buf) and database and not connect_with_db:
            # self.cmd_init_db(database)
            yield self.cmd_init_db(database)
        
    def _handle_fields(self, nrflds):
        """Reads a number of fields from a result set."""
        i = 0
        fields = []
        while i < nrflds:
#            buf = self.conn.recv()[0]
            buf = (yield self.conn.recv())[0]
            fld = protocol.FieldPacket(buf)
            fields.append(fld)
            i += 1
#        return fields
        yield fields
    
    def _handle_resultset(self, pkt):
        """Processes a resultset getting fields information.
        
        The argument pkt must be a protocol.Packet with length 1, a byte
        which contains the number of fields.
        """
        if not isinstance(pkt, protocol.PacketIn):
            raise ValueError("%s is not a protocol.PacketIn" % pkt)
        
        if len(pkt) == 1:
            (buf,nrflds) = utils.read_lc_int(pkt.data)
            
            # Get the fields
#            fields = self._handle_fields(nrflds)
            fields = yield self._handle_fields(nrflds)

            buf = (yield self.conn.recv())[0]
            eof = protocol.EOFPacket(buf)

#            return (nrflds, fields, eof)
            yield (nrflds, fields, eof)
        else:
            raise errors.InterfaceError('Something wrong reading result after query.')

    def result_get_row(self):
        """Get data for 1 row
        
        Get one row's data. Should be called after getting the field
        descriptions.

        Returns a tuple with 2 elements: a row's data and the
        EOF packet.
        """
#        buf = self.conn.recv()[0]
        buf = (yield self.conn.recv())[0]
        if self.is_eof(buf):
            eof = protocol.EOFPacket(buf)
            rowdata = None
        else:
            eof = None
            rowdata = utils.read_lc_string_list(buf[4:])
#        return (rowdata, eof)
        yield (rowdata, eof)
    
    def result_get_rows(self, cnt=None):
        """Get all rows
        
        Returns a tuple with 2 elements: a list with all rows and
        the EOF packet.
        """
        rows = []
        eof = None
        rowdata = None
        while eof is None:
#            (rowdata,eof) = self.result_get_row()
            (rowdata,eof) = yield self.result_get_row()
            if eof is None and rowdata is not None:
                rows.append(rowdata)
#        return (rows,eof)
        yield (rows,eof)

    def cmd_query(self, query):
        """
        Sends a query to the server.
        
        Returns a tuple, when the query returns a result. The tuple
        consist number of fields and a list containing their descriptions.
        If the query doesn't return a result set, the an OKResultPacket
        will be returned.
        """
        try:
            cmd = protocol.CommandPacket()
            cmd.set_command(ServerCmd.QUERY)
            cmd.set_argument(query)
            cmd.create()
#            self.conn.send(cmd.get()) # Errors handled in _handle_error()
            yield self.conn.send(cmd.get()) # Errors handled in _handle_error()
        
#            buf = self.conn.recv()[0]
            buf = (yield self.conn.recv())[0]
            if self.is_ok(buf):
                # Query does not return a result (INSERT/DELETE/..)
#                return protocol.OKResultPacket(buf)
                yield protocol.OKResultPacket(buf)

            p = protocol.PacketIn(buf)
#            (nrflds, fields, eof) = self._handle_resultset(p)
            (nrflds, fields, eof) = yield self._handle_resultset(p)
        except:
            raise
        else:
#            return (nrflds, fields)
            yield (nrflds, fields)
    
#        return (0, ())
        yield (0, ())
    
    def cmd_refresh(self, opts):
        """Send the Refresh command to the MySQL server.
        
        The argument should be a bitwise value using the protocol.RefreshOption
        constants.
        
        Usage:
        
         RefreshOption = mysql.connector.RefreshOption
         refresh = RefreshOption.LOG | RefreshOption.THREADS
         db.cmd_refresh(refresh)
         
        """
        cmd = self._cmd_simple(ServerCmd.REFRESH, opts)
        try:
#            self.conn.send(cmd.get())
            yield self.conn.send(cmd.get())
#            buf = self.conn.recv()[0]
            buf = (yield self.conn.recv())[0]
        except:
            raise
        
        if self.is_ok(buf):
#            return True
            yield True

#        return False
        yield False
    
    def cmd_quit(self):
        """Closes the current connection with the server."""
        cmd = self._cmd_simple(ServerCmd.QUIT)
#        self.conn.send(cmd.get())
        yield self.conn.send(cmd.get())
               
    def cmd_init_db(self, database):
        """
        Send command to server to change databases.
        """
        cmd = self._cmd_simple(ServerCmd.INIT_DB, database)
#        self.conn.send(cmd.get())
        yield self.conn.send(cmd.get())
#        self.conn.recv()[0]
        (yield self.conn.recv())[0]
        
    def cmd_shutdown(self):
        """Shuts down the MySQL Server.
        
        Careful with this command if you have SUPER privileges! (Which your
        scripts probably don't need!)
        
        Returns True if it succeeds.
        """
        cmd = self._cmd_simple(ServerCmd.SHUTDOWN)
        try:
#            self.conn.send(cmd.get())
            yield self.conn.send(cmd.get())
#            buf = self.conn.recv()[0]
            buf = (yield self.conn.recv())[0]
        except:
            raise

#        return True
        yield True
    
    def cmd_statistics(self):
        """Sends statistics command to the MySQL Server
        
        Returns a dictionary with various statistical information.
        """
        cmd = self._cmd_simple(ServerCmd.STATISTICS)
        try:
#            self.conn.send(cmd.get())
            yield self.conn.send(cmd.get())
#            buf = self.conn.recv()[0]
            buf = (yield self.conn.recv())[0]
        except:
            raise
        
        p = Packet(buf)
        info = str(p.data)
        
        res = {}
        pairs = info.split('\x20\x20') # Information is separated by 2 spaces
        for pair in pairs:
            (lbl,val) = [ v.strip() for v in pair.split(':') ]
            # It's either an integer or a decimal
            try:
                res[lbl] = long(val)
            except:
                try:
                    res[lbl] = Decimal(val)
                except:
                    raise ValueError(
                        "Got wrong value in COM_STATISTICS information (%s : %s)." % (lbl, val))
#        return res
        yield res
    
    def cmd_process_kill(self, mypid):
        """Kills a MySQL process using it's ID.
        
        The mypid must be an integer.
        
        """
        cmd = protocol.KillPacket(mypid)
        cmd.create()
        try:
#            self.conn.send(cmd.get())
            yield self.conn.send(cmd.get())
#            buf = self.conn.recv()[0]
            buf = (yield self.conn.recv())[0]
        except:
            raise
        
        if self.is_eof(buf):
#            return True
            yield True

#        return False
        yield False
    
    def cmd_debug(self):
        """Send DEBUG command to the MySQL Server
        
        Needs SUPER privileges. The output will go to the MySQL server error log.
        
        Returns True when it was succesful.
        """
        cmd = self._cmd_simple(ServerCmd.DEBUG)
        try:
#            self.conn.send(cmd.get())
            yield self.conn.send(cmd.get())
#            buf = self.conn.recv()[0]
            buf = (yield self.conn.recv())[0]
        except:
            raise
        
        if self.is_eof(buf):
#            return True
            yield True

#        return False
        yield False
        
    def cmd_ping(self):
        """
        Ping the MySQL server to check if the connection is still alive.

        Returns True when alive, False when server doesn't respond.
        """
        cmd = self._cmd_simple(ServerCmd.PING)
        try:
#            self.conn.send(cmd.get())
            yield self.conn.send(cmd.get())
#            buf = self.conn.recv()[0]
            buf = (yield self.conn.recv())[0]
        except:
#            return False
            yield False
        else:
            if self.is_ok(buf):
#                return True
                yield True

#        return False
        yield False

    def cmd_change_user(self, username, password, database=None):
        """Change the user with given username and password to another optional database.
        """
        if not database:
            database = self.database
        
        cmd = protocol.ChangeUserPacket()
        cmd.create(username=username, password=password, database=database,
            charset=self.charset, seed=self.handshake.info['seed'])
        try:
#            self.conn.send(cmd.get())
            yield self.conn.send(cmd.get())
#            buf = self.conn.recv()[0]
            buf = (yield self.conn.recv())[0]
        except:
            raise
        
        if not self.is_ok(buf):
            raise errors.OperationalError(
                "Failed getting OK Packet after changing user")
        
#        return True
        yield True