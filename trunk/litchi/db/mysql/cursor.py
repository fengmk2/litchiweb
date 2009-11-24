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
from mysql.connector import errors
from mysql.connector import protocol
from mysql.connector.cursor import MySQLCursor, MySQLCursorBuffered

        
class AsyncMySQLCursor(MySQLCursor):
    
    def __iter__(self):
        """
        Iteration over the result set which calls self.fetchone()
        and returns the next row.
        """
        return iter(self.fetchone, None)

    def next(self):
        """
        Used for iterating over the result set. Calles self.fetchone()
        to get the next row.
        """
        try:
#            row = self.fetchone()
            row = yield self.fetchone()
        except errors.InterfaceError:
            raise StopIteration
        if not row:
            raise StopIteration
#        return row
        yield row
    
    def _handle_noresultset(self, res):
        """Handles result of execute() when there is no result set."""
        try:
            self.rowcount = res.affected_rows
            self.lastrowid = res.insert_id
            self._warning_count = res.warning_count
            if self._get_warnings is True and self._warning_count:
#                self._warnings = self._fetch_warnings()
                self._warnings = yield self._fetch_warnings()
        except StandardError, e:
            raise errors.ProgrammingError(
                "Failed handling non-resultset; %s" % e)
    
    def execute(self, operation, params=None):
        """
        Executes the given operation. The parameters given through params
        are used to substitute %%s in the operation string.
        For example, getting all rows where id is 5:
          cursor.execute("SELECT * FROM t1 WHERE id = %s", (5,))
        
        If warnings where generated, and db.get_warnings is True, then
        self._warnings will be a list containing these warnings.
        
        Raises exceptions when any error happens.
        """
        if not operation:
#            return 0
            yield 0
        self._reset_result()
        stmt = ''
        
        # Make sure we send the query in correct character set
        try:
            if isinstance(operation, unicode):
                operation.encode(self.db.charset_name)
            if params is not None:
                stmt = operation % self._process_params(params)
            else:
                stmt = operation
#            res = self.protocol.cmd_query(stmt)
            res = yield self.protocol.cmd_query(stmt)
            if isinstance(res, protocol.OKResultPacket):
                self._have_result = False
#                self._handle_noresultset(res)
                yield self._handle_noresultset(res)
            else:
                self.description = self._get_description(res)
                self._have_result = True
                self._handle_resultset()
        except errors.ProgrammingError:
            raise
        except errors.OperationalError:
            raise
        except StandardError, e:
            raise errors.InterfaceError(
                "Failed executing the operation; %s" % e)
        else:
            self._executed = stmt
#            return self.rowcount
            yield self.rowcount
            
#        return 0
        yield 0
    
    def executemany(self, operation, seq_params):
        """Loops over seq_params and calls excute()"""
        if not operation:
#            return 0
            yield 0
            
        rowcnt = 0
        try:
            for params in seq_params:
#                self.execute(operation, params)
                yield self.execute(operation, params)
                if self._have_result:
#                    self.fetchall()
                    yield self.fetchall()
                rowcnt += self.rowcount
        except (ValueError,TypeError), e:
            raise errors.InterfaceError(
                "Failed executing the operation; %s" % e)
        except:
            # Raise whatever execute() raises
            raise
            
#        return rowcnt
        yield rowcnt
    
    def callproc(self, procname, args=()):
        """Calls a stored procedue with the given arguments
        
        The arguments will be set during this session, meaning
        they will be called like  _<procname>__arg<nr> where
        <nr> is an enumeration (+1) of the arguments.
        
        Coding Example:
          1) Definining the Stored Routine in MySQL:
          CREATE PROCEDURE multiply(IN pFac1 INT, IN pFac2 INT, OUT pProd INT)
          BEGIN
            SET pProd := pFac1 * pFac2;
          END
          
          2) Executing in Python:
          args = (5,5,0) # 0 is to hold pprod
          cursor.callproc(multiply, args)
          print cursor.fetchone()
          
          The last print should output ('5', '5', 25L)

        Does not return a value, but a result set will be
        available when the CALL-statement execute succesfully.
        Raises exceptions when something is wrong.
        """
        argfmt = "@_%s_arg%d"
        
        try:
            procargs = self._process_params(args)
            argnames = []
        
            for idx,arg in enumerate(procargs):
                argname = argfmt % (procname, idx+1)
                argnames.append(argname)
                setquery = "SET %s=%%s" % argname
#                self.execute(setquery, (arg,))
                yield self.execute(setquery, (arg,))
        
            call = "CALL %s(%s)" % (procname,','.join(argnames))
#            res = self.protocol.cmd_query(call)
            res = yield self.protocol.cmd_query(call)
            
            select = "SELECT %s" % ','.join(argnames)
#            self.execute(select)
            yield self.execute(select)
            
        except errors.ProgrammingError:
            raise
        except StandardError, e:
            raise errors.InterfaceError(
                "Failed calling stored routine; %s" % e)
    
    def _fetch_warnings(self):
        """
        Fetch warnings doing a SHOW WARNINGS. Can be called after getting
        the result.

        Returns a result set or None when there were no warnings.
        """
        res = []
        try:
            c = self.db.cursor()
#            cnt = c.execute("SHOW WARNINGS")
            cnt = yield c.execute("SHOW WARNINGS")
#            res = c.fetchall()
            res = yield c.fetchall()
            c.close()
        except StandardError, e:
            raise errors.ProgrammingError(
                "Failed getting warnings; %s" % e)
        else:
            if len(res):
#                return res
                yield res
            
#        return None
        yield None
    
    def _handle_eof(self, eof):
        self._have_result = False
        self._nextrow = (None, None)
        self._warning_count = eof.warning_count
        if self.db.get_warnings is True and eof.warning_count:
#            self._warnings = self._fetch_warnings()
            self._warnings = yield self._fetch_warnings()
    
    def _fetch_row(self):
        if self._have_result is False:
#            return None
            yield None
        row = None
        try:
            if self._nextrow == (None, None):
#                (row, eof) = self.protocol.result_get_row()
                (row, eof) = yield self.protocol.result_get_row()
            else:
                (row, eof) = self._nextrow
            if row:
#                (foo, eof) = self._nextrow = self.protocol.result_get_row()
                (foo, eof) = self._nextrow = yield self.protocol.result_get_row()
                if eof is not None:
#                    self._handle_eof(eof)
                    yield self._handle_eof(eof)
                if self.rowcount == -1:
                    self.rowcount = 1
                else:
                    self.rowcount += 1
            if eof:
#                self._handle_eof(eof)
                yield self._handle_eof(eof)
        except:
            raise
        else:
#            return row
            yield row
            
#        return None
        yield None
        
    def fetchone(self):
#        row = self._fetch_row()
        row = yield self._fetch_row()
        if row:
#            return self._row_to_python(row)
            yield self._row_to_python(row)
#        return None
        yield None
        
    def fetchmany(self,size=None):
        res = []
        cnt = (size or self.arraysize)
        while cnt > 0 and self._have_result:
            cnt -= 1
#            row = self.fetchone()
            row = yield self.fetchone()
            if row:
                res.append(row)
        
#        return res
        yield res
    
    def fetchall(self):
        if self._have_result is False:
            raise errors.InterfaceError("No result set to fetch from.")
        res = []
        row = None
        while self._have_result:
#            row = self.fetchone()
            row = yield self.fetchone()
            if row:
                res.append(row)
#        return res
        yield res

class AsyncMySQLCursorBuffered(AsyncMySQLCursor, MySQLCursorBuffered):
    """Cursor which fetches rows within execute()"""
    
    def __init__(self, db=None):
#        MySQLCursor.__init__(self, db)
        super(AsyncMySQLCursorBuffered, self).__init__(db)
        self._rows = []
        self._next_row = 0
    
    def _handle_resultset(self):
#        self._get_all_rows()
        yield self._get_all_rows()
    
    def _get_all_rows(self):
#        (self._rows, eof) = self.protocol.result_get_rows()
        (self._rows, eof) = yield self.protocol.result_get_rows()
        self.rowcount = len(self._rows)
#        self._handle_eof(eof)
        yield self._handle_eof(eof)
        self._next_row = 0
        self._have_result = True