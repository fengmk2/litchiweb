#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A DB connections pool.
"""
from collections import deque

from litchi.systemcall import Wait, Fire


class Pool(object):
    """A simple connection pool."""
    
    FREE_CONN_EVENT = '__FREE_CONN_EVENT__'
    
    def __init__(self, connect, minsize=5, maxsize=10, *args, **kwargs):
        self.connect = connect
        self.free_items = deque()
        if minsize > maxsize:
            minsize = maxsize
        self.minsize = minsize
        self.maxsize = maxsize
        self.connected_count = 0
        self.dbargs = args
        self.dbkwargs = kwargs
        self.waittings = 0
    
    def _connect(self):
        self.connected_count += 1
        conn = yield self.connect(*self.dbargs, **self.dbkwargs)
        yield conn
        
    def init(self):
        if self.connected_count == 0 and self.minsize > 0:
            for _ in range(self.minsize):
                conn = yield self._connect()
                self.free_items.append(conn)
    
    def get(self):
        if self.free_items:
            conn = self.free_items.popleft()
        elif self.connected_count < self.maxsize:
            conn = yield self._connect()
        else:
            self.waittings += 1
            conn = yield Wait(self.FREE_CONN_EVENT)
        yield conn
    
    def put(self, conn):
        if self.waittings > 0:
            self.waittings -= 1
            yield Fire(self.FREE_CONN_EVENT, conn)
        else:
            if len(self.free_items) < self.maxsize:
                self.free_items.append(conn)
                if self.waittings:
                    self.waittings = False
                    yield Fire(self.FREE_CONN_EVENT)
            else:
                conn.close()