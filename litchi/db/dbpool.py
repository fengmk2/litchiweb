#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A DB connections pool.
"""
from collections import deque

from litchi.systemcall import Wait, Fire


class Pool(object):
    
    def __init__(self, connect, minsize=5, maxsize=10, *args, **kwargs):
        self.connect = connect
        self.items = deque()
        if minsize > maxsize:
            minsize = maxsize
        self.minsize = minsize
        self.maxsize = maxsize
        self.connected_count = 0
        self.dbargs = args
        self.dbkwargs = kwargs
        self.waittings = False
    
    def _connect(self):
        self.connected_count += 1
        conn = yield self.connect(*self.dbargs, **self.dbkwargs)
        yield conn
        
    def init(self):
        if self.connected_count == 0 and self.minsize > 0:
            for _ in range(self.minsize):
                conn = yield self._connect()
                self.items.append(conn)
    
    def get(self):
        while True:
            if self.items:
                conn = self.items.popleft()
                break
            elif self.connected_count < self.maxsize:
                conn = yield self._connect()
                break
            else:
                self.waittings = True
                yield Wait('free_conn')
        yield conn
    
    def put(self, conn):
        if len(self.items) < self.maxsize:
            self.items.append(conn)
            if self.waittings:
                self.waittings = False
                yield Fire('free_conn')
        else:
            conn.close()