#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A simple pool.
"""
from collections import deque

from litchi.systemcall import Wait, Fire
from litchi.schedule import Scheduler


class Pool(object):
    """A simple pool."""
    
    FREE_CONN_EVENT = '__FREE_CONN_EVENT__'
    
    def __init__(self, connect, minsize=5, maxsize=10, *args, **kwargs):
        self.connect = connect
        self.free_items = deque()
        if minsize > maxsize:
            minsize = maxsize
        self.minsize = minsize
        self.maxsize = maxsize
        self.connected_count = 0
        self.args = args
        self.kwargs = kwargs
        self.waittings = 0
        self.wait_event = '%s_%s' % (self.FREE_CONN_EVENT, id(self))
        
        # init pool
        Scheduler.instance().new(self.init(), 'PoolInit')
    
    def _connect(self):
        self.connected_count += 1
        conn = yield self.connect(*self.args, **self.kwargs)
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
            conn = yield Wait(self.wait_event)
        yield conn
    
    def put(self, conn):
        if self.waittings > 0:
            self.waittings -= 1
            yield Fire(self.wait_event, conn)
        else:
            if len(self.free_items) < self.maxsize:
                self.free_items.appendleft(conn)
            else:
                del conn