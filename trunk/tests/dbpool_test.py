#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""dbpool module test
"""

from litchi.pool import Pool
from litchi.db.mysql import connect
from litchi.schedule import Scheduler
from litchi.systemcall import WaitTask, NewTask, Sleep


def free_conn(pool, conn):
    yield Sleep(1)
    assert pool.waittings
    assert pool.connected_count == pool.maxsize
    assert pool.wait_event in schedule.event_waitting
    yield pool.put(conn)

def pool_test(minsize, maxsize):
    pool = Pool(connect, minsize, maxsize, host='10.20.238.182', port=3306, user='mercury', password='mercury123', db='webauth')
    assert pool.connected_count == 0
    assert len(pool.free_items) == 0
    assert not pool.waittings
    assert pool.maxsize == maxsize
    init_task = yield NewTask(pool.init())
    yield WaitTask(init_task)
    assert pool.connected_count == minsize
    assert len(pool.free_items) == minsize
    assert not pool.waittings
    
    connected_count = pool.connected_count
    conns = []
    for _ in range(minsize):
        conns.append((yield pool.get()))
    assert connected_count == pool.connected_count
    assert len(pool.free_items) == 0
    
    for _ in range(maxsize - minsize):
        conns.append((yield pool.get()))
        assert len(pool.free_items) == 0
    assert pool.connected_count == maxsize
    assert len(pool.free_items) == 0
    
    yield NewTask(free_conn(pool, conns.pop()))
    
    conns.append((yield pool.get()))
    assert len(conns) == maxsize
    assert pool.connected_count == maxsize
    
    
    exit()

schedule = Scheduler.instance()
schedule.new(pool_test(5, 10))
schedule.mainloop()