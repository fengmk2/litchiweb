#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""unit test for schedule.py
"""
import time

from litchi.schedule import Scheduler
from litchi.systemcall import GetTaskid, NewTask, KillTask, WaitTask, Sleep


def foo():
    taskid = yield GetTaskid()
    for i in range(10):
        print 'I am foo', taskid
        yield

def bar():
    taskid = yield GetTaskid()
    for i in range(5):
        print 'I am bar', taskid
        yield
        
def new_bar():
    taskid = yield GetTaskid()
    for i in range(5):
        print 'I am new_bar', taskid
        yield
        
def create_task():
    taskid = yield GetTaskid()
    print 'I am create_task', taskid
    new_taskid = yield NewTask(new_bar())
    print 'create_task task', new_taskid
    for i in range(2):
        yield
    assert (yield KillTask(new_taskid))
    print 'task %d kill' % new_taskid
    
def waitchild():
    taskid = yield GetTaskid()
    for i in xrange(5):
        print 'I am waitchild', taskid
        yield
        
def wait():
    child = yield NewTask(waitchild())
    print 'Waiting for child %d finished' % child
    yield WaitTask(child)
    print 'Child Done'
    
def sleep():
    print 'start sleep', time.time()
    yield Sleep(2)
    print 'wake up', time.time()

import logging
logging.root.setLevel(logging.DEBUG)
s = Scheduler.instance()
s.new(foo())
s.new(bar())
s.new(create_task())
s.new(wait())
s.new(sleep())
s.mainloop()