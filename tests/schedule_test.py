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
    taskid = yield GetTaskid()
    try:
        print 'start sleep', time.time()
        yield Sleep(2)
        print 'wake up', time.time()
    except StopIteration:
        print 'I am be killed.', taskid

def sleepkill():
    taskid = yield GetTaskid()
    try:
        print taskid, 'start sleep', time.time()
        yield Sleep(2)
        print taskid, 'wake up', time.time()
    finally:
        print 'finally'
        print 'I am be killed.', taskid
        
def sleeps(scheduler):
    s1 = yield NewTask(sleep())
    s2 = yield NewTask(sleepkill())
    yield
    print 'close', s2
    scheduler.taskmap[s2].target.close()

import logging
logging.root.setLevel(logging.DEBUG)
s = Scheduler.instance()
s.new(foo())
s.new(bar())
s.new(create_task())
s.new(wait())
s.new(sleeps(s))
s.mainloop()