#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""wait and fire call test
"""

from litchi.schedule import Scheduler
from litchi.systemcall import Wait, Fire

def wait():
    print 'waitting...'
    a = yield Wait('aaa')
    print 'get:', a
    yield Fire('aaa', 'come from wait()')
    print 'wait end'
    
def wait2():
    print 'waitting...2'
    a = yield Wait('aaa')
    print '2 get:', a
    print 'wait2 end'
    
def fire():
    print 'fire'
    yield Fire('aaa', ('something', 123))
    print 'fire end'
    
s = Scheduler.instance()
s.new(wait())
s.new(wait2())
s.new(fire())
s.mainloop()