#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""memcache test
"""

from litchi.schedule import Scheduler
from litchi.memcached import AsyncClient


def test():
    client = AsyncClient(['127.0.0.1:11211'], debug=True)
    print (yield client.get_stats())
    print (yield client.set('abc', 123123))
    print (yield client.get('abc'))
#    print (yield client.delete('abc'))
#    print (yield client.delete('abc'))
    print (yield client.get('abc'))
    
    exit()

s = Scheduler.instance()
s.new(test())
s.mainloop()