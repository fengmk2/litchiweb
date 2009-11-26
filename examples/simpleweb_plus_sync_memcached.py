#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A very simple small web
"""
from datetime import datetime

from litchi.http import HTTPServer, HTTPReponse
from litchi.schedule import Scheduler
from litchi.utils.memcache import Client


cache = Client(['127.0.0.1:11211'])

count = 0
debug = True

def handler(request):
    if debug:
        global count
        count += 1
        index = count
        print 'start-%d' % index
    rs = cache.get('abc')
    if debug:
        print 'end-%d' % index
   
    r = HTTPReponse("""Hello world, %s<br /> <br /> 
        request: %s <br /> <br /> 
        data: %s <br /> <br /> 
        Schedule: %s<br /> <br />  """ % \
        (datetime.now(),
         request, 
         rs, 
         schedule), content_type='text/plain')
    
    yield r
    
    
#import logging
#logging.root.setLevel(logging.DEBUG)
httpserver = HTTPServer(handler)
httpserver.listen(8081)
schedule = Scheduler.instance()
schedule.new(httpserver.start(), 'httpserver')
schedule.mainloop()

"""
TEST:

httperf --hog --server=10.20.208.25 --timeout 5 --uri=/ --port=8081 \
    --rate 100 --num-conns=1000 --num-calls=10
"""


