#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A very simple small web
"""
from datetime import datetime
import time

from litchi.http import HTTPServer, HTTPReponse
from litchi.schedule import Scheduler


def handler(request):
    cookie = request.get_cookie('testcookie')
    r = HTTPReponse("Hello world, %s, cookie: %s, <br /> %s" % (datetime.now(), cookie, request), request)
    r.set_cookie('testcookie', '%s' % time.time())
    r.set_cookie('testcookie22', 'abc')
    yield r
    
    
#import logging
#logging.root.setLevel(logging.DEBUG)
httpserver = HTTPServer(handler)
httpserver.listen(8081)
s = Scheduler.instance()
s.new(httpserver.start())
s.mainloop()


