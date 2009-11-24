#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A very simple small web
"""
from datetime import datetime
    
from litchi.http import HTTPServer
from litchi.schedule import Scheduler


def handler(request):
    yield "Hello world, %s" % datetime.now()
    
    
#import logging
#logging.root.setLevel(logging.DEBUG)
httpserver = HTTPServer(handler)
httpserver.listen(8081)
s = Scheduler.instance()
s.new(httpserver.start())
s.mainloop()


