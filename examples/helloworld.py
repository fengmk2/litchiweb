#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A very simple small web
"""
from litchi.schedule import Scheduler
from litchi.http import HTTPServer


def handler(request):
    yield 'Hello world'
    
httpserver = HTTPServer(handler)
httpserver.listen(8081)
s = Scheduler.instance()
s.new(httpserver.start())
s.mainloop()


