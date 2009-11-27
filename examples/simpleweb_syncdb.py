#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A very simple small web
"""
from random import randint
from datetime import datetime
import time

from mysql.connector import Connect

from litchi.http import HTTPServer, HTTPReponse
from litchi.schedule import Scheduler

conn = Connect(host='10.20.238.182', port=3306, user='mercury', password='mercury123', db='webauth')

count = 0
debug = True

def handler(request):
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM url_source where id>%s LIMIT 1", (10000 + randint(1000, 5000),))
        rs = cur.fetchall()
    finally:
        cur.close()
    cookie = request.get_cookie('testcookie')
    r = HTTPReponse("""Hello world, %s, cookie: %s,
        request: %s
        data: %s
        Schedule: %s""" % \
        (datetime.now(), cookie, 
         request, 
         rs[0], 
         schedule), 'text/plain')
    r.set_cookie('testcookie', '%s' % time.time())
    
    yield r
    
    
httpserver = HTTPServer(handler)
httpserver.listen(8081)
schedule = Scheduler.instance()
schedule.new(httpserver.start())
schedule.mainloop()

