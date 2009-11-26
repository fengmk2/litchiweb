#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A very simple small web
"""
from random import randint
from datetime import datetime
import time

from litchi.http import HTTPServer, HTTPReponse
from litchi.schedule import Scheduler
from litchi.db.mysql import connect
from litchi.pool import Pool

pool = Pool(connect, 10, 50, host='10.20.238.182', port=3306, user='mercury', password='mercury123', db='webauth')

count = 0
debug = True

def handler(request):
    
    if debug:
        global count
        count += 1
        index = count
        print 'start-%d' % index, pool.connected_count, len(pool.free_items), pool.waittings
    conn = yield pool.get()
    print 'get-%d' % index, pool.connected_count, len(pool.free_items), pool.waittings
    cur = conn.cursor(True)
    try:
        yield cur.execute("SELECT * FROM url_source where id>%s LIMIT 1", (10000 + randint(1000, 5000),))
        
        rs = yield cur.fetchall()
#        print cur.description
#        print rs
    finally:
        cur.close()
        yield pool.put(conn)
        # print r
        # ...or...
    #    for r in rs:
    #        print r[8], r
        #print cur.execute('delete from url_source where id=%s', (1686,))
        #print conn.commit()
    if debug:
        print 'end-%d' % index, pool.connected_count, len(pool.free_items), pool.waittings
    cookie = request.get_cookie('testcookie')
    r = HTTPReponse("""Hello world, %s, cookie: %s,
        request: %s
        data: %s
        connected: %s, pool left: %s
        Schedule: %s""" % \
        (datetime.now(), cookie, 
         request, 
         rs[0], 
         pool.connected_count, len(pool.free_items), 
         schedule), 'text/plain')
    r.set_cookie('testcookie', '%s' % time.time())
    
    yield r
    
    
#import logging
#logging.root.setLevel(logging.DEBUG)
httpserver = HTTPServer(handler)
httpserver.listen(8081)
schedule = Scheduler.instance()
schedule.new(pool.init())
schedule.new(httpserver.start())
schedule.mainloop()

"""
TEST:

httperf --hog --server=10.20.208.25 --timeout 5 --uri=/ --port=8081 \
    --rate 100 --num-conns=1000 --num-calls=10
"""


