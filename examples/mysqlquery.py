#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""how to use async mysql query
"""
from litchi.db.mysql import connect
from litchi.schedule import Scheduler

#def foo1():
#    conn = raw_connect(host='10.20.238.182', port=3306, user='mercury', password='mercury123', db='webauth')
#    print conn
#    cur = conn.cursor()
#    cur.execute("SELECT * FROM url_source where id>10088 LIMIT 100")
#    
#    # print cur.description
#    
#    rs = cur.fetchall()
#    # print r
#    # ...or...
#    for r in rs:
#        print r[8], r
#    #print cur.execute('delete from url_source where id=%s', (1686,))
#    #print conn.commit()
#    cur.close()
#    conn.close()
#
#foo1()

def foo():
    conn = yield connect(host='10.20.238.182', port=3306, user='mercury', password='mercury123', db='webauth')

    cur = conn.cursor()
    yield cur.execute("SELECT * FROM url_source where id>10088 LIMIT 100")
    
    # print cur.descriptio
    
    rs = yield cur.fetchall()
    # print r
    # ...or...
    for r in rs:
        print r[8], r[0]
    #print cur.execute('delete from url_source where id=%s', (1686,))
    #print conn.commit()
    cur.close()
    conn.close()

#import logging
#logging.root.setLevel(logging.DEBUG)
s = Scheduler.instance()
#s.new(alive())
s.new(foo())
s.mainloop()