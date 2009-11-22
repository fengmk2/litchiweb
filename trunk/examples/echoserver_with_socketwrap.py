#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""echo server use socket wrap demo
"""
from socket import AF_INET, SOCK_STREAM

from litchi.schedule import Scheduler
from litchi.systemcall import NewTask
from litchi.socketwrap import Socket
        

clients = []

def handle_client(client, addr):
    print 'Connection from %s %s' % addr
    while True:
#        yield ReadWait(client)
#        data = client.recv(65536)
        data = yield client.recv(65536)
        data = data.strip()
        if not data:
            break
        data = '[%s %s] %s\r\n' % (addr[0], addr[1], data)
        for other in clients:
            if other != client:
                yield other.send(data)
#        yield WriteWait(client)
#        client.send(`data`)
    for other in clients:
        if other == client:
            clients.remove(other)
            break
    client.close()
    print '%s %s Client closed' % addr

def server(port):
    print 'Server starting port: %s' % port
    sock = Socket(AF_INET, SOCK_STREAM)
    sock.bind(('', port))
    sock.listen(10)
    try:
        while True:
#            yield ReadWait(sock)
#            client, addr = sock.accept()
            client, addr = yield sock.accept()
            clients.append(client)
            yield NewTask(handle_client(client, addr))
    finally:
        sock.close()

def alive():
    while True:
        print 'I am alive!'
        yield
        
s = Scheduler()
#s.new(alive())
s.new(server(45002))
s.mainloop()