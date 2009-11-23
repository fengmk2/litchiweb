#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""echo server demo
"""
from socket import socket, AF_INET, SOCK_STREAM

from litchi.schedule import Scheduler
from litchi.systemcall import ReadWait, NewTask, WriteWait


def accept(sock):
    yield ReadWait(sock)
    yield sock.accept()
    
def send(sock, buffer):
    while buffer:
        yield WriteWait(sock)
        len = sock.send(buffer)
        buffer = buffer[len:]
        
def recv(sock, maxbytes):
    yield ReadWait(sock)
    yield sock.recv(maxbytes)
        

clients = []

def send_to(client, data):
    yield WriteWait(client)
    client.send(data)

def handle_client(client, addr):
    print 'Connection from %s %s' % addr
    while True:
#        yield ReadWait(client)
#        data = client.recv(65536)
        data = yield recv(client, 65536)
        data = data.strip()
        if not data:
            break
        data = '[%s %s] %s\r\n' % (addr[0], addr[1], data)
        for other in clients:
            if other != client:
                yield send(other, data)
#                yield WriteWait(other)
#                other.send(data)
#                yield NewTask(send_to(other, data))
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
    sock = socket(AF_INET, SOCK_STREAM)
    sock.bind(('', port))
    sock.listen(10)
    try:
        while True:
#            yield ReadWait(sock)
#            client, addr = sock.accept()
            client, addr = yield accept(sock)
            clients.append(client)
            yield NewTask(handle_client(client, addr))
    finally:
        sock.close()

def alive():
    while True:
        print 'I am alive!'
        yield
        
s = Scheduler.instance()
#s.new(alive())
s.new(server(45000))
s.mainloop()