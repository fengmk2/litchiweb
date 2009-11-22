#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""wrap the io select or epoll
"""
import select


class EventHub(object):
    
    def __init__(self):
        self.read_waiting = set()
        self.write_waiting = set()
        
    def poll(self, timeout=0):
        raise NotImplementedError()
    
    def register_read(self, fd):
        self.read_waiting.add(fd)
        
    def unregister_read(self, fd):
        self.read_waiting.remove(fd)
    
    def register_write(self, fd):
        self.write_waiting.add(fd)
        
    def unregister_write(self, fd):
        self.write_waiting.remove(fd)
    
    
class SelectEventHub(EventHub):
    
    def poll(self, timeout=0):
        r, w, _ = select.select(self.read_waiting, self.write_waiting, [], timeout)
        return r, w
    
    
class EPollEventHub(EventHub):
    '''A epoll-based hub.
    '''
    SIZE_HINT = 50000
    def __init__(self):
        super(EPollEventHub, self).__init__()
        self.epoll = select.epoll(self.SIZE_HINT)
        
    def poll(self, timeout=0):
        if timeout is None:
            timeout = -1
        events = self.epoll.poll(timeout)
        r = (fd for fd, event in events if event in (select.EPOLLIN, select.EPOLLPRI)) # read ready
        w = (fd for fd, event in events if event == select.EPOLLOUT) # write ready
        return r, w
    
    def register_read(self, fd):
        if fd not in self.read_waiting and fd not in self.write_waiting:
            self.epoll.register(fd, select.EPOLLIN | select.EPOLLPRI)
        super(EPollEventHub, self).register_read(fd)
        
    def register_write(self, fd):
        if fd not in self.write_waiting:
            if fd in self.read_waiting:
                self.epoll.modify(fd, select.EPOLLIN | select.EPOLLPRI | select.EPOLLOUT)
            else:
                self.epoll.register(fd, select.EPOLLIN | select.EPOLLPRI | select.EPOLLOUT)
                self.read_waiting.add(fd)
        super(EPollEventHub, self).register_write(fd)
    
    def unregister_read(self, fd):
        super(EPollEventHub, self).unregister_read(fd)
        self.epoll.unregister(fd)
        
    def unregister_write(self, fd):
        super(EPollEventHub, self).unregister_write(fd)
        if fd in self.read_waiting:
            self.epoll.modify(fd, select.EPOLLIN | select.EPOLLPRI)
        else:
            self.epoll.unregister(fd)
        
        
def get_hub():
    try:
        hub = EPollEventHub()
    except:
        hub = SelectEventHub()
    return hub