#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""wrap the io select or epoll
"""
import select


class EventHub(object):
    
    # Constants from the epoll module
    _EPOLLIN = 0x001
    _EPOLLPRI = 0x002
    _EPOLLOUT = 0x004
    _EPOLLERR = 0x008
    _EPOLLHUP = 0x010
    _EPOLLRDHUP = 0x2000
    _EPOLLONESHOT = (1 << 30)
    _EPOLLET = (1 << 31)

    # Our events map exactly to the epoll events
    NONE = 0
    READ = _EPOLLIN
    WRITE = _EPOLLOUT
    ERROR = _EPOLLERR | _EPOLLHUP | _EPOLLRDHUP
    
    def __init__(self):
        self.fds = {}
        
    def poll(self, timeout=0):
        raise NotImplementedError()
    
    def register(self, fd, events):
        self.fds[fd] = events
        
    def unregister(self, fd):
        del self.fds[fd]
    
    
class SelectEventHub(EventHub):
    
    def __init__(self):
        super(SelectEventHub, self).__init__()
        
    def poll(self, timeout=0):
        reads = (fd for fd, events in self.fds.iteritems() if events & self.READ)
        writes = (fd for fd, events in self.fds.iteritems() if events & self.WRITE)
        r, w, e = select.select(reads, writes, self.fds, timeout)
        eventpairs = []
        for fd in r:
            eventpairs.append((fd, self.READ))
        for fd in w:
            eventpairs.append((fd, self.WRITE))
        for fd in e:
            eventpairs.append((fd, self.ERROR))
        return eventpairs
    
    
class EPollEventHub(EventHub):
    """A epoll-based hub."""
    
    SIZE_HINT = 50000
    def __init__(self):
        super(EPollEventHub, self).__init__()
        self.epoll = select.epoll(self.SIZE_HINT)
        
    def poll(self, timeout=0):
        if timeout is None:
            timeout = -1
        return self.epoll.poll(timeout)
    
    def register(self, fd, events):
        if fd not in self.fds:
            self.epoll.register(fd, events | self.ERROR)
        else:
            self.epoll.modify(fd, events | self.ERROR)
        super(EPollEventHub, self).register(fd, events)
        
    def unregister(self, fd):
        super(EPollEventHub, self).unregister(fd)
        self.epoll.unregister(fd)
        
        
def get_hub():
    try:
        hub = EPollEventHub()
    except:
        hub = SelectEventHub()
    return hub