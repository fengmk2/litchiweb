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
            
#    def register_read(self, fd):
#        self.read_waiting.add(fd)
#        
#    def unregister_read(self, fd):
#        if fd in self.read_waiting:
#            self.read_waiting.remove(fd)
#            return True
#        return False
#    
#    def register_write(self, fd):
#        self.write_waiting.add(fd)
#        
#    def unregister_write(self, fd):
#        if fd in self.write_waiting:
#            self.write_waiting.remove(fd)
#            return True
#        return False
    
    
#class SelectEventHub(EventHub):
#    
#    def poll(self, timeout=0):
#        r, w, _ = select.select(self.read_waiting, self.write_waiting, [], timeout)
#        return r, w
    
    
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
#        r, w, e = [], [], []
#        for fd, events in eventpairs:
#            if events & self.READ: # read ready
#                r.append(fd)
#            if events & self.WRITE: # write ready
#                w.append(fd)
#            if events & self.ERROR: # write ready
#                e.append(fd)
#                print fd, '%x' % events
#        return r, w, e
    
    def register(self, fd, events):
        if fd not in self.fds:
            self.epoll.register(fd, events | self.ERROR)
        else:
            self.epoll.modify(fd, events | self.ERROR)
        super(EPollEventHub, self).register(fd, events)
        
    def unregister(self, fd):
        super(EPollEventHub, self).unregister(fd)
        self.epoll.unregister(fd)

#    def register_write(self, fd):
#        if fd not in self.read_waiting and fd not in self.write_waiting:
#            self.epoll.register(fd, self.WRITE | self.ERROR)
#        else:
#            self.epoll.modify(fd, self.WRITE | self.ERROR)
#        super(EPollEventHub, self).register_write(fd)
#    
#    def unregister_read(self, fd):
#        if super(EPollEventHub, self).unregister_read(fd):
#            self.epoll.unregister(fd)
#            return True
#        return False
#        
#    def unregister_write(self, fd):
#        if super(EPollEventHub, self).unregister_write(fd):
#            self.epoll.unregister(fd)
#            return True
#        return False
        
        
def get_hub():
    try:
        hub = EPollEventHub()
    except:
        hub = SelectEventHub()
    return hub