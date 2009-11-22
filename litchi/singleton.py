#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Implement Singleton base class.
"""

import threading


class Singleton(object):
    """A thread safe singleton base class.
    
    remark: any class cannot inherit from child singleton class, like:
    child1 --> Singleton
    child2 X-> child1, must be child2 --> Singleton
    
    Use case:
class A(Singleton):
    name = 'Child A'
    
a = A.get_instance()
assert id(a) == id(A())

    """

    __lock = threading.Lock()  # lock object
    __instance = None  # the unique instance

    def __new__(cls, *args, **kargs):
        raise NotImplementedError('Must be use get_instance() to create instance of %r' % cls)
#        return cls.get_instance(cls, *args, **kargs)

    def __init__(self, *args, **kargs):
        pass
    
    @classmethod
    def instance(cls, *args, **kargs):
        '''Static method to have a reference to **THE UNIQUE** instance'''
        # Critical section start
        cls.__lock.acquire()
        try:
            if cls.__instance is None:
                # (Some exception may be thrown...)
                # Initialize **the unique** instance
                cls.__instance = object.__new__(cls, *args, **kargs)
                cls.__instance.__init__(*args, **kargs)
        finally:
            #  Exit from critical section whatever happens
            cls.__lock.release()
        # Critical section end
        return cls.__instance
    