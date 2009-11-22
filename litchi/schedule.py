#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""The core scheduler

scheduler = Scheduler()
...
scheduler.new(task1)
...
scheduler.new(taskn)
scheduler.mainloop()

       Design Discussion
• Real operating systems have a strong notion of
  "protection" (e.g., memory protection)
• Application programs are not strongly linked
  to the OS kernel (traps are only interface)
• For sanity, we are going to emulate this
     • Tasks do not see the scheduler
     • Tasks do not see other tasks
     • yield is the only external interface

"""
from types import GeneratorType
from Queue import Queue

from litchi.systemcall import SystemCall
from litchi.io import get_hub


class _Task(object):
    """A task"""
    # use to create unique id
    _taskid = 0
    
    def __init__(self, target):
        """Init the task with target.
        @param target: target must be a coroutine(Generator).
        """
        assert isinstance(target, GeneratorType), 'target must be a Coroutine(Generator)'
        _Task._taskid += 1
        self.taskid = _Task._taskid
        self.target = target
        self.sendval = None
        
    def run(self):
        """Start the task"""
        return self.target.send(self.sendval) # start coroutine
    
    def __str__(self):
        return 'Task %d %r' % (self.taskid, self)


class Scheduler(object):
    """Schedule the task how to run."""
    
    def __init__(self):
        self.ready = Queue() # the ready to run task queue
        self.taskmap = {} # the task dict for use taskid to find match task quickly
        self.exit_waiting = {} # exit waiting tasks
        self.read_waiting = {} # read waiting tasks
        self.write_waiting = {}
        self.hub = get_hub()
        
    def new(self, target):
        """Create a new task, Task's factory method.
        @param target: target must be a coroutine(Generator).
        
        @return: the new task id.
        """
        task = _Task(target)
        self.taskmap[task.taskid] = task
        self.schedule(task) # schedule the task to ready start
        return task.taskid
        
    def schedule(self, task):
        """Schedule a task to ready queue.
        @param task: A Task instance, create by scheduler.new(target).
        """
        self.ready.put(task)
        
    def exit(self, task):
        print '%s terminated' % task
        del self.taskmap[task.taskid] # remove from task dict, because the task is dead.
        # Notify other tasks waiting for exit
        for task in self.exit_waiting.pop(task.taskid, []):
            self.schedule(task)
    
    def wait_for_exit(self, task, wait_taskid):
        """task waitting another task to exit"""
        if wait_taskid in self.taskmap:
            self.exit_waiting.setdefault(wait_taskid, []).append(task)
            return True
        return False
    
    def wait_for_read(self, task, fd):
        self.read_waiting[fd] = task
        self.hub.register_read(fd)
    
    def wait_for_write(self, task, fd):
        self.write_waiting[fd] = task
        self.hub.register_write(fd)
    
    def _iopoll(self, timeout=None):
        """The optional timeout argument specifies a time-out as a floating point number in seconds. 
        When the timeout argument is omitted the function blocks until at least one file descriptor is ready. 
        A time-out value of zero specifies a poll and never blocks."""
        if self.read_waiting or self.write_waiting:
            rlist, wlist = self.hub.poll(timeout)
#            print rlist, wlist
            for fd in rlist:
                self.schedule(self.read_waiting.pop(fd))
                self.hub.unregister_read(fd)
            for fd in wlist:
                self.schedule(self.write_waiting.pop(fd))
                self.hub.unregister_write(fd)
                
    def _io_task(self):
        while True:
            if self.ready.empty(): # only io waiting
                self._iopoll(None) # blocks until at least one file descriptor is ready
            else:
                self._iopoll(0) # a poll and never blocks
            yield
    
    def mainloop(self):
        """start main loop"""
        # schedule io task, launch I/O polls
        self.new(self._io_task())
        
        while self.taskmap:
            task = self.ready.get()
            try:
                result = task.run()
                if isinstance(result, SystemCall): # if systemcall, let call to handle it
                    result.task = task
                    result.scheduler = self
                    result.handle()
                    continue
            except StopIteration:
                self.exit(task)
                continue
            self.schedule(task)