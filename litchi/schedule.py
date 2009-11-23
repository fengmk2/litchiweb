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
import time
from types import GeneratorType
from Queue import Queue
import logging

from litchi.singleton import Singleton
from litchi.systemcall import SystemCall, KillTask
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
        self.trampolining_stack = []
        
    def close(self):
        self.target.close()
        for t in self.trampolining_stack:
            t.close()
        self.trampolining_stack = []
    
    def run(self):
        """Start the task.
        Implementation coroutine trampolining, detail please see ../examples/trampolining.py
        """
        while True:
            try:
                result = self.target.send(self.sendval) # start coroutine
                if isinstance(result, SystemCall):
                    return result
                if isinstance(result, GeneratorType): # coroutine trampolining
                    # call suspendable coroutines
                    self.trampolining_stack.append(self.target)
                    self.sendval = None
                    self.target = result
                else:
                    if not self.trampolining_stack: # hit top target, just return result
                        return result
                    # else, return result to parent target
                    self.sendval = result
                    self.target = self.trampolining_stack.pop()
            except StopIteration:
#                print 'tramp', self.trampolining_stack
                if not self.trampolining_stack: # top target, just raise, normal exit
                    raise
                self.sendval = None
                self.target = self.trampolining_stack.pop()
    
    def __repr__(self):
        return '<Task %d>' % self.taskid


class Scheduler(Singleton):
    """Schedule the task how to run."""
    
    def __init__(self):
        self.ready = Queue() # the ready to run task queue
        self.taskmap = {} # the task dict for use taskid to find match task quickly
        self.exit_waiting = {} # exit waiting tasks
        self.read_waiting = {} # read waiting tasks
        self.write_waiting = {}
        self.sleep_waiting = {} # task sleeping
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
        del self.taskmap[task.taskid] # remove from task dict, because the task is dead.
        # Notify other tasks waiting for exit
        for task in self.exit_waiting.pop(task.taskid, []):
            self.schedule(task)
        logging.debug('%s terminated, %s, %s' % (task, self.taskmap, self.sleep_waiting))
    
    def wait_for_exit(self, task, wait_taskid):
        """task waitting another task to exit"""
        if wait_taskid in self.taskmap:
            self.exit_waiting.setdefault(wait_taskid, []).append(task)
            return True
        return False
    
    def wait_for_read(self, task, fd):
        self.read_waiting[fd] = task
        self.hub.register(fd, self.hub.READ)
    
    def wait_for_write(self, task, fd):
        self.write_waiting[fd] = task
        self.hub.register(fd, self.hub.WRITE)
    
    def _iopoll(self, timeout=None):
        """The optional timeout argument specifies a time-out as a floating point number in seconds. 
        When the timeout argument is omitted the function blocks until at least one file descriptor is ready. 
        A time-out value of zero specifies a poll and never blocks."""
        error_tasks = []
        if self.read_waiting or self.write_waiting:
            eventpairs = self.hub.poll(timeout)
            READ = self.hub.READ
            WRITE = self.hub.WRITE
            ERROR = self.hub.ERROR
            for fd, events in eventpairs:
                self.hub.unregister(fd)
                if events & READ:
                    self.schedule(self.read_waiting.pop(fd))
                if events & WRITE:
                    self.schedule(self.write_waiting.pop(fd))
                if events & ERROR:
                    if fd in self.read_waiting:
                        error_tasks.append(self.read_waiting.pop(fd))
                    if fd in self.write_waiting:
                        error_tasks.append(self.write_waiting.pop(fd))
        return error_tasks
#            rlist, wlist = list(rlist), list(wlist)
#            print rlist, wlist
#            print self.read_waiting[rlist[0]].taskid
#            for fd in r:
#                self.hub.unregister_read(fd)
#                self.schedule(self.read_waiting.pop(fd))
#            for fd in w:
#                self.hub.unregister_write(fd)
#                self.schedule(self.write_waiting.pop(fd))
#            for fd in e:
#                self.hub.unregister_read(fd)
#                self.hub.unregister_write(fd)
#                task = self.write_waiting.pop(fd)
#                task.close()
#            print self.hub.read_waiting, self.hub.write_waiting
                
    def _io_task(self):
        while True:
            if self.ready.qsize() == 1 and not self.sleep_waiting: # only io waiting
                error_tasks = self._iopoll(None) # blocks until at least one file descriptor is ready
            else:
                error_tasks = self._iopoll(0) # a poll and never blocks
            if error_tasks:
                logging.debug('io error events: %s, %s, %s' % (error_tasks, self.taskmap, self.ready.qsize()))
            if error_tasks:
                for task in error_tasks:
                    yield KillTask(task.taskid)
            else:
                yield
    
    def _check_sleeping_tasks(self):
        while True:
            yield
            if self.sleep_waiting:
                now = time.time()
                wakeups = []
                for taskid, (start_time, seconds) in self.sleep_waiting.iteritems():
                    if now - start_time > seconds:
                        # wake up the task
                        wakeups.append(taskid)
                for taskid in wakeups:
                    task = self.taskmap[taskid]
                    self.schedule(task)
                    del self.sleep_waiting[taskid]
            yield
    
    def wait_for_sleep(self, task, seconds):
        self.sleep_waiting[task.taskid] = (time.time(), seconds)
    
    def mainloop(self):
        """start main loop"""
        # schedule io task, launch I/O polls
        self.new(self._io_task())
        self.new(self._check_sleeping_tasks())
        
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