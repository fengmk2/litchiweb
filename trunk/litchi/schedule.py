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
import logging
from collections import deque, defaultdict

from litchi.utils.singleton import Singleton
from litchi.systemcall import SystemCall
from litchi.io import get_hub


class Task(object):
    """A task"""
    # use to create unique id
    _taskid = 0
    
    def __init__(self, target, name):
        """Init the task with target.
        @param target: target must be a coroutine(Generator).
        """
        assert isinstance(target, GeneratorType), 'target must be a Coroutine(Generator)'
        Task._taskid += 1
        self.taskid = Task._taskid
        self.target = target
        self.sendval = None
        self.trampolining_stack = []
        self.name = name if name is not None else self.__class__.__name__
        
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
        return '<%s %d>' % (self.name, self.taskid)


class _TaskQueue(object):
    """"""
    def __init__(self):
        self.tasks = deque()
        self.taskids = set()
        
    def put(self, task, first=False):
        if first:
            self.tasks.appendleft(task)
        else:
            self.tasks.append(task)
        self.taskids.add(task.taskid)
        
    def get(self):
        task = self.tasks.popleft()
        self.taskids.remove(task.taskid)
        return task
    
    def qsize(self):
        return len(self.taskids)
    
    def __contains__(self, taskid):
        return taskid in self.taskids
    
    def __repr__(self):
        return '%r' % self.taskids


class Scheduler(Singleton):
    """Schedule the task how to run."""
    
    def __init__(self, debug=False):
        self.ready = _TaskQueue() # the ready to run task queue
        self.taskmap = {} # the task dict for use taskid to find match task quickly
        self.exit_waiting = {} # exit waiting tasks
        self.read_waiting = {} # read waiting tasks
        self.write_waiting = {}
        self.sleep_waiting = {} # task sleeping
        self.event_waitting = defaultdict(list) # event waitting list
        self.hub = get_hub()
        self.debug = debug
        if self.debug:
            logging.root.setLevel(logging.DEBUG)
        
    def __repr__(self):
        return """
taskmap: %r
ready: %r
sleep: %r
read waitting: %r
write waitting: %r
exit waitting: %r
---------------------------------------------------------
""" % (self.taskmap, self.ready, self.sleep_waiting, 
       self.read_waiting, self.write_waiting, self.exit_waiting) 
        
    def new(self, target, taskname=None):
        """Create a new task, Task's factory method.
        @param target: target must be a coroutine(Generator).
        
        @return: the new task id.
        """
        task = Task(target, taskname)
        self.taskmap[task.taskid] = task
        self.schedule(task) # schedule the task to ready start
        return task.taskid
        
    def schedule(self, task):
        """Schedule a task to ready queue.
        @param task: A Task instance, create by scheduler.new(target).
        """
        if task.taskid not in self.ready:
            self.ready.put(task)
        
    def exit(self, task):
        del self.taskmap[task.taskid] # remove from task dict, because the task is dead.
        # Notify other tasks waiting for exit
        for task in self.exit_waiting.pop(task.taskid, []):
            self.schedule(task)
        if self.debug:
            logging.debug('%s terminated\n%r' % (task, self))
    
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
    
    def kill_tasks(self, taskids):
        """Kill tasks"""
        killids = []
        for taskid in taskids:
            task = self.taskmap.get(taskid, None)
            if task:
                task.close() # close task
                killids.append(taskid) # tell the caller if success kill
                # not in the ready queue, add to, make sure the target raise StopIteration
                if taskid not in self.ready: 
                    self.schedule(task)
        return killids
    
    def _iopoll(self, timeout=None):
        """The optional timeout argument specifies a time-out as a floating point number in seconds. 
        When the timeout argument is omitted the function blocks until at least one file descriptor is ready. 
        A time-out value of zero specifies a poll and never blocks."""
        error_tasks, error_fds = [], []
        if self.read_waiting or self.write_waiting:
            eventpairs = self.hub.poll(timeout)
            READ = self.hub.READ
            WRITE = self.hub.WRITE
            ERROR = self.hub.ERROR
            for fd, events in eventpairs:
                self.hub.unregister(fd)
                if events & ERROR:
                    if fd in self.read_waiting:
                        error_tasks.append(self.read_waiting.pop(fd))
                    if fd in self.write_waiting:
                        error_tasks.append(self.write_waiting.pop(fd))
                    error_fds.append((fd, '0x%X' % events))
                else:
                    if events & READ:
                        self.schedule(self.read_waiting.pop(fd))
                    if events & WRITE:
                        self.schedule(self.write_waiting.pop(fd))
        if error_tasks and self.debug:
            logging.debug('io error events: %s\n%r' % (zip(error_tasks, error_fds), self))
        return error_tasks
                
    def _io_task(self):
        while True:
            if self.ready.qsize() == 1 and not self.sleep_waiting: # only io waiting
                error_tasks = self._iopoll(None) # blocks until at least one file descriptor is ready
            else:
                error_tasks = self._iopoll(0) # a poll and never blocks
            if error_tasks:
                self.kill_tasks((t.taskid for t in error_tasks))
            yield
    
    def _check_sleeping_tasks(self):
        """Check sleeping tasks"""
        while self.sleep_waiting:
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
        if not self.sleep_waiting:
            self.new(self._check_sleeping_tasks(), 'CheckSleepTask') # start sleep check
        self.sleep_waiting[task.taskid] = (time.time(), seconds)
        
    def wait_for_event(self, event, task):
        self.event_waitting[event].append(task)
        
    def fire_event(self, event, value):
        if event not in self.event_waitting:
            return
        tasks = self.event_waitting.pop(event)
        if tasks:
            print tasks
            for task in tasks:
                task.sendval = value
                self.schedule(task)
    
    def mainloop(self):
        """start main loop"""
        # schedule io task, launch I/O polls
        self.new(self._io_task(), 'IOTask')
        
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