#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""System call, the task target could call, and get the info from scheduler.

             System Calls
• In a real operating system, traps are how
  application programs request the services of
  the operating system (syscalls)
• In our code, the scheduler is the operating
  system and the yield statement is a trap
• To request the service of the scheduler, tasks
  will use the yield statement with a value

              Task Management
• Let's make more some system calls
• Some task management functions
     • Create a new task
     • Kill an existing task
     • Wait for a task to exit
• These mimic common operations with
  threads or processes

"""
from types import GeneratorType


class SystemCall(object):
    """A system call base interface."""
    def handle(self):
        raise NotImplementedError('MUST be implement by the child')
    
class Sleep(SystemCall):
    """Sleep Call, if seconds == 0, task will add to the end of schedule queue."""
    def __init__(self, seconds=0):
        self.seconds = seconds
        
    def handle(self):
        if self.seconds <= 0:
            self.scheduler.schedule(self.task)
        else:
            self.scheduler.wait_for_sleep(self.task, self.seconds)
    
class GetTaskid(SystemCall):
    """Get the related task id"""
    
    def handle(self):
        self.task.sendval = self.task.taskid
        self.scheduler.schedule(self.task, True)
        
class NewTask(SystemCall):
    def __init__(self, target):
        """target create a new task call.
        @param target: target must be a Coroutine(Generator)
        """
        assert isinstance(target, GeneratorType), 'target must be a Coroutine(Generator)'
        self.target = target
        
    def handle(self):
        taskid = self.scheduler.new(self.target)
        self.task.sendval = taskid # return the new task id to the caller
        self.scheduler.schedule(self.task)
        
class KillTask(SystemCall):
    def __init__(self, taskids):
        if isinstance(taskids, int):
            taskids = [taskids]
        self.taskids = taskids
        
    def handle(self):
        self.task.sendval = self.scheduler.kill_tasks(self.taskids)
        self.scheduler.schedule(self.task, True)
        
class WaitTask(SystemCall):
    """
          Design Discussion
• The only way for tasks to refer to other tasks
  is using the integer task ID assigned by the the
  scheduler
• This is an encapsulation and safety strategy
• It keeps tasks separated (no linking to internals)
• It places all task management in the scheduler
  (which is where it properly belongs)

    """
    def __init__(self, wait_taskid):
        self.wait_taskid = wait_taskid
        
    def handle(self):
        result = self.scheduler.wait_for_exit(self.task, self.wait_taskid)
        self.task.sendval = result
        # If waiting for a non-existent task,
        # return immediately without waiting
        if not result:
            self.scheduler.schedule(self.task)

class ReadWait(SystemCall):
    """Waiting for file descriptor readable"""
    def __init__(self, f):
        self.f = f
    
    def handle(self):
        fd = self.f.fileno()
        self.scheduler.wait_for_read(self.task, fd)
        
class WriteWait(SystemCall):
    """Waiting for file descriptor writable"""
    def __init__(self, f):
        self.f = f
    
    def handle(self):
        fd = self.f.fileno()
        self.scheduler.wait_for_write(self.task, fd)
        
class Wait(SystemCall):
    """Wait for some event happened"""
    def __init__(self, event):
        self.event = event
        
    def handle(self):
        self.scheduler.wait_for_event(self.event, self.task)
        
class Fire(SystemCall):
    def __init__(self, event, value=None):
        self.event = event
        self.value = value
    
    def handle(self):
        self.scheduler.fire_event(self.event, self.value)
        self.scheduler.schedule(self.task, True) # let task finish