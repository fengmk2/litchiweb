#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""example for coroutine trampolining

  Coroutine Trampolining
  • A picture of the control flow
  
      run()            main()        add(x,y)
       |                
       v                 
 m.send(None) ------> starts
                        |
                        v
       sub  <------ yield add(2,2)
        |
        v
sub.send(None) ---------------------> starts
                                        |
                                        v
    result <------------------------ yield x+y
       |
       v
m.send(result) --------> r
                         |
                         v
                       print r



       Some Comments
• This is insane!
• You now have two types of callables
    • Normal Python functions/methods
    • Suspendable coroutines
• For the latter, you always have to use yield for
  both calling and returning values
• The code looks really weird at first glance
"""

# A subroutine
def add(x, y):
    yield x + y # This is insane!
    
# A function that calls a subroutine
def main():
    # The code looks really weird at first glance
    r = yield add(2, 2) # you always have to use yield for both calling and returning values
    print r
    yield

# Here is very simpler scheduler code
def run():
    m = main()
    # An example of a "trampoline"
    sub = m.send(None)
    result = sub.send(None)
    m.send(result)
    
run()