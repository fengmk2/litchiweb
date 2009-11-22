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

"""

# A subroutine
def add(x, y):
    yield x + y
    
# A function that calls a subroutine
def main():
    r = yield add(2, 2)
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