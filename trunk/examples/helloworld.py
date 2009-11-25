#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A very simple small web
"""
from litchi.schedule import Scheduler
from litchi.http import HTTPServer


def handler(request):
    yield 'Hello world'
    
httpserver = HTTPServer(handler)
httpserver.listen(8081)
s = Scheduler.instance(debug=False)
s.new(httpserver.start())
s.mainloop()


"""
TEST:

httperf --hog --timeout=5 --client=0/1 --server=10.20.208.25 --port=8081 --uri=/ --rate=100 --send-buffer=4096 --recv-buffer=16384 --num-conns=1500 --num-calls=20
httperf: warning: open file limit > FD_SETSIZE; limiting max. # of open files to FD_SETSIZE
Maximum connect burst length: 1

Total: connections 1500 requests 30000 replies 30000 test-duration 14.998 s

Connection rate: 100.0 conn/s (10.0 ms/conn, <=2 concurrent connections)
Connection time [ms]: min 6.7 avg 7.5 max 10.0 median 7.5 stddev 0.3
Connection time [ms]: connect 0.2
Connection length [replies/conn]: 20.000

Request rate: 2000.3 req/s (0.5 ms/req)
Request size [B]: 63.0

Reply rate [replies/s]: min 1999.8 avg 2000.0 max 2000.1 stddev 0.2 (2 samples)
Reply time [ms]: response 0.4 transfer 0.0
Reply size [B]: header 99.0 content 11.0 footer 0.0 (total 110.0)
Reply status: 1xx=0 2xx=30000 3xx=0 4xx=0 5xx=0

CPU time [s]: user 3.10 system 11.89 (user 20.7% system 79.3% total 99.9%)
Net I/O: 337.9 KB/s (2.8*10^6 bps)

Errors: total 0 client-timo 0 socket-timo 0 connrefused 0 connreset 0
Errors: fd-unavail 0 addrunavail 0 ftab-full 0 other 0

    

$ ab -k -c 1500 -n 30000 http://10.20.208.25:8081/
This is ApacheBench, Version 2.0.40-dev <$Revision: 1.146 $> apache-2.0
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Copyright 2006 The Apache Software Foundation, http://www.apache.org/

Benchmarking 10.20.208.25 (be patient)
Completed 2000 requests
Completed 4000 requests
Completed 6000 requests
Completed 8000 requests
Completed 10000 requests
Completed 12000 requests
Completed 14000 requests
Completed 16000 requests
Completed 18000 requests
Finished 20000 requests


Server Software:        Litchi/0.1
Server Hostname:        10.20.208.25
Server Port:            8081

Document Path:          /
Document Length:        11 bytes

Concurrency Level:      1500
Time taken for tests:   4.809460 seconds
Complete requests:      20000
Failed requests:        0
Write errors:           0
Keep-Alive requests:    20000
Total transferred:      2680000 bytes
HTML transferred:       220000 bytes
Requests per second:    4158.47 [#/sec] (mean)
Time per request:       360.710 [ms] (mean)
Time per request:       0.240 [ms] (mean, across all concurrent requests)
Transfer rate:          544.14 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   2.6      0      27
Processing:    21   55 186.8     44    4759
Waiting:       20   55 186.8     44    4758
Total:         27   55 187.9     44    4770

Percentage of the requests served within a certain time (ms)
  50%     44
  66%     49
  75%     51
  80%     53
  90%     56
  95%     57
  98%     59
  99%     60
 100%   4770 (longest request)
 

ab -k -c 1500 -n 25000 http://10.20.208.25:8081/
This is ApacheBench, Version 2.0.40-dev <$Revision: 1.146 $> apache-2.0
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Copyright 2006 The Apache Software Foundation, http://www.apache.org/

Benchmarking 10.20.208.25 (be patient)
Completed 2500 requests
Completed 5000 requests
Completed 7500 requests
Completed 10000 requests
Completed 12500 requests
Completed 15000 requests
Completed 17500 requests
Completed 20000 requests
Completed 22500 requests
Finished 25000 requests


Server Software:        Litchi/0.1
Server Hostname:        10.20.208.25
Server Port:            8081

Document Path:          /
Document Length:        11 bytes

Concurrency Level:      1500
Time taken for tests:   6.363659 seconds
Complete requests:      25000
Failed requests:        0
Write errors:           0
Keep-Alive requests:    25000
Total transferred:      3350000 bytes
HTML transferred:       275000 bytes
Requests per second:    3928.56 [#/sec] (mean)
Time per request:       381.820 [ms] (mean)
Time per request:       0.255 [ms] (mean, across all concurrent requests)
Transfer rate:          514.01 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   2.4      0      27
Processing:    18   63 241.4     51    6325
Waiting:       18   63 241.4     51    6325
Total:         30   64 242.4     51    6351

Percentage of the requests served within a certain time (ms)
  50%     51
  66%     55
  75%     58
  80%     59
  90%     62
  95%     63
  98%     64
  99%     66
 100%   6351 (longest request)
"""