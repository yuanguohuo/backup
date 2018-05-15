#!/bin/bash

HOST=127.0.0.1
PORT=8191

curl -s "http://$HOST:$PORT/common/test/testlock?me=locker-1" &
curl -s "http://$HOST:$PORT/common/test/testlock?me=locker-2" &
curl -s "http://$HOST:$PORT/common/test/testlock?me=locker-3" &
curl -s "http://$HOST:$PORT/common/test/testlock?me=locker-4" &
curl -s "http://$HOST:$PORT/common/test/testlock?me=locker-5" &
curl -s "http://$HOST:$PORT/common/test/testlock?me=locker-6" &
curl -s "http://$HOST:$PORT/common/test/testlock?me=locker-7" &
curl -s "http://$HOST:$PORT/common/test/testlock?me=locker-8" &

wait
