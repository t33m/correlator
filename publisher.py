#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
import json

config = {
    'host': HOSTNAME,
    'port': PORT,
    'db': DB,
    'password': PASSWORD 
}

r = redis.StrictRedis(**config)

for p in range(1,10000):
    print p
    msg = json.dumps({'P1': p, 'P2': p + 3})
    r.publish("Events", msg)


    

