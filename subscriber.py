#!/usr/bin/env jython
# -*- coding: utf-8 -*-

import sys
import json
import redis
import random
import java.util.Map as Map
import java.lang.Double 
from jython_utils import *
from jycep import EsperEngine
from jycep import EsperStatement
from jycep import EventListener

config = {
    'host': HOSTNAME,
    'port': PORT,
    'db': DB,
    'password': PASSWORD 
    }

r = redis.StrictRedis(**config)

pubsub = r.pubsub()
pubsub.subscribe("Events")

cep = EsperEngine("CEPEngine")

cep.define_event("Events", {"eventlog_id": jint,
                            "Status": jstr,
                            "TargetUserName": jstr,
                            "syslog_message": jstr,
                            "syslog_hostname": jstr,
                            "syslog_program": jstr})

def callback(stmtname, data_new, data_old):
    print "#" * 30
    print "\n%s" % stmtname
    if data_old:
        print("Old Data:" + str(data_old))
    if data_new:
        print("New Data:" + str(data_new))

stmtname = 'EventLog: Bruteforce blocked accounts'
stmt = cep.create_query('select * from Events(eventlog_id = 4776 and Status = "0xc0000234").win:time(120 sec) GROUP BY TargetUserName HAVING count(eventlog_id) > 5')
stmt.addListener(EventListener(callback, stmtname))

stmtname = 'EventLog: Account blocked multiple times (5)'
stmt = cep.create_query('select * from Events(eventlog_id = 4740).win:time(24 hours) GROUP BY TargetUserName HAVING count(eventlog_id) > 5')
stmt.addListener(EventListener(callback, stmtname))

stmtname = 'Syslog: Linux SSH bruteforce'
stmt = cep.create_query('select * from Events(syslog_message like "Failed password for%").win:time(120 sec) GROUP BY syslog_hostname HAVING count(syslog_message) > 5')
stmt.addListener(EventListener(callback, stmtname))

while True:
    for item in pubsub.listen():
        if item['type'] != 'subscribe':
            event = json.loads(item['data'])
            cep.send_event(event, "Events")    

