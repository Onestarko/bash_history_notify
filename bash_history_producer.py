# -*- coding: utf-8 -*-
# @Description: 监听文件变更向kafka发送
# @Author: kowhoy

import pyinotify
import time
import os
import sys
import json
from kafka import KafkaProducer
import datetime
import socket

bootstrap_servers = "localhost:9092"
topic = "bash_history"
host_name = "bcc_00"
host_ip = socket.gethostbyname(socket.getfqdn(socket.gethostname()))

'''
@name: [class]Monitor_file
@date: 2020-03-26 09:41:13
@desc: 监听文件
@param: [str]filename 
@return: 
'''
class Monitor_file:
    def notify_bash_history(self, filename):
        wm = pyinotify.WatchManager()
        notifier = pyinotify.Notifier(wm)
        wm.watch_transient_file(filename, pyinotify.IN_MODIFY, Process_transient_file)
        notifier.loop()

'''
@name: [class]Process_transient_file
@date: 2020-03-26 09:44:29
@desc: 文件发生变化触发
@param: 
@return: 
'''
class Process_transient_file(pyinotify.ProcessEvent):
    def process_IN_MODIFY(self, event):
        line = file.readlines()

        if line:
            if len(line) == 2:
                order_time = int(line[0][1:-1])
                order_time_array = time.localtime(order_time)
                order_time_str = time.strftime("%Y-%m-%d %H:%M:%S", order_time_array)
                order = line[1][:-1]
                msg = {
                    "host_name": host_name,
                    "host_ip": host_ip,
                    "order_time": order_time_str,
                    "order": order
                }
                Send_to_kafka().send_msg(msg)

'''
@name: [class]Send_to_kafka
@date: 2020-03-26 09:47:50
@desc: 向kafka发送msg
@param:
@return: 
'''
class Send_to_kafka():
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def send_msg(self, msg):
        msg = json.dumps(msg, ensure_ascii=False).encode("utf-8")

        self.producer.send(topic, msg)

        print("-"*10, "数据已发送", "-"*10)
        print(msg)

        self.producer.flush()

if __name__ == "__main__":
    filename = sys.argv[1]

    if not os.path.exists(filename):
        raise FileExistsError

    file_stat = os.stat(filename)

    file_size = file_stat[6]

    file = open(filename, "r")
    file.seek(file_size)

    Monitor_file().notify_bash_history(filename)

