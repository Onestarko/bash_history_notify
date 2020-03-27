#!/usr/bin/env python
# coding=utf-8
# -*- coding: utf-8 -*-
# @Description: 
# @Author: kowhoy
# @Date:   2020-03-26 10:15:24
# @Last Modified time: 2020-03-26 11:25:15

from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import pandas as pd
from sqlalchemy import create_engine
import json
import sys
import os

dbh_config = {
    "host": "localhost",
    "user": "root",
    "passwd": "",
    "database": "bash_history",
    "table": "bash_history_log_new"
}

save_to_tb = True ## 是否存数据库

topic = "bash_history"
bootstrap_servers = ["localhost:9092"]
single_file_size = 1 #M

class Bash_history_consumer:
    def __init__(self):
        self.topic = "bash_history"
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)

        self.log_file_path = "./bash_history_log/"

        log_files = os.listdir(self.log_file_path)

        max_log_count = max([int(filename.split("_")[-1]) for filename in log_files]) if len(log_files) > 0 else -1

        self.count = max_log_count + 1

        self.save_list = []

        if save_to_tb:
            e = "mysql+pymysql://%s:%s@%s/%s"%(dbh_config["user"], dbh_config["passwd"], dbh_config["host"], dbh_config["database"])
            self.dbh = create_engine(e)

            empty_sql = 'select * from information_schema.TABLES where TABLE_SCHEMA = "{db}" and TABLE_NAME = "{tb}"'.\
            format(db=dbh_config["database"], tb=dbh_config["table"])

            empty_df = pd.read_sql_query(empty_sql, self.dbh)

            self.tb_empty = empty_df.empty


    def bash_consumer(self):
        self.consumer.assign([TopicPartition(topic=topic, partition=0)])

        for msg in self.consumer:
            msg_offset = msg.offset
            msg_value = (msg.value).decode("utf-8")
            msg_value = json.loads(msg_value)

            self.save_list.append(msg_value)

            save_data_size = sys.getsizeof(self.save_list)

            if save_data_size >= single_file_size * 1024 * 1024:
                save_file = self.log_file_path + "bash_history_log_" + str(self.count)

                self.count += 1 
                with open(save_file, "w+") as f:
                    for line in self.save_list:
                        f.write(line.decode("utf-8")+"\n")

            self.save_list = []
            print("*"*10, "写入文件", "*"*10)


            if save_to_tb:
                if self.tb_empty:
                    add_way = "replace"
                    self.tb_empty = False
                else:
                    add_way = "append"
                    

                for k, v in msg_value.items():
                    msg_value[k] = [v]
                insert_df = pd.DataFrame.from_dict(msg_value)

                insert_df.to_sql(dbh_config["table"], self.dbh, if_exists=add_way, index=False)

            print("当前size", save_data_size, "\t", msg_value)

if __name__ == "__main__":
    Bash_history_consumer().bash_consumer()
