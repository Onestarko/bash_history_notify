## kafak示例-远程监控终端输入

> 使用pyinotify监听.bash_history文件，使用kafka生产消费

### 一、kafka安装配置
1. 安装java环境

2. 官网下载kafka安装包
```
wget http://archive.apache.org/dist/kafka/1.0.0/kafka_2.12-1.0.0.tgz
```

3. 解压压缩包

4. 配置/config/server.properties
```
advertised.listeners=PLAINTEXT://{{IP}}:9092
```

5. 启动kafka
```
##zookeeper启动
./bin/zookeeper-server-start.sh ./config/zookeeper.properties 

##server启动
./bin/kafka-server-start.sh ./config/server.properties
```

6. 创建topic
```
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic {{topic_name}}
```

### 二、安装python依赖
```
#kafka-python
pip install kafka-python

#pyinotify
pip install pyinotify
```

### 三、服务器端代码

```python
# -*- coding: utf-8 -*-
# @Description: 监听文件变更向kafka发送
# @Author: kowhoy
# @Last Modified by:   kowhoy
# @filename: bash_history_producer.py

import pyinotify
import time
import os
import sys
import json
from kafka import KafkaProducer
import datetime
import socket

bootstrap_servers = "ip:9092"
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
```

### 四、客户端代码
```python
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
    "passwd": "passwd",
    "database": "bash_history",
    "table": "bash_history_log"
}

save_to_tb = True ## 是否存数据库

topic = "bash_history"
bootstrap_servers = ["ip:9092"]
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
```

### 五、开启任务
> 我是使用bcc_00作为唯一的消费者，bcc_00和bcc_01作为两个生产者，修改好代码中的配置数据

1. bcc_00开启消费者
```
python bash_history_consumer.py
```

2. bcc_00 和 bcc_01分别开启生产者
```
python bash_history_producer.py ~/.bash_history
```

3. 新起窗口进行命令行操作，就可以在控制台和数据库看到相关bash

- 00_consumer:
![00_consumer](https://md-img.su.bcebos.com/00_consumer.png)

- 00_producer:
![00_producer](https://md-img.su.bcebos.com/00_producer.png)

- 01_producer:
![01_producer](https://md-img.su.bcebos.com/01_producer.png)

- sql_table(两台bcc内网ip一样):
![sql_table](https://md-img.su.bcebos.com/sql_table.png)
