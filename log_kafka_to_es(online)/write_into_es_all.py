import configparser

import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from kafka import KafkaConsumer
import uuid


def load_conf():
    print("加载配置文件")
    cf = configparser.ConfigParser()
    print('---')
    cf.read("base.conf", encoding="UTF-8")
    print('=====')
    secs = cf.sections()
    if len(secs) == 0:
        raise TypeError("配置文件无效")
    print("配置文件已加载")
    print("解析配置文件")
    conf_dict_list = [dict()]
    for x in range(len(conf_dict_list)):
        sec = secs[x]
        ops_list = cf.options(sec)
        for ops in ops_list:
            conf_dict_list[x][ops] = cf.get(sec, ops)
    print("配置文件解析成功")
    return conf_dict_list[0]


def kafka_to_es():
    consumer = KafkaConsumer(topic,
                             auto_offset_reset=auto_offset_reset,
                             bootstrap_servers=eval(bootstrap_servers))
    print('连接kafka成功,等待数据......')

    es = Elasticsearch(
        ['10.10.100.5:29200', '10.10.100.6:29200','10.10.100.7:29200','10.10.100.8:29200','10.10.100.9:29200','10.10.100.10:29200','10.10.100.11:29200','10.10.100.12:29200','10.10.100.13:29200','10.10.100.14:29200'])

    actions = []
    for message in consumer:
        # 读取kafka数据
        line = message.value.decode().split('|')
        try:
            action = {
                "_index": "log-2018-06",
                "_type": "log",
                "_id": uuid.uuid1(),
                "_source": {
                    'ServerIp': line[0],
                    'SpiderType': line[1],
                    'Level': line[2],
                    'Date': datetime.datetime.strptime(line[3], '%Y-%m-%d %H:%M:%S:%f'),
                    'Type': line[4],
                    'OffSet': line[5],
                    'DockerId': line[6],
                    'WebSiteId': line[7],
                    'Url': line[8],
                    'DateStamp': line[9],
                    'NaviGationId': line[10],
                    'ParentWebSiteId': line[11],
                    'TargetUrlNum': line[12],
                    'Timeconsume': line[13],
                    'Success': line[14],
                    'Msg': line[15],
                    'Extend1': line[16],
                    'Extend2': line[17],
                    'Extend3': line[18],
                }
            }
            actions.append(action)
        except:
            print(message.value.decode())

        if len(actions) == strip_number:
            helpers.bulk(es, actions)
            print('写入成功----------')
            del actions[0:len(actions)]

    if len(actions) > 0:
        helpers.bulk(es, actions)


if __name__ == '__main__':
    # 加载模型
    print("启动程序")
    base_dict = load_conf()
    group_id = base_dict['group_id']
    auto_offset_reset = base_dict['auto_offset_reset']
    bootstrap_servers = base_dict['bootstrap_servers']
    strip_number = int(base_dict['strip_number'])
    topic = base_dict['topic']
    kafka_to_es()
