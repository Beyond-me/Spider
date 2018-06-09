import configparser
import os
from kafka import KafkaConsumer
import utils
"""
消费kafka商家数据落磁盘

1、根据第二个字段来建立目录
>>>>1.新闻(news)，2.微博(weibo)，3.微信(weixin)，4.APP(app)，5.报刊(newspaper)，6.论坛(luntan)，7.博客(blog)，8.视频(video)，9.商机(shangji)，
    10.商家(shangjia)，11.国土资源(gtzy)，12.政府招投标(zfztb)，13. 公益扶贫救孤脱贫(gyfp)，14.高精准(gjz)，15.政企学校(zfxx)，
    16.普通招投标(ptztb)，17.企业(company)，18.房产(house)，19.医院(hospital)，20.银行(bank)，21.园区(zone)，
    22.快递网点(express)，23.招聘岗位(zpgw)，24.知识产权（商标，专利，出版社，著作权）(zscq)，25.酒店(hotel)，26.裁判文书(cpws)，27.高新企业(gxqy)，
    28.股票基金(gpjj)，29.地图应用（百度，高德，腾讯）(dtyy)，30.百度百科应用(bdbk)
    
2、根据第7个字段创建文件名，如 20180601174033543_m-xmm.tmp 

3、当文件写入行数达到strip_number（可配置）时，自动将tmp（临时文件）转为out（完整文件）

4、如果遇到进程意外挂掉的情况，需二次启动，二次启动需要能够自动修复上次挂掉产生的临时文件
"""


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


def kafka_to_disk():
    print('启动前检测上次运行时是否存在意外中断的数据文件......')
    print('搜索最近一次执行脚本产生的时间目录......')
    # 待处理临时文件列表
    tmp_list = []
    try:
        for category_dir in os.listdir(local_file_path):
            if len(os.listdir(local_file_path+os.sep+category_dir)) > 0:
                for date_dir in os.listdir(local_file_path+os.sep+category_dir):
                    if len(os.listdir(local_file_path+os.sep+category_dir+os.sep+date_dir)) > 0:
                        for file in os.listdir(local_file_path+os.sep+category_dir+os.sep+date_dir):
                            if suffix in file:
                                tmp_list.append(local_file_path+os.sep+category_dir+os.sep+date_dir+os.sep+file)
    except Exception as e:
        pass
    if len(tmp_list) == 0:
        print('未扫描任何残留临时文件')
    else:
        print('开始修复残留临时文件......')
    tmp_num = 0
    for tmp in tmp_list:
        os.rename(tmp, tmp.split('.')[0]+'.out')
        tmp_num += 1
    print('本次启动共修复残留临时文件★★★★★-----{}个-----★★★★★'.format(tmp_num))

    category_poor = {
        '1': 'news', '2': 'weibo', '3': 'weixin', '4': 'app', '5': 'newspaper', '6': 'luntan',
        '7': 'blog', '8': 'video', '9': 'shangji', '10': 'shangjia', '11': 'gtzy', '12': 'zfztb',
        '13': 'gyfp', '14': 'gjz', '15': 'zfxx', '16': 'ptztb', '17': 'company', '18': 'house',
        '19': 'hospital', '20': 'bank', '21': 'zone', '22': 'express', '23': 'zpgw', '24': 'zscq',
        '25': 'hotel', '26': 'cpws', '27': 'gxqy', '28': 'gpjj', '29': 'dtyy', '30': 'bdbk'}

    time_stamp = utils.get_time_stamp()  # 初始化毫秒级时间戳 ： 20180509103015125
    consumer = KafkaConsumer(topic, group_id=group_id, auto_offset_reset=auto_offset_reset, bootstrap_servers=eval(bootstrap_servers))
    print('连接kafka成功,数据筛选中......')
    file_poor = {}                          # 子类池用于文件计数器
    time_stamp_poor = {}                    # 子类时间戳池，用于触发文件切换
    time_stamp = utils.get_time_stamp()     # 初始化毫秒级时间戳 ：20180509103015125
    for message in consumer:

        # 提取第8个字段自动匹配目录进行创建
        if message.value.decode().split('|')[1] in category_poor:
            category = category_poor[message.value.decode().split('|')[1]]
        else:
            print(message.value.decode())
            continue
        category_dir = local_file_path + os.sep + category
        time_dir = category_dir + os.sep + time_stamp[0:8]
        if not os.path.exists(time_dir):
            os.makedirs(time_dir)

        # 提取第2个字段，用于生成文件名
        if message.value.decode().split('|')[7] in time_stamp_poor:
            shot_file_name = time_stamp_poor[message.value.decode().split('|')[7]] + '_' + message.value.decode().split('|')[7]
        else:
            shot_file_name = time_stamp + '_' + message.value.decode().split('|')[7]
        file_name = time_dir + os.sep + shot_file_name + '.tmp'

        # 给每一个文件设定一个计数器
        if message.value.decode().split('|')[7] not in file_poor:
            file_poor[message.value.decode().split('|')[7]] = 0

        with open(file_name, 'a', encoding='utf-8')as f1:
            f1.write(message.value.decode())
            file_poor[message.value.decode().split('|')[7]] += 1

        # 触发切换文件的操作,用时间戳生成第二文件名
        if file_poor[message.value.decode().split('|')[7]] == strip_number:
            os.rename(file_name, file_name.split('.')[0]+'.out')
            time_stamp_poor[message.value.decode().split('|')[7]] = utils.get_time_stamp()
            file_poor[message.value.decode().split('|')[7]] = 0


if __name__ == '__main__':
    # 加载模型
    print("启动程序")
    base_dict = load_conf()
    group_id = base_dict['group_id']
    auto_offset_reset = base_dict['auto_offset_reset']
    local_file_path = base_dict['local_file_path']
    bootstrap_servers = base_dict['bootstrap_servers']
    strip_number = int(base_dict['strip_number'])
    topic = base_dict['topic']
    suffix = base_dict['suffix']
    strip_time = int(base_dict['strip_time'])
    kafka_to_disk()
