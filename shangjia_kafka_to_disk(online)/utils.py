# -*-coding:utf-8-*-
import os
import datetime
import time


def get_num_content(input_file_f, num):
    content = list()
    for x in range(num):
        line = input_file_f.readline()
        if line:
            content.append(line.strip())
        else:
            break
    return content


def get_file_path_file_name_file_ext(filename):
    file_path, temp_file_name = os.path.split(filename)
    shot_name, extension = os.path.splitext(temp_file_name)
    return file_path, shot_name, extension


def get_time_list(num_day):
    format_str = '%Y-%m-%d'
    now = datetime.datetime.now()
    time_list = list()
    for x in range(num_day):
        time_list.append(now + datetime.timedelta(days=-x))
    return [x.strftime(format_str) for x in time_list]


def get_time_stamp():
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = "%s.%03d" % (data_head, data_secs)
    stamp_01 = "".join(time_stamp.split()[0].split("-"))
    stamp_02 = "".join(":".join(time_stamp.split()[1].split(".")).split(":"))
    stamp = stamp_01 + stamp_02
    return stamp


def get_time_hm():
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%H:%M", local_time).split(":")
    return data_head
