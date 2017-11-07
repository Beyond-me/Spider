import requests
import re
import json

class TB(object):
    def __init__(self, n):
        # 找出url的分页规律
        base_url = 'https://s.taobao.com/list?cat=56974003&bcoffset=12&s='
        # 创建请求头
        self.headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36'
        }
        # 生成前n页的url列表，n表示页数，创建实例是传入
        self.url_list = [base_url+str(i*60) for i in range(n)]

    # 定义一个实例方法，用于接受请求，返回响应
    def get_data(self, url):
        response = requests.get(url, headers=self.headers).content.decode()
        # 由于该网站的内容为js动态加载，因此这里使用正则从js中提取
        resp = re.search(r'"itemlist":(.*)', response)
        return resp.group(1)

    # 定义一个实例方法，用于处理返回的响应，从中提取数据
    def parse_data(self, response):
        # 正则匹配出汽车型号，得到的是一个符合匹配结果的列表(键值对组成的列表，不是字典列表，需要再次处理)
        raw_title = re.findall(r'("raw_title":".*?")', response)
        # 新建一个空列表用于存放汽车型号键值对构成的字典
        raw_title_list = []
        # 将冒号后面的字符串取出，作为键，存到字典中
        for title in raw_title:
            # 新建字典，遍历匹配结果，存汽车型号
            temp = {}
            temp["汽车型号"] = eval(title.split(":")[-1])
            raw_title_list.append(str(temp))

        # 下面的４个列表操作和上面的一样
        nid = re.findall(r'("nid":".*?")', response)
        nid_list = []
        for id in nid:
            temp={}
            temp["id"] = int(eval(id.split(":")[-1]))
            nid_list.append(str(temp))

        # 由价格组成的列表
        price = re.findall(r'("view_price":".*?")', response)
        price_list = []
        for p in price:
            temp = {}
            temp["价格"] = int(eval(p.split(":")[-1]).split(".")[0])
            price_list.append(str(temp))

        # 由地址组成的列表
        item_loc = re.findall(r'("item_loc":".*?")', response)
        item_loc_list = []
        for item in item_loc:
            temp = {}
            temp["地址"] = eval(item.split(":")[-1])
            item_loc_list.append(str(temp))

        # 由url组成的列表
        pic_url = re.findall(r'("pic_url":".*?")', response)
        pic_url_list = []
        for url in pic_url:
            temp = {}
            temp["url"] = eval(url.split(":")[-1])
            pic_url_list.append(str(temp))

        # 使用map映射将上面的列表中的内容进行一对一的提取，得到一个迭代器
        result = map(lambda a,b,c,d,e:[a,b,c,d,e],raw_title_list,nid_list,item_loc_list,price_list,pic_url_list)
        # 创建一个文件用于写入
        with open('taobao.json', 'a') as f:
            # 遍历映射后的列表，将其中的由字典构成的字符串取出
            for i in list(result):
                temp = {}
                # 将五个字典的对应的键值取出来拼接成一个字典，对应一辆车
                for data in i:
                    if "汽车型号" in eval(data).keys():
                        temp["汽车型号"] = eval(data)["汽车型号"]
                    elif "url" in eval(data).keys():
                        temp["url"] = eval(data)["url"]
                    elif "id" in eval(data).keys():
                        temp["id"] = eval(data)["id"]
                    elif "地址" in eval(data).keys():
                        temp["地址"] = eval(data)["地址"]
                    elif "价格" in eval(data).keys():
                        temp["价格"] = eval(data)["价格"]
                    # else:
                    #     continue
                        # 转化成json字符串，写入文件
                f.write(json.dumps(temp,ensure_ascii=False) + ',\n')

    def run(self):
        for url in self.url_list:
            response = self.get_data(url)
            self.parse_data(response)

if __name__ == '__main__':
    tb = TB(10)
    tb.run()
