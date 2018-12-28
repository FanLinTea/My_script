from utils import *
import re
import time

city = citys()
excel = read_excel('ss.xlsx', '价格区间', start=2)
db = Connect_mysql('测试库')
city_all = []
r = re.compile(r"text:\'(.*)\'")
for k in excel:
    city_all.append(k[0])


# print('????????????',city_all)


def insert_data():
    sqls = []
    for i in city:
        city_name = i.get('name')
        city_id = i.get('id')
        city_spy = i.get('logogram')
        city_fpy = i.get('city_fpy')
        property_type = [{'label': '写字楼', 'value': 7}, {'label': '别墅', 'value': 4}, {'label': '商铺', 'value': 3},
                          {'label': '住宅', 'value': 1}, {'label': '商住', 'value': 6}]
        property_type = json.dumps(property_type, ensure_ascii=False)
        if city_name in city_all:
            for k in excel:
                price = []
                # print(k[0])
                # print(list(k))

                ma = re.search(city_name, str(k))
                if ma:
                    ma = ma.group()
                    del k[0]
                    for s in k:
                        s = str(s)

                        yixia = re.search(r'以下', s)
                        yishang = re.search(r'以上', s)
                        if yixia:
                            num = re.search('\d+', s).group()
                            dict = {'value': '0-%s' % num, 'label': '%s元/平以下' % num}
                            # dict = json.dumps(dict, ensure_ascii=False)
                        elif yishang:
                            num = re.search('\d+', s).group()
                            dict = {'value': '%s-999999999' % num, 'label': '%s元/平以上' % num}
                            # dict = json.dumps(dict, ensure_ascii=False)
                        else:
                            num = re.search('\d+-\d+', s)
                            if num:
                                num = num.group()
                                dict = {'value': '%s' % num, 'label': '%s元/平' % num}
                                # dict = json.dumps(dict, ensure_ascii=False)
                            else:
                                continue
                        price.append(dict)

                    price_range = json.dumps(price, ensure_ascii=False)
                    times = int(time.time());
                    sql = f"insert into spider.city_bladeinfo_copy1 (city_id,city_name,price_range,property_type,status,ctime,city_fpy,city_spy,commercial_type) values ({city_id},'{city_name}','{price_range}','{property_type}',1,{times},'{city_fpy}','{city_spy}',0)"

                    sqls.append(sql)
                    # try:
                    #     db.execute(sql)
                    #     print(sql)
                    # except Exception as e:
                    #     print('错误的sql:----', sql)
                    # break

        else:
            times = int(time.time())
            price = [{"value": "0-4000", "label": "4000元/平以下"}, {"value": "4000-5000", "label": "4000-5000元/平"}, {"value": "5000-6000", "label": "5000-6000元/平"}, {"value": "6000-7000", "label": "6000-7000元/平"}, {"value": "7000-8000", "label": "7000-8000元/平"}, {"value": "8000-10000", "label": "8000-10000元/平"}, {"value": "10000-999999999", "label": "10000元/平以上"}]
            price_range = json.dumps(price, ensure_ascii=False)
            sql = f"insert into spider.city_bladeinfo_copy1 (city_id,city_name,price_range,property_type,status,ctime,city_fpy,city_spy,commercial_type) values ({city_id},'{city_name}','{price_range}','{property_type}',1,{times},'{city_fpy}','{city_spy}',0)"
            sqls.append(sql)
            # try:
            #     db.execute(sql)
            # except Exception as e:
            #     print(sql)
    # db.close()
    db.thread_sql(sqls)

if __name__ == '__main__':
    insert_data()

    '''更新商住 为 商业'''
    property_type = [{"label": "写字楼", "value": 7}, {"label": "别墅", "value": 4}, {"label": "商铺", "value": 3}, {"label": "住宅", "value": 1}, {"label": "商业", "value": 6}]
    citytt = ['北京','佛山','广州','泉州','石家庄','珠海','成都','东莞','福州','厦门','上海']
    property_type = json.dumps(property_type, ensure_ascii=False)
    sqls = []
    for i in citytt:
        sql = f"UPDATE spider.city_bladeinfo_copy1 SET property_type = '{property_type}' WHERE city_name = '{i}'"
        sqls.append(sql)
    db.thread_sql(sqls)

    '''更新  商业化城市  commercial_type =1'''
    excel = read_excel('新房商业化城市名单.xlsx', '工作表1',row_or_col='row', start=0)
    cityss = []
    for i in excel:
        cityss.append(i[0])
    sqls = []
    for k in cityss:
        sql = f"UPDATE spider.city_bladeinfo_copy1 SET commercial_type = 1 WHERE city_name = '{k}'"
        sqls.append(sql)
    db.thread_sql(sqls)










