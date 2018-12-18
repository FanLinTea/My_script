from utils import *
import re
import time

city = citys()
excel = read_excel('ss.xlsx', '价格区间', 2)
db = connect_mysql('101.201.119.240', 'afe-rw', 'HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ', 'spider')
city_all = []
r = re.compile(r"text:\'(.*)\'")
for k in excel:
    city_all.append(r.findall(str(k[0]))[0])


def insert_data():
    for i in city:
        city_name = i.get('name')
        city_id = i.get('id')
        city_spy = i.get('logogram')
        city_fpy = i.get('city_fpy')
        property_type = [{'label': '写字楼', 'value': 7}, {'label': '豪宅', 'value': 4}, {'label': '商铺', 'value': 3},
                         {'label': '办公', 'value': 2}, {'label': '住宅', 'value': 1}, {'label': '商住', 'value': 6}]
        property_type = json.dumps(property_type, ensure_ascii=False)
        if city_name in city_all:
            for k in excel:
                price = []
                print(k[0])
                print(list(k))

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
                    sql = f"insert into city_bladeinfo (city_id,city_name,price_range,property_type,status,ctime,city_fpy,city_spy,commercial_type) values ({city_id},'{city_name}','{price_range}','{property_type}',1,{times},'{city_fpy}','{city_spy}',1)"

                    try:
                        db.execute(sql)
                        print(sql)
                    except Exception as e:
                        print('错误的sql:----', sql)
                    break

        else:
            times = int(time.time());
            sql = f"insert into city_bladeinfo (city_id,city_name,property_type,status,ctime,city_fpy,city_spy,commercial_type) values ({city_id},'{city_name}','{property_type}',1,{times},'{city_fpy}','{city_spy}',1)"
            try:
                db.execute(sql)
            except Exception as e:
                print(sql)
    db.close()

if __name__ == '__main__':
    insert_data()
    # property_type = [{"label": "写字楼", "value": 7}, {"label": "豪宅", "value": 4}, {"label": "商铺", "value": 3}, {"label": "办公", "value": 2}, {"label": "住宅", "value": 1}, {"label": "商业", "value": 6}]
    # property_type = json.dumps(property_type, ensure_ascii=False)
    # sql = f"UPDATE city_bladeinfo SET property_type = '{property_type}' WHERE city_name = '上海'"
    # db.execute(sql)



