from utils import *

db = Connect_mysql('zhuge_dm')
city_list = db.select_sql(sql='select distinct(city) from zhuge_dm.city_source where is_dock=1')
print(city_list)
spider_db = Connect_mysql('二手房')

count = []
for i in city_list:
    if i['city'] == 'bj':
        sql = f'select count(*) from spider.house_sell_gov where source_type = 5 and status = 1'
    else:
        sql = f'select count(*) from spider_{i["city"]}.house_sell_gov where source_type = 5 and status = 1'
    data = spider_db.select_sql(sql=sql)
    if not data:
        continue
    count.append(data[0]['count(*)'])
    print(i['city'], '二手房')


newspider_db = Connect_mysql('二手房_新')
for i in city_list:
    if i['city'] == 'bj':
        sql = f'select count(*) from spider.house_sell_gov where source_type = 5 and status = 1'
    else:
        sql = f'select count(*) from spider_{i["city"]}.house_sell_gov where source_type = 5 and status = 1'
    data = newspider_db.select_sql(sql=sql)
    if not data:
        continue
    count.append(data[0]['count(*)'])
    print(i['city'], '二手房新势力')

rent_db = Connect_mysql('租房')
for i in city_list:
    sql = f'select count(*) from rent_{i["city"]}.house_rent_gov where source_type = 5 and status = 1'
    data = rent_db.select_sql(sql=sql)
    if not data:
        continue
    count.append(data[0]['count(*)'])
    print(i['city'], '租房')

num = 0
for i in count:
    num += i
print(num)