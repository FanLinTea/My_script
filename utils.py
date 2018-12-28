import requests
import xlrd
import pymysql
import json
import time
from DBUtils.PooledDB import PooledDB
import threadpool
import redis
from pymongo import MongoClient


def citys(city_name=None, city_Jpy=None, index=None, size=None):
    """
    获取城市详细信息, 单个城市,多个城市,或者全部城市. (支持中文,简拼)
    支持分页查询, index:第几页,  size: 每页显示多少
    -------默认返回所有城市信息
    :param city_name: 城市中文名
    :param city_Jpy: 简拼
    :param index: 页码
    :param size: 每页数量
    :return: list []
    """
    if city_name:
        if isinstance(city_name, str):
            body = {
                "filter": {"name": city_name}
            }
            city_info = requests.post('http://config.dapi.zhugefang.com/config/getcityinfo', json=body)
            city_info = json.loads(city_info.text).get('data')[0]
            return city_info

        if isinstance(city_name, list):
            city_info = []
            for i in city_name:
                body = {
                    "filter": {"name": i}
                }
                city_data = requests.post('http://config.dapi.zhugefang.com/config/getcityinfo', json=body)
                city_data = json.loads(city_data.text).get('data')[0]
                city_info.append(city_data)
            return city_info

    if city_Jpy:
        if isinstance(city_Jpy, str):
            body = {
                "filter": {"logogram": city_Jpy}
            }
            city_info = requests.post('http://config.dapi.zhugefang.com/config/getcityinfo', json=body)
            city_info = json.loads(city_info.text).get('data')[0]
            return city_info

        if isinstance(city_Jpy, list):
            city_info = []
            for i in city_Jpy:
                body = {
                    "filter": {"logogram": i}
                }
                city_data = requests.post('http://config.dapi.zhugefang.com/config/getcityinfo', json=body)
                city_data = json.loads(city_data.text).get('data')[0]
                city_info.append(city_data)
            return city_info

    if index:
        if city_name or city_Jpy:
            return '分页不支持输入指定城市'
        if not size:
            return '请输入每页返回的城市数量 size'
        if not isinstance(index, int) or not isinstance(size, int):
            return 'Index和size需要int类型'

        body = {
            "page": {
                "index": index,
                "size": size
            },
            "filter": {

            }
        }
        city_info = []
        city_data = requests.post('http://config.dapi.zhugefang.com/config/getcityinfo', json=body)
        city_info = json.loads(city_data.text).get('data')
        city_num = len(city_info)
        print('共有城市: '+ str(city_num) +'个')
        return city_info

    if not city_name and not city_Jpy and not index and not size:
        city_info = []
        for i in range(1, 10000):
            body = {
                "page": {
                    "index": i,
                    "size": 50
                },
                "filter": {

                }
            }
            city_data = requests.post('http://config.dapi.zhugefang.com/config/getcityinfo', json=body)
            time.sleep(1)
            city_data = json.loads(city_data.text).get('data')
            print(city_data)
            if city_data:
                city_info += city_data
            else:
                break
        city_num = len(city_info)
        print('共有城市: ' + str(city_num) + '个')
        return city_info


def read_excel(file_name, table_name, row_or_col='row', start=0):
    """
    读取 xlsx 表格
    :param file_name:  文件名
    :param table_name:  表名
    :param row_or_col:  读取 每行内容 还是 每列内容?
    :param start:  从第几行 或者 第几列 开始?
    :return: list 套 list
    """
    try:
        workbook = xlrd.open_workbook(file_name)
    except Exception as e:
        return e
    if table_name:
        table = workbook.sheet_by_name(table_name)
    else:
        table = workbook.sheet_by_index(0)
    row_num = table.nrows  # 总行数
    cols_num = table.ncols  # 总列数

    data = []
    if row_or_col:
        if row_or_col == 'row':
            for rownum in range(start, row_num):
                info = table.row_values(rownum)
                data.append(info)
            print(data)
            return data
        if row_or_col == 'col':
            for colnum in range(start, cols_num):
                info = table.col_values(colnum)
                data.append(info)
            print(data)
            return data
    else:
        for rownum in range(start, row_num):
            info = table.row_values(rownum)
            data.append(info)
        print(data)
        return data


class Connect_mysql(object):
    '''
    使用mysql连接池  以及  线程池 执行sql语句
    实例化类的时候  输入想要连接的数据库
    只需要调取 thread_sql 方法  注意参数是列表
    '''

    _mysql_con = []
    _mysql_config = {
        '测试库': {'host': '101.201.119.240', 'port': 3306, 'user': 'afe-rw',
                'password': 'HMugq0Fjz3bK67tHdSFottW2ORwSKpcJ'},
        '新房': {'host': 'mysql.zhugefang.com', 'port': 9543, 'user': 'data_r',
               'password': 'ugtQiLyMAgBUf81tyOoMcRgzIzYOszjL'},
        'tidb': {'host': 'tidb.zhuge.com', 'port': 9902, 'user': 'data_r',
                 'password': 'BQ6Qr1*dIh%##bK3zg5p0M6x@Mkqs&hg'},
        'zhuge_dm': {'host': 'mysql.zhugefang.com', 'port': 9571, 'user': 'dm_rw',
                     'password': 'CszwRk3breCsM5BCH0yDfHLorJM5QB5T'},
        '二手房_新': {'host': 'mysql.zhugefang.com', 'port': 9511, 'user': 'data_rw',
                  'password': 'BQ6Qr1*dIh%##bK3zg5p0M6x@Mkqs&hg'},
        '二手房': {'host': 'mysql.zhugefang.com', 'port': 9521, 'user': 'data_r',
                'password': 'BQ6Qr1*dIh%##bK3zg5p0M6x@Mkqs&hg'},
        '租房': {'host': 'mysql.zhugefang.com', 'port': 9532, 'user': 'data_rw',
               'password': 'BQ6Qr1*dIh%##bK3zg5p0M6x@Mkqs&hg'},
        '临时库': {'host': 'rm-2ze7398d890nw9k12po.mysql.rds.aliyuncs.com', 'port': 3306, 'user': 'data_r',
                'password': 'BQ6Qr1*dIh%##bK3zg5p0M6x@Mkqs&hg'},
    }


    def __init__(self, mysql_name):
        self.mysql = Connect_mysql._mysql_config.get(mysql_name)
        if not self.mysql:
            raise Exception('你输入的数据库别名有误,或者你数据库未配置')
        #  连接池
        try:
            self.pool = PooledDB(pymysql, 5, host=self.mysql.get('host'), user=self.mysql.get('user'),
                            passwd=self.mysql.get('password'), port=self.mysql.get('port'), charset="utf8")
        except Exception as e:
            print(e)

        #  线程池
        self.thread_poll = threadpool.ThreadPool(5)

    def select_sql(self, sql=''):
        conn = self.pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            cursor.execute(sql)
            data = cursor.fetchall()
            print('查询出的数据是: ', data)
            cursor.close()
            return data
        except Exception as e:
            pass
        finally:
            cursor.close()
            conn.close()

    def other_sql(self, sql=''):
        conn = self.pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            cursor.execute(sql)
            info = cursor.rowcount
            if info:
                print('操作数据库成功')
            else:
                print('数据库操作失败')
                print(sql)
            conn.commit()
            return info
        except Exception as e:
            print(e)
        finally:
            cursor.close()
            conn.close()

    def thread_sql(self, sqls):
        if not isinstance(sqls, list):
            raise Exception('需要执行的sql应该放到列表中,这样才能启动多线程')
        if 'select' in sqls[0]:
            request = threadpool.makeRequests(self.select_sql, sqls)
            for req in request:
                self.thread_poll.putRequest(req)
            self.thread_poll.wait()

        else:
            request = threadpool.makeRequests(self.other_sql, sqls)
            for req in request:
                self.thread_poll.putRequest(req)
            self.thread_poll.wait()

def connect_redis(host=None, port=None, redis_url=None):
    if redis_url:
        pool = redis.ConnectionPool.from_url(redis_url)
        r = redis.StrictRedis(connection_pool=pool)
        return r
    else:
        pool = redis.ConnectionPool(host=host, port=port)
        r = redis.StrictRedis(connection_pool=pool)
        return r

def connect_mongo(host=None, port=None, user=None, passwd=None, db=None, table=None):
    if user and passwd:
        uri = f"mongodb://{user}:{passwd}@{host}:{port}"
    else:
        uri = f"mongodb://{host}:{port}"

    client = MongoClient(uri)
    mongo_db = client.get_database(db)
    mongo_tb = mongo_db.get_collection(table)
    return mongo_tb


if __name__ == '__main__':
    r = connect_redis('redis://:zhugeZHAOFANG1116@r-2zefc71473d249c4.redis.rds.aliyuncs.com:6379/0')
    data = r.llen('hefei-MeiliwuApt_backup')
    print(data)