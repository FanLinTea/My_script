import requests
import xlrd
import pymysql
import json


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
            city_data = json.loads(city_data.text).get('data')
            if city_data:
                city_info += city_data
            else:
                break
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
        if row_or_col=='row':
            for rownum in range(start, row_num):
                info = table.row_values(rownum)
                data.append(info)
            return data
        if row_or_col=='col':
            for colnum in range(start, cols_num):
                info = table.col_values(colnum)
                data.append(info)
            return data
    else:
        for rownum in range(start, row_num):
            info = table.row_values(rownum)
            data.append(info)
        return data


def connect_mysql(host, user, password, db):
    mysqldb = pymysql.connect(host, user, password, db)
    cursor = mysqldb.cursor()
    return cursor


if __name__ == '__main__':
    citys()
