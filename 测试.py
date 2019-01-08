import requests
import json

cityss = requests.get('http://api.zhuge.com/newhouse/api/v1/city/getopenarea')
cityss = json.loads(cityss.text).get('data')
print(cityss)
print(len(cityss))