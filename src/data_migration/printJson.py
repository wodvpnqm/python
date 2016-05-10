# -*-encoding:utf-8-*-
import json
f = file(r'../2.json')
jsonobj = json.load(f)
# 列表用序号来查询
print jsonobj[0]["commons"]["source"]["host"]
f.close
