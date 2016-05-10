#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb.cursors
import sys
import json
import string
import time
f = file(r'../2.json')
jsonobj = json.load(f)
# 列表用序号来查询

commons = jsonobj[0]["commons"]
# 打开数据库连接
db1 = MySQLdb.connect(host=commons["source"]["host"], 
                      user=commons["source"]["user"], 
                      passwd=commons["source"]["passwd"], 
                      db=commons["source"]["db"],
                      port=string.atoi(commons["source"]["port"]),
                      charset=commons["source"]["charset"],
                      cursorclass= MySQLdb.cursors.DictCursor
                      )
db2 = MySQLdb.connect(host=commons["target"]["host"], 
                      user=commons["target"]["user"], 
                      passwd=commons["target"]["passwd"], 
                      db=commons["target"]["db"],
                      port=string.atoi(commons["target"]["port"]),
                      charset=commons["target"]["charset"],
                      cursorclass= MySQLdb.cursors.DictCursor
                      )
db11 = MySQLdb.connect(host=commons["source"]["host"], 
                      user=commons["source"]["user"], 
                      passwd=commons["source"]["passwd"], 
                      db=commons["source"]["db"],
                      port=string.atoi(commons["source"]["port"]),
                      charset=commons["source"]["charset"]
                      )
db22 = MySQLdb.connect(host=commons["target"]["host"], 
                      user=commons["target"]["user"], 
                      passwd=commons["target"]["passwd"], 
                      db=commons["target"]["db"],
                      port=string.atoi(commons["target"]["port"]),
                      charset=commons["target"]["charset"]
                      )

# 使用cursor()方法获取操作游标 
jsonobj = jsonobj[0]
cursor1 = db1.cursor()
cursor2 = db2.cursor()
cursor11 = db11.cursor()
cursor22 = db22.cursor()
schema1 = commons["source"]["db"]
schema2 = commons["target"]["db"]

#时间转换为字符串
def date_to_varchar(date):
    return time.strftime("%Y-%m-%d %H-%M-%S", date)

#将字符串转换为日期
def varchar_to_date(dateStr):
    time.strptime(dateStr, "%Y-%m-%d %H-%M-%S")

#不变对象
def  constant(source):
    return source



# print table1,table2,schema1,schema2
# SQL 查询语句
# sql1 = "select column_name from information_schema.columns t where t.TABLE_SCHEMA = '"+schema1+"' and t.TABLE_NAME = '" + table1 +"'"
#sql = "select column_name from information_schema.columns t where t.TABLE_SCHEMA = '"+schema2+"' and t.TABLE_NAME = '" + table2+"'"
# print sql1
# print sql2
try:
    datas = jsonobj["data"]
    #对每一个转换关系都有
    result = []
    line = {}
    for data in datas:
        line = data.copy()
        sourceTable = data["source"]
        targetTable = data["target"]
        cursor1.execute("select * from " + sourceTable)
        sourceDataq = cursor1.fetchall()
        options = data["options"]
        #一个关系有多个转换
        for option in options:
            if type == 'delColumn': 
                columnNames = option["columnNames"].split(",")
                for column in columnNames:
                    del line[column]
            if type == 'modifyColumn':
                funcs = option['rules'].split(",") 
                sourceColumns = option['sourceColumns'].split(",") 
                targetColumns = option['targetColumns'].split(",") 
                for index, column in enumerate(columnNames):
                    line[targetColumns[index]] = eval(funcs[index])(line[column])
            if type == 'addColumn':
                columnNames = option["columnNames"].split(",")
                values = option["defaultValues"].split(",")
                for index, column in enumerate(columnNames):
                    line[column] = values[index]
            result.append(line)
        sql = "select column_name from information_schema.columns t where t.TABLE_SCHEMA = '"+schema2+"' and t.TABLE_NAME = '" + targetTable+"'"
        cursor22.execute(sql)        
        resultColumns = cursor22.fetchall()
        str1 = ",".join(map(resultColumns,lambda item:item[0]))
        str2 = ",".join(map(resultColumns,lambda item:item[0]+":"))
        cursor2.executemany("INSERT INTO " + targetTable +" (" + str1 + ")"+" VALUES (" + str2+")",result)
        db2.commit()
#     if tableNum1 != tableNum2:
#         raise Exception("Error:数据同步之前请先进行结构同步--数据库%s的表数量%s,数据库%s的表数量%s" % ("test", tableNum1, "test1", tableNum2))
#     results1 = cursor1.fetchall()
#     results2 = cursor2.fetchall()
except Exception, msg:
    print "异常: %s" % msg 
    sys.exit("同步失败")
finally:
    cursor1.close()
    cursor2.close()
    db1.close()
    db2.close()
    f.close
# 关闭数据库连接
print "同步成功"
