#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb.cursors
import sys
import json
import string
import datetime
import logging.config

logging.config.fileConfig("../log.properties")
logger = logging.getLogger("file")

logger.info('---------------数据迁移启动---------------------')


f = file(r'../3.json')
jsonobj = json.load(f)
logger.info('加载配置文件成功:' + str(jsonobj))
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
def date_to_varchar(dateTime):
    return dateTime.strftime("%Y-%m-%d %H:%M:%S")

#将字符串转换为日期
def varchar_to_date(dateStr):
    return datetime.datetime.strptime(dateStr, "%Y-%m-%d %H:%M:%S")

#不变对象
def  constant(source):
    return source

def my_map(iterable,func):
    target =  []
    for item in iterable:
        target.append(func(item))
    return target
    

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
        sourceData = cursor1.fetchall()
        options = data["options"]
        for source in sourceData:
            #一个关系有多个转换
            line = source.copy()
            for option in options:
                typeName = option['type']
                if typeName == 'delColumn': 
                    columnNames = option["columnNames"].split(",")
                    for column in columnNames:
                        del line[column]
                if typeName == 'modifyColumn':
                    columnNames = option["columnNames"]
                    for item in columnNames:
                        if item.has_key('tranfer'):
                            line[item['target']] = eval(item['tranfer'])(line[item['source']])
                        else:
                            line[item['target']] = line[item['source']]
                if typeName == 'addColumn':
                    #columnNames = option["columnNames"].split(",")
                    columnNames = option["columnNames"]
                    for item in columnNames:
                        if item.has_key('default'):
                            line[item['column']] = item['default']
                        else:
                            line[item['column']] = None
            result.append(line)
        sql = "select column_name from information_schema.columns t where t.TABLE_SCHEMA = '"+schema2+"' and t.TABLE_NAME = '" + targetTable+"'"
        cursor22.execute(sql)        
        resultColumns = cursor22.fetchall()
        str1 = ",".join(my_map(resultColumns,lambda item:item[0]))
        str2 = ",".join(my_map(resultColumns,lambda item: "%("+item[0]+")s"))
        sql = "INSERT INTO " + targetTable +" (" + str1 + ")"+" VALUES (" + str2+")"
        logger.info("execute sql sql = " + sql)
        logger.info("execute sql params = " + str(result))
        cursor2.executemany(sql,result)
        db2.commit()
#     if tableNum1 != tableNum2:
#         raise Exception("Error:数据同步之前请先进行结构同步--数据库%s的表数量%s,数据库%s的表数量%s" % ("test", tableNum1, "test1", tableNum2))
#     results1 = cursor1.fetchall()
#     results2 = cursor2.fetchall()
except Exception, msg:
    logger.error( "异常: %s" % msg) 
    logger.error("数据迁移中断退出")
    raise
    sys.exit()
finally:
    cursor1.close()
    cursor2.close()
    db1.close()
    db2.close()
    f.close
# 关闭数据库连接
logger.info("同步成功")
logger.info('---------------数据迁移结束---------------------')