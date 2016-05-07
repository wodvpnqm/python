#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb
import sys

# 打开数据库连接
db1 = MySQLdb.connect("localhost", "ceshi", "ceshi@2015", "test")
db2 = MySQLdb.connect("localhost", "ceshi", "ceshi@2015", "test1")

# 使用cursor()方法获取操作游标 
cursor1 = db1.cursor()
cursor2 = db2.cursor()

# SQL 查询语句
sql = "show tables"
try:
    # 执行SQL语句
    tableNum1 = cursor1.execute(sql)
    tableNum2 = cursor2.execute(sql)
    # 获取所有记录列表
    if tableNum1 != tableNum2:
        raise Exception("Error:数据同步之前请先进行结构同步--数据库%s的表数量%s,数据库%s的表数量%s" % ("test",tableNum1,"test1",tableNum2))
    results1 = cursor1.fetchall()
    results2 = cursor2.fetchall()
except Exception,msg:
    print "异常: %s" % msg 
finally:
    cursor1.close()
    cursor2.close()
    db1.close()
    db2.close()
    sys.exit("同步失败")
# 关闭数据库连接
print "同步成功"
