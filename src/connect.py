# coding=utf-8
import MySQLdb
 
try:
    conn = MySQLdb.connect(host='localhost', user='root', passwd='123', port=3306, charset='utf8')
    cur = conn.cursor()
     
    conn.select_db('zhsq')
    print  '你好'
 
    count = cur.execute('select * from earole')
    print 'there has %s rows record' % count
 
    result = cur.fetchmany(10)
    results = cur.fetchmany(5)
    for r in results:
        print r
#     print 'ID: %s info %s' % result
#  
#    
#  
#     print '==' * 10
#     cur.scroll(0, mode='absolute')
#  
#     results = cur.fetchall()
#     for r in results:
#         print r[1]
#      
#  
#     conn.commit()
    cur.close()
    conn.close()
 
except MySQLdb.Error, e:
    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
