import cx_Oracle
queue_id = "_QUEUE1"
ip = 'localhost'
port = '1521'
SID = 'xe'
dsn_tns = cx_Oracle.makedsn(ip, port, SID)
db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

cursor = db.cursor()
# while(True):
result = cursor.callfunc('UPDATE_LOCK',int,[queue_id])
print(result)
# if int(result[0][0]):
