import cx_Oracle

queue = "_QUEUE1"

ip = 'localhost'
port = '1521'
SID = 'xe'
dsn_tns = cx_Oracle.makedsn(ip, port, SID)
db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
# db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)

cursor = db.cursor()
cursor.execute('DELETE "' + str(queue) + '" WHERE STATUS = \'C\'')
db.commit()
if cursor:
    cursor.close()
if db:
    db.close()