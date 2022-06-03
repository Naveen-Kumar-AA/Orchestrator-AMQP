import socket
import cx_Oracle
import time
import threading
from ora_fns import *

def checkPing():
    while(True):
        ip = 'localhost'
        port = '1521'
        SID = 'xe'
        dsn_tns = cx_Oracle.makedsn(ip, port, SID)
        db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
        # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)

        cursor = db.cursor()
        cursor.execute('SELECT * FROM "_WORKER_STATUS"')
        result = cursor.fetchall()

        for row in result:
            
            if(int(row[1]) + 10 <= int(time.time())):
                
                updateStatusOffline(row[0])
            else:
                updateStatusOnline(row[0])
        if cursor:
            cursor.close()
        if db:
            db.close()



if __name__ == "__main__":
    # workers_list = ['_WORKER1', '_WORKER2']
    # input_list = getScheduleInput()
    # insertInto('_SCHEDULE_INFO', ['job7', 4, 10, '_WORKER1', str(int(time.time()))])

    # insertInto('_SCHEDULE_INFO', ['job2', 8, 5, '_WORKER2', str(int(time.time()))])

    # creating thread
    next_job = threading.Thread(target=retriveNextJob)
    chk_ping = threading.Thread(target= checkPing)
    # starting thread 1
    next_job.start()    
    
    chk_ping.start()

    while(True):
        getScheduleInput()

    # wait until thread 1 is completely executed
    next_job.join()

    chk_ping.join()
    