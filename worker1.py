# importing module
from tracemalloc import start
import cx_Oracle
import time
import threading
import subprocess
import codecs
import os

# select * from "_comment" where post_id = 1 and rownum = 1
worker_id = '_WORKER1'
def updateTime():
    global worker_id
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)
    current_time = str(int(time.time()))
    cursor = db.cursor()
    update_stmt = 'UPDATE "_WORKER_STATUS" SET LAST_UPDATED  = \'' + str(int(time.time())) + '\' WHERE WORKER_ID = \'' + worker_id + '\''
    cursor.execute(update_stmt)
    db.commit()

    if cursor:
        cursor.close()
    if db:
        db.close()
    

def ping():
    while(True):
        time.sleep(5)
        updateTime()
    

def setStatus(status, job_id):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute('UPDATE "_WORKER1" SET STATUS = \'' + str(status) + '\' WHERE JOB_ID = ' + str(job_id))
    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()

def runJob(job, job_id):
    
    try:
        file_name = str(job_id) + '.py'
        print(file_name)
        fp = open(file_name , 'w')
        fp.write(job)
        fp.close()
        output = subprocess.check_output('python ' + file_name)
        print(output)
        result = output.decode()
        status = 'SUCCESS' 
    except Exception as e:
        result = "Unexpected error : " + str(e)
        print(result)
        status = 'FAILURE'
    os.remove(file_name)
    return [result, status]
    
def updateExeTable(job_id, worker_id, status, start_time, end_time, result):
    insert_stmt = 'INSERT INTO "_EXECUTION_TABLE" VALUES (\'' + str(job_id) + '\',\'' + str(worker_id) + '\',\'' + str(status) + '\', \'' + str(start_time) + '\',\'' + str(end_time) + '\', utl_raw.cast_to_raw(\'' + str(result) + '\'))'
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    cursor = db.cursor()
    # print(insert_stmt)
    cursor.execute(insert_stmt)
    db.commit()

    if cursor:
        cursor.close()
    if db:
        db.close()

def fetchJob():
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute('SELECT JOB_ID, utl_raw.cast_to_varchar2(JOB), STATUS FROM "_WORKER1" WHERE STATUS = \'N\' AND ROWNUM = 1')
    
    result = cursor.fetchall()
    if result:
        curr_job_ID = result[0][0]
        # print(curr_job_ID)
        hex_job = result[0][1]
        bin_job = hex_job.encode()
        job = codecs.decode(bin_job, "hex_codec")
        job = job.decode()
        print(job)
        setStatus('X',curr_job_ID)
        start_time = str(int(time.time()))
        res_status_list = runJob(job,curr_job_ID)
        end_time = str(int(time.time()))
        setStatus("C",curr_job_ID)
        res = res_status_list[0]
        res = res.encode()
        res = codecs.encode(res, "hex_codec")
        res = res.decode()
        status = res_status_list[1]
        updateExeTable(result[0][0], '_WORKER1', status, start_time, end_time, res)
    if cursor:
        cursor.close()
    if db:
        db.close()





if __name__ == "__main__":
    ping_t = threading.Thread(target=ping)
    ping_t.start()
    while(True):
        fetchJob()
    ping_t.join()