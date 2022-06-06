import queue
import cx_Oracle
import time
import threading
import subprocess
import codecs
import os


def setLockToZero(queue_id):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute("UPDATE \"_QUEUE_LOCK\" SET LOCK_V = 0 WHERE QUEUE_ID = '" + str(queue_id) + "'")
    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()
    return

def acquireLock(queue_id):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute("SELECT LOCK_V FROM \"_QUEUE_LOCK\" WHERE QUEUE_ID = '" + str(queue_id) + "'")
    result = cursor.fetchall()
    if cursor:
        cursor.close()
    if db:
        db.close()
    
    s = int(result[0][0])

    while(True):

        if s == 1:
            setLockToZero(queue_id)
            return True



def releaseLock(queue_id):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute("UPDATE \"_QUEUE_LOCK\" SET LOCK_V = 1 WHERE QUEUE_ID = '" + str(queue_id) + "'")
    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()

    return True




def updateTime(worker_id):
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


def ping(worker_id):
    while(True):
        time.sleep(5)
        updateTime(worker_id)


def setStatus(worker_id, status, job_id):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute('UPDATE "' + str(worker_id) + '" SET STATUS = \'' +
                   str(status) + '\' WHERE JOB_ID = ' + str(job_id))
    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()


def runJob(worker_id, job, job_id):
    global count
    count = count - 1
    start_time = str(int(time.time()))
    try:

        file_name = str(job_id) + '.py'
        print(file_name)
        fp = open(file_name, 'w')
        fp.write(job)
        fp.close()
        output = subprocess.check_output('python ' + file_name)
        print(output)
        output = output.decode()
        status = 'SUCCESS'

    except Exception as e:
        output = "Unexpected error : " + str(e)
        print(output)
        status = 'FAILURE'

    os.remove(file_name)

    end_time = str(int(time.time()))
    setStatus(worker_id, "C", job_id)
    res = output
    res = res.encode()
    res = codecs.encode(res, "hex_codec")
    res = res.decode()
    
    updateExeTable(job_id, worker_id, status, start_time, end_time, res)
    count = count + 1


def updateExeTable(job_id, worker_id, status, start_time, end_time, result):
    insert_stmt = 'INSERT INTO "_EXECUTION_TABLE" VALUES (\'' + str(job_id) + '\',\'' + str(worker_id) + '\',\'' + str(
        status) + '\', \'' + str(start_time) + '\',\'' + str(end_time) + '\', utl_raw.cast_to_raw(\'' + str(result) + '\'))'
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


def fetchJob(worker_id):
    
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute(
        'SELECT JOB_ID, utl_raw.cast_to_varchar2(JOB), STATUS FROM "' + str(worker_id) + '" WHERE STATUS = \'N\' AND ROWNUM = 1')

    result = cursor.fetchall()
    if result:
        curr_job_ID = result[0][0]
        # print(curr_job_ID)
        hex_job = result[0][1]
        bin_job = hex_job.encode()
        job = codecs.decode(bin_job, "hex_codec")
        job = job.decode()
        print(job)
        setStatus(worker_id, 'X', curr_job_ID)
        start_time = str(int(time.time()))
        run_job_thread = threading.Thread(target=runJob, args=(worker_id, job, curr_job_ID))

        run_job_thread.start()
        if cursor:
            cursor.close()
        if db:
            db.close()
        return run_job_thread
        
    if cursor:
        cursor.close()
    if db:
        db.close()
