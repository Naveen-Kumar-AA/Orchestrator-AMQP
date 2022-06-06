# importing module
import threading
import cx_Oracle
import time
import subprocess
import codecs
import os




def setLockToZero(queue_id):
    ip = host
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
    ip = host
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    cursor = db.cursor()
    while(True):
        # cursor.execute("SELECT LOCK_V FROM \"_QUEUE_LOCK\" WHERE QUEUE_ID = '" + str(queue_id) + "'")
        #UODATE   from _QUEUE_LOCK set LOCK_V = 0 where QUEUE_ID = "" and LOCK_V = 1;
        result = cursor.callfunc('UPDATE_LOCK',int,[queue_id])
        if result == 1:
                      
            return
            # setLockToZero(queue_id)
            # return True

def releaseLock(queue_id):
    ip = host
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
    ip = host
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
    ip = host
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


def runJob(queue_id, worker_id, job, job_id, job_name):
    global no_of_active_threads
    no_of_active_threads = no_of_active_threads - 1
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
    setStatus(queue_id, "C", job_id)
    res = output
    res = res.encode()
    res = codecs.encode(res, "hex_codec")
    res = res.decode()
    
    updateExeTable(job_id, worker_id, status, start_time, end_time, res, job_name)
    no_of_active_threads = no_of_active_threads + 1


def updateExeTable(job_id, worker_id, status, start_time, end_time, result, job_name):
    insert_stmt = 'INSERT INTO "_EXECUTION_TABLE" VALUES (\'' + str(job_id) + '\',\'' + str(worker_id) + '\',\'' + str(
        status) + '\', \'' + str(start_time) + '\',\'' + str(end_time) + '\', utl_raw.cast_to_raw(\'' + str(result) + '\'), \'' + str(job_name) + '\')'
    ip = host
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


def fetchJob(queue_id,worker_id):
    
    ip = host
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute(
        'SELECT JOB_ID, JOB_NAME, utl_raw.cast_to_varchar2(JOB), STATUS FROM "' + str(queue_id) + '" WHERE STATUS = \'N\' AND ROWNUM = 1')

    result = cursor.fetchall()
    if result:
        curr_job_ID = result[0][0]
        curr_job_name = result[0][1]
        print(curr_job_name)
        # print(curr_job_ID)
        hex_job = result[0][2]
        bin_job = hex_job.encode()
        job = codecs.decode(bin_job, "hex_codec")
        job = job.decode()
        print(job)
        setStatus(queue_id, 'X', curr_job_ID)
        # start_time = str(int(time.time()))
        run_job_thread = threading.Thread(target=runJob, args=(queue_id, worker_id, job, curr_job_ID, curr_job_name))

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









host = 'localhost'
queue_id = '_QUEUE2'
# no_of_available_threads = 5
worker_id = "_WORKER2_1"
no_of_active_threads = 5


if __name__ == "__main__":
    ping_t = threading.Thread(target=ping, args=(worker_id,))
    ping_t.start()
   
    threads = []
    while(True):
        # time.sleep(1)
        if no_of_active_threads:
            # print(no_of_active_threads)
            acquireLock(queue_id)
            threads.append(fetchJob(queue_id,worker_id))
            releaseLock(queue_id)
        # else:
            # print("exeeded max no of threads...wait!")
        
    
    for th in threads:
        th.join()

    ping_t.join()
