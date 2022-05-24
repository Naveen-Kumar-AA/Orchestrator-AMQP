from ast import arg
import socket
import cx_Oracle
import time
import threading
import codecs


#input function
def getInput():
    return ['job1',5,10,'worker1',str(int(time.time()))]



def getResultByJobID(job_id):
    result = ""
    
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute('select utl_raw.cast_to_varchar2(dbms_lob.substr(RESULT)) from "_EXECUTION_TABLE" WHERE job_ID = ' + "'" + job_id + "'")
    result = cursor.fetchall()
    

    result = result[0][0]
    result = result.encode()
    result = codecs.decode(result, "hex_codec")
    result = result.decode()

    
    return result




def insertInto(table_name, input_list):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    insert_stmt = 'INSERT INTO "' + str(table_name) + '" VALUES (' + "'" + str(input_list[0]) + "'" + ',' + str(input_list[1]) + ',' + str(input_list[2]) + ",'" + str(input_list[3]) +"','" + str(input_list[4]) + "'" + ')'

    cursor = db.cursor()
    cursor.execute(insert_stmt)
    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()


def getNextFireTime(current_fire_time, no_of_sec):
    #return next fire time in seconds
    return current_fire_time + (no_of_sec)

def getJobByJobID(job_id):
    job = ""
    
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute('select utl_raw.cast_to_varchar2(dbms_lob.substr(JOB_CONTENT)) from "_DEPLOYED_JOBS" WHERE JOB_ID = ' + "'" + job_id + "'")
    result = cursor.fetchall()
    
    for i in result:
        job = i[0]
    job = job.encode()
    job = codecs.decode(job, "hex_codec")
    job = job.decode()

    
    return job


def sendJobsToExchanger(job_id, worker_id):

    job = getJobByJobID(job_id)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

    # get local machine name
    host = socket.gethostname()                           

    port = 9994

    # connection to hostname on the port.
    s.connect((host, port))                               

    # Receive no more than 1024 bytes
    # tm = s.recv(1024)

    next_job = worker_id + ' ' + job


    s.send(next_job.encode())

    s.close()

def getNextFireTime():
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute('SELECT NEXT_FIRE_TIME FROM "_SCHEDULE_INFO"')
    result = cursor.fetchall()

    
    nft = []
    for i in result:
        nft.append(i[0])
    return nft

#get job id and worker id for corresponding nft    
def getJobidAndWorkerid(nft):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute("SELECT * FROM \"_SCHEDULE_INFO\" WHERE NEXT_FIRE_TIME = " + "'" + str(nft) + "'")
    result = cursor.fetchall()

    job_id = result[0][0]
    worker_id = result[0][3]
    return [job_id, worker_id]

def deleteRow(job_id):

    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    cursor = db.cursor()
    cursor.execute("DELETE FROM \"_SCHEDULE_INFO\" WHERE JOB_ID = " + "'" + job_id + "'")
    db.commit()

    if cursor:
        cursor.close()
    if db:
        db.close()
    
def updateScheduleInfo(job_id):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)
    
    cursor = db.cursor()
    cursor.execute("UPDATE \"_SCHEDULE_INFO\" SET NO_OF_OCCURENCE = NO_OF_OCCURENCE - 1, NEXT_FIRE_TIME = NEXT_FIRE_TIME + TIME_INTERVAL WHERE JOB_ID = '" + job_id + "'")
    db.commit()
    
    cursor.execute("SELECT NO_OF_OCCURENCE FROM \"_SCHEDULE_INFO\"")
    result = cursor.fetchall()

    for i in result:
        if i[0] == 0:
            deleteRow(job_id)

    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()

    

def retriveNextJob():
    while(True):
        epoch_time = getNextFireTime()
        job_worker_list = ""
        for i in epoch_time:
            if(int(i) <= int(time.time())):
                job_worker_list = getJobidAndWorkerid(i)
                
                updateScheduleInfo(job_worker_list[0])
                print(job_worker_list[0], " ", int(time.time()))
                sendJobsToExchanger(job_worker_list[0], job_worker_list[1])



def updateStatusOffline(worker_id):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)
    current_time = str(int(time.time()))
    cursor = db.cursor()
    update_stmt = 'UPDATE "_WORKER_STATUS" SET STATUS  = \'OFFLINE\' WHERE WORKER_ID = \'' + worker_id + '\''
    cursor.execute(update_stmt)
    db.commit()

    if cursor:
        cursor.close()
    if db:
        db.close()
    
def updateStatusOnline(worker_id):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)
    current_time = str(int(time.time()))
    cursor = db.cursor()
    update_stmt = 'UPDATE "_WORKER_STATUS" SET STATUS  = \'ONLINE\' WHERE WORKER_ID = \'' + worker_id + '\''
    cursor.execute(update_stmt)
    db.commit()

    if cursor:
        cursor.close()
    if db:
        db.close()
    



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
    workers_list = ['_WORKER1', '_WORKER2']
    input_list = getInput()
    insertInto('_SCHEDULE_INFO', ['job5', 4, 10, '_WORKER1', str(int(time.time()))])

    insertInto('_SCHEDULE_INFO', ['job6', 8, 5, '_WORKER2', str(int(time.time()))])

    # creating thread
    next_job = threading.Thread(target=retriveNextJob)
    chk_ping = threading.Thread(target= checkPing)
    # starting thread 1
    next_job.start()    
    
    chk_ping.start()

    # wait until thread 1 is completely executed
    next_job.join()

    chk_ping.join()
    