
import socket
import cx_Oracle
import time
import codecs
from datetime import datetime

host = 'localhost'

def checkPing():
    while(True):
        ip = host
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



def insertJobInto(table_name, input_list):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    insert_stmt = 'INSERT INTO "' + str(table_name) + '" VALUES (' + "'" + str(input_list[0]) + "',utl_raw.cast_to_raw('" + str(input_list[1]) + "'))"
    cursor = db.cursor()
    cursor.execute(insert_stmt)
    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()

def jobDeployement():

    file_name = input("Enter file name : ")
    uniq_job_id = input("Enter unique job ID : ")
    bin_data = open(file_name, 'rb').read()
    # print(bin_data)
    hex_data = codecs.encode(bin_data, "hex_codec")
    # print(hex_data)
    hex_data = hex_data.decode()
    # print(hex_data)
    #hex_data = hex_data.encode()
    #print(hex_data)
    #hex_data = codecs.decode(hex_data, "hex_codec")
    #print(hex_data)
    
    input_list = [uniq_job_id, hex_data]
    # print(input_list)
    insertJobInto("_DEPLOYED_JOBS", input_list)

#input function
def getScheduleInput():
    
    choice = int(input("JOB DEPLOYEMENT OR JOB SCHEDULE (0/1)?? "))
    if choice == 0:
        jobDeployement()
    
    elif choice == 1:
        job_id = input("Enter job ID : ")
        no_of_occurence = int(input("Enter no of occurence : "))
        time_interval = int(input("Enter time interval in seconds : "))
        worker_id = input("Enter worker ID : ")
        table_name = "_SCHEDULE_INFO"
        insertInto(table_name, [job_id, no_of_occurence, time_interval, worker_id, str(int(time.time()))])
    
    else:
        print("Enter a valid choice!!")
        return

    



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
    return current_fire_time + no_of_sec

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



def getNextFireTime():
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute('SELECT NEXT_FIRE_TIME FROM "_SCHEDULE_INFO"')
    result = cursor.fetchall()

    # print(result)
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
    # print(result)
    deleted = False
    for i in result:
        # print(job_id)
        print(i[0])
        if i[0] == 0:
            deleteRow(job_id)
            deleted = True

    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()

    return deleted


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

    next_job = worker_id + ' ' + job_id + ' ' + job


    s.send(next_job.encode())

    s.close()

def getInfiniteJobs(nft):

    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    cursor = db.cursor()


    cursor.execute("SELECT JOB_ID, WORKER_ID FROM \"_SCHEDULE_INFO\" WHERE NEXT_FIRE_TIME = " + "'" + str(nft) + "' AND NO_OF_OCCURENCE = -2")
    # print("SELECT * FROM \"_SCHEDULE_INFO\" WHERE NEXT_FIRE_TIME = " + "'" + str(1654161545) + "' AND NO_OF_OCCURENCE = -1")

    result = cursor.fetchall()
    # print(result)

    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()

    return result



def updateInfiniteJobScheduleInfo(job_id):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)
    
    cursor = db.cursor()
    cursor.execute("UPDATE \"_SCHEDULE_INFO\" SET NEXT_FIRE_TIME = NEXT_FIRE_TIME + TIME_INTERVAL WHERE JOB_ID = '" + job_id + "' AND NO_OF_OCCURENCE = -1")
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
        # print(epoch_time)
        job_worker_list = []
        for i in epoch_time:
            if(int(i) <= int(time.time())):
                job_worker_list = getJobidAndWorkerid(i)
                # print(getInfiniteJobs(i))
                res = getInfiniteJobs(i)
                if res:
                    
                    for i in res:
                        updateInfiniteJobScheduleInfo(i[0])
                        # print(i[0], " ",i[1]," ", int(time.time()))
                        sendJobsToExchanger(i[0],i[1])
                else:    
                    sendJobsToExchanger(job_worker_list[0], job_worker_list[1])
                    updateScheduleInfo(job_worker_list[0])
                    # print(job_worker_list[0]," ",job_worker_list[1], " ", int(time.time()))



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
    


def getExecResult():
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    cursor = db.cursor()
    query = 'select job_name, worker_id, status, start_time, end_time, utl_raw.cast_to_varchar2(result) from "_EXECUTION_TABLE"'
    cursor.execute(query)
    result = cursor.fetchall()
    exe_result = []

    for i in result:
        exe_result.append(list(i))  

    for i in exe_result:
        i[5] = i[5].encode()
        i[5] = codecs.decode(i[5], "hex_codec")
        i[5] = i[5].decode()

    for i in exe_result:
        # print(i[3],datetime.fromtimestamp(int(i[3])))
        i[3] = str(datetime.fromtimestamp(int(i[3])))
        i[4] = str(datetime.fromtimestamp(int(i[4])))
        

    if cursor:
        cursor.close()
    if db:
        db.close()
    return exe_result
    

def getJobContentByJobID(job_id):
    
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)
    # current_time = str(int(time.time()))
    cursor = db.cursor()
    query = 'select utl_raw.cast_to_varchar2(JOB_CONTENT) from "_DEPLOYED_JOBS" where job_id =\'' + str(job_id) + '\''
    print(query)
    cursor.execute(query)
    result = cursor.fetchall()
    db.commit()

    hex_data = result[0][0].encode()
    hex_data = codecs.decode(hex_data, "hex_codec")
    job = hex_data.decode()


    if cursor:
        cursor.close()
    if db:
        db.close()
    return job
    