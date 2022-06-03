import socket                                         
import time
import cx_Oracle
import codecs
import threading

workers_list = ['_QUEUE1','_QUEUE2']


def cleanQueues():
    global workers_list
    while(True):
        time.sleep(5)
        for queue in workers_list:
            # print(queue)
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



# select job_id_seq.nextval as job_id from DUAL;
def selectSeqNo():
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    # db = cx_Oracle.connect('SYS/1025@localhost:1521/xe', cx_Oracle.SYSDBA)

    cursor = db.cursor()
    cursor.execute('select job_id_seq.nextval as job_id from DUAL')
    result = cursor.fetchall()
    if cursor:
        cursor.close()
    if db:
        db.close()
    return result[0][0]


def insertInto(table_name, job, job_id):

    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)
    
    insert_stmt = 'INSERT INTO "' + str(table_name) + '" VALUES (' + str(job_id) + ',utl_raw.cast_to_raw(' + "'" + str(job) +"'" + '), \'N\')'
    print(insert_stmt)
    cursor = db.cursor()
    cursor.execute(insert_stmt)
    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()

def writeJobToQueue(worker_id, job):
    bin_data = job.encode()
    hex_data = codecs.encode(bin_data, "hex_codec")
    hex_data = hex_data.decode()
    print("WORKERS_LIST : ", workers_list)
    for i in workers_list:
        if worker_id == i:
            print("I, JOB : ",i, hex_data)
            job_id = selectSeqNo()
            insertInto(i, hex_data, job_id)
            

clean_Qs = threading.Thread(target=cleanQueues)
clean_Qs.start()

# create a socket object
serversocket = socket.socket( socket.AF_INET, socket.SOCK_STREAM) 

# get local machine name
host = socket.gethostname()                           

port = 9994                         

# bind to the port
serversocket.bind((host, port))                                  

# queue up to 5 requests
serversocket.listen(5)                                           

while True:
    # establish a connection
    clientsocket,addr = serversocket.accept()      
    print("Got a connection from %s" % str(addr))
    # currentTime = time.ctime(time.time()) + "\r\n"
    # clientsocket.send(currentTime.encode('ascii'))
    


    message = clientsocket.recv(1024)
    job_content = message.decode()
    
    if job_content:
        w_id_job_list = job_content.split(' ', 1)
        worker_id = w_id_job_list[0]
        job = w_id_job_list[1]

        
        writeJobToQueue(worker_id, job)


    clientsocket.close()


clean_Qs.join()
