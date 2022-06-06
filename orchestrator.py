# import socket
# import cx_Oracle
# import time
import threading
from or_fns import *


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
    