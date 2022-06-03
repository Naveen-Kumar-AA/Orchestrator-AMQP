# importing module
import threading
from w_fns import *


worker_id = '_QUEUE1'


if __name__ == "__main__":
    ping_t = threading.Thread(target=ping, args=(worker_id,))
    ping_t.start()

    threads = []
    while(True):
        threads.append(fetchJob(worker_id))

    for th in threads:
        th.join()

    ping_t.join()
