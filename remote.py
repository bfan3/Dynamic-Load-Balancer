
▽
#authoer Bowen Fan

import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

                                                               12,1          Top
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

#Parameters that are set by user
numWorkThreads = 2
throttle = 0.0
cpu_usage = -1.0
ETA_finish = -1.0 #approximate finish time
                                                                      12,1          Top
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

#Parameters that are set by user
numWorkThreads = 2
throttle = 0.0
cpu_usage = -1.0
ETA_finish = -1.0 #approximate finish time

#Helper function that pipelines all TCP sends for consistency
def sendData(data):
        with lock:
                                                                           12,1          Top
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

#Parameters that are set by user
numWorkThreads = 2
throttle = 0.0
cpu_usage = -1.0
ETA_finish = -1.0 #approximate finish time

#Helper function that pipelines all TCP sends for consistency
def sendData(data):
        with lock:
                sendSocket.sendall(data)

#Transfers incomplete jobs to other node
                                                                               12,1          Top
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

#Parameters that are set by user
numWorkThreads = 2
throttle = 0.0
cpu_usage = -1.0
ETA_finish = -1.0 #approximate finish time

#Helper function that pipelines all TCP sends for consistency
def sendData(data):
        with lock:
                sendSocket.sendall(data)

#Transfers incomplete jobs to other node
def transfer_Manager(maxIdx):
        global A
                                                                                    12,1          Top
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

#Parameters that are set by user
numWorkThreads = 2
throttle = 0.0
cpu_usage = -1.0
ETA_finish = -1.0 #approximate finish time

#Helper function that pipelines all TCP sends for consistency
def sendData(data):
        with lock:
                sendSocket.sendall(data)

#Transfers incomplete jobs to other node
def transfer_Manager(maxIdx):
        global A
        soFar = 0
                                                                                     12,1          Top
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

#Parameters that are set by user
numWorkThreads = 2
throttle = 0.0
cpu_usage = -1.0
ETA_finish = -1.0 #approximate finish time

#Helper function that pipelines all TCP sends for consistency
def sendData(data):
        with lock:
                sendSocket.sendall(data)

#Transfers incomplete jobs to other node
def transfer_Manager(maxIdx):
        global A
        soFar = 0
                                                                                       12,1          Top
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

#Parameters that are set by user
numWorkThreads = 2
throttle = 0.0
cpu_usage = -1.0
ETA_finish = -1.0 #approximate finish time

#Helper function that pipelines all TCP sends for consistency
def sendData(data):
        with lock:
                sendSocket.sendall(data)

#Transfers incomplete jobs to other node
def transfer_Manager(maxIdx):
        global A
        soFar = 0
        while 1:
                                                                                       12,1          Top
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

#Parameters that are set by user
numWorkThreads = 2
throttle = 0.0
cpu_usage = -1.0
ETA_finish = -1.0 #approximate finish time

#Helper function that pipelines all TCP sends for consistency
def sendData(data):
        with lock:
                sendSocket.sendall(data)

#Transfers incomplete jobs to other node
def transfer_Manager(maxIdx):
        global A
        soFar = 0
        while 1:
                                                                                        12,1          Top
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

#Parameters that are set by user
numWorkThreads = 2
throttle = 0.0
cpu_usage = -1.0
ETA_finish = -1.0 #approximate finish time

#Helper function that pipelines all TCP sends for consistency
def sendData(data):
        with lock:
                sendSocket.sendall(data)

#Transfers incomplete jobs to other node
def transfer_Manager(maxIdx):
        global A
        soFar = 0
        while 1:
                                                                                         12,1          Top
#Group 6 - Chen Wu | Bowen Fan, Jingwen Jiang, Yijun Zhang
#MP4
import sys
import collections
import queue
import threading
import pickle
import struct
import socket
import time
import psutil
import zlib
from timeit import default_timer as timer

psutil.cpu_percent(interval=0)
Job = collections.namedtuple('Job','ID min max')
TCP_PORT = 2002
A = []
jobQueue = queue.Queue()
lock = threading.Lock()
elems = 1024*1024*32
jobIndexInterval = 1000
bootstrap_Done = False
job_delay = 0.1
finish_up = False

#Parameters that are set by user
numWorkThreads = 2
throttle = 0.0
cpu_usage = -1.0
ETA_finish = -1.0 #approximate finish time

#Helper function that pipelines all TCP sends for consistency
def sendData(data):
        with lock:
                sendSocket.sendall(data)

#Transfers incomplete jobs to other node
def transfer_Manager(maxIdx):
        global A
        soFar = 0
        while 1:
                                                                                          12,1          Top

if sys.argv[1] == 'local': #SMALLER MACHINE
        TCP_IP = '172.22.156.69'
else:
        TCP_IP = '172.22.156.9'

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serverSocket.bind((TCP_IP, TCP_PORT))
serverSocket.listen(1)

sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
if sys.argv[1] == 'local': #SMALLER MACHINE
        A = [1.111111]*elems
        setupJobs(jobIndexInterval)
        while 1:
                result = sendSocket.connect_ex(('172.22.156.9', TCP_PORT))
                if result == 0:
                        print('connected to big')
                        break
else:
        A = [None]*elems
        while 1:
                result = sendSocket.connect_ex(('172.22.156.69', TCP_PORT))
                if result == 0:
                        print('connected to small')
                        break

conn, addr = serverSocket.accept()
print ('Connection address:', addr)
spawn_thread(1,bootstrap,dict(unused=None)) #Bootstrap Setup
spawn_thread(1,listener,dict(unused=None)) #Listener Thread
spawn_thread(1,hardware_Monitor,dict(unused=None))
spawn_thread(1,throttle_Monitor,dict(unused=None))
state_Manager()

while 1:
        if bootstrap_Done: #Once boostrap is finished, start working
                spawn_thread(numWorkThreads,work,dict(unused=None))
                break
while 1:
        pass #Keep on going so that the children threads keep running
conn.close()
                                                                                           297,1         Bot