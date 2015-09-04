#auther Bowen Fan, Chen Wu

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
    if jobQueue.qsize() == 0:
      return False
    if soFar >= maxIdx:
      return True
    item = jobQueue.get()
    send = A[item.min:item.max+1]
    soFar += len(send)
    my_bytes = struct.pack("iii%sd" % len(send),item.ID,item.min,item.max,*send)
    my_bytes = zlib.compress(my_bytes)
    my_bytes = struct.pack('>cI', b'j',len(my_bytes)) + my_bytes
    print("Transfering:","ID:",item.ID,len(send),item.max,item.min,len(my_bytes))
    sendData(my_bytes)
    jobQueue.task_done()

#Transfers complete jobs to remote node
def transfer_completed(item):
  global A
  send = A[item.min:item.max+1]
  my_bytes = struct.pack("iii%sd" % len(send),item.ID,item.min,item.max,*send)
  my_bytes = zlib.compress(my_bytes)
  my_bytes = struct.pack('>cI', b'd',len(my_bytes)) + my_bytes
  sendData(my_bytes)

#worker thread
def work(unused):
  global throttle,text_file,bootstrap_Done
  while 1:
    start_time = timer()
    item = jobQueue.get()
    do_work(item)
    jobQueue.task_done()
    end = timer()
    total = end-start_time
    time.sleep(total*throttle)
    if sys.argv[1] == 'local':
      transfer_completed(item)

#Heartbeats state information to the other node
def state_Manager():
  global ETA_finish, throttle, bootstrap_Done, job_delay
  threading.Timer(2,state_Manager).start()

  if not bootstrap_Done:
    return
  non_throttle_time = job_delay * jobQueue.qsize()
  ETA_finish = non_throttle_time + (non_throttle_time * throttle)
  print("HeartBeat:","Q:",jobQueue.qsize(),"ETA:",ETA_finish, "CPU:",cpu_usage)
  my_bytes = struct.pack("ddd",ETA_finish,throttle,cpu_usage)
  state = struct.pack('>cI',b's',len(my_bytes)) + my_bytes
  sendData(state)

#Collects CPU information
def hardware_Monitor(unused):
  global cpu_usage
  while 1:
    psutil.cpu_percent(interval=0)
    cpu_usage = psutil.cpu_percent(interval=0)
    time.sleep(5)

#Watches for a change in throttle by user input
def throttle_Monitor(unused):
  global throttle
  while 1:
    s = input('Throttle Value:')
    if not s.isdigit():
      continue
    throttle = 1.0-float(s)/100

#Sets up the jobs for each node initially
def bootstrap(unused):
  if not sys.argv[1] == 'local': return
  maxIdx = int((elems/2))
  if transfer_Manager(maxIdx):
    boot_done = struct.pack('>cI',b'b',0)
    sendData(boot_done)

#Helper function to create initial jobs
def setupJobs(next):
  global A
  total = len(A)-1
  cur = 0
  _id = 1;
  while cur < total:
    p = Job(ID=_id,min=cur,max=cur+next-1)
    jobQueue.put(p)
    cur += next
    if cur+next > total:
      next = total - cur +1
    _id += 1

#Creates N threads for given work function
def spawn_thread(n,worker,params):
  for i in range(n):
    t = threading.Thread(target=worker, kwargs=params)
    t.daemon = True  # thread dies when main thread (only non-daemon thread) exits.
    t.start()

#Adds a job to the JobQueue
def addJob(data,which):
  global A
  #print(len(data))
  #print("###:",(struct.unpack("iii%sd" % int(num), data))[0:3])
  decompressed = zlib.decompress(data)
  numData = (len(decompressed)-16) /8
  #print(len(decompressed), (len(decompressed)-16) /8)
  unpack_data = struct.unpack("iii%sd" % int(numData), decompressed)
  newID,newMin,newMax= unpack_data[0:3]
  newVals = unpack_data[3:]
  #print("Adding NEW JOB ID:%s, Min:%s, Max%s, length:%i" %(newID,newMin,newMax,len(newVals)))
  p = Job(ID=newID,min=newMin,max=newMax)
  updateValues(p,newVals)
  if (which == 'j'):
    jobQueue.put(p)
    print("Adding Job To Queue ID:%s, Min:%s, Max%s, length:%i" %(newID,newMin,newMax,len(newVals)))
  else:
    pass
    #print("Completed Transfer Finished JOB ID:%s, Min:%s, Max%s, length:%i" %(newID,newMin,newMax,len(newVals)))
    #print(A[newMin:newMin+10])
  #print(A[0],A[10000],A[16777215],A[16777215+1],A[16777215+2])

#Determines whether or not to transfer jobs to the other node
#Bandwidth x Delay is computed as RTT (STop and Wait Policy for TCP) 
#RTT = (size/10Mbps) + 54ms*2
def adaptor(data):
  global ETA_finish, throttle, job_delay, A, finish_up
  if finish_up == True:
    return
  unpack_data = struct.unpack("ddd", data)
  #print(unpack_data,ETA_finish)
  remote_ETA = unpack_data[0]
  remote_throttle = unpack_data[1]
  remote_cpu = unpack_data[2]
  diff_sec = (ETA_finish - remote_ETA)
  if diff_sec > 0:
    new_Jobs = int(diff_sec/(job_delay*(throttle+1)))
    remote_Jobs = int(diff_sec/(job_delay*(remote_throttle+1)))
    remote_bandwidth_ms = remote_Jobs*70*8 / 10000000 * 1000
    time_here = (new_Jobs*job_delay)+(new_Jobs*job_delay*throttle) * 1000
    time_there = (remote_Jobs*job_delay)+(remote_Jobs*job_delay*throttle) * 1000
    RTT = (54*2) + remote_bandwidth_ms
    print("Adaptor:","RTT:",RTT,"Time There:",time_there,"RemoteThrottle:",remote_throttle,remote_Jobs,new_Jobs)
    if (time_there > RTT):
      transfer_Manager(remote_Jobs)
  elif remote_ETA == 0 and ETA_finish == 0 and not sys.argv[1] == 'local':
    finish_up = True
    for i in range(len(A)):
      if not A[i] == 1112.2221109999998:
        print('warning:',A[i],i)
        finish_up = False
        break
    if finish_up == True:
      print('A Vector is correct. Now Face the TA.')
    else:
      print('no finish_up')

#Update A vector given a subvector 
def updateValues(job,newVals):
  global A
  j = 0
  for i in range(job.min,job.max+1):
    A[i] = newVals[j]
    j+= 1

#Helper function to recieve bytes
def recv_msg(sock):
  # Read message length and unpack it into an integer
  raw_msglen = recvall(sock, 5)
  if raw_msglen[0] == None or raw_msglen[1] == None:
    return None,None
  header = struct.unpack('>cI', raw_msglen)
  msglen = header[1]
  # Read the message data
  return recvall(sock, msglen),header[0]

#Wrapper of recv to help consistency with recv
def recvall(sock, n):
  # Helper function to recv n bytes or return None if EOF is hit
  data = b''
  while len(data) < n:
    packet = sock.recv(n - len(data))
    if not packet:
      return None,None
    data += packet
  return data

#Listens for incoming recv TCP
def listener(unused):
  global bootstrap_Done
  while 1:
    data, msg_type = recv_msg(conn)
    if msg_type == b'j' and data:
      spawn_thread(1,addJob,dict(data=data,which='j'))
    elif msg_type == b'b':
      print("bootstrap done")
      if bootstrap_Done == False:
        boot_done = struct.pack('>cI',b'b',0)
        sendData(boot_done)
      bootstrap_Done = True
    elif msg_type == b's':
      spawn_thread(1,adaptor,dict(data=data))
    elif msg_type == b'd' and data:
      spawn_thread(1,addJob,dict(data=data,which='d'))

#Does work to the A vector
def do_work(p):
  global A
  for i in range(p.min,p.max+1):
    for j in range(10):
      A[i] += 111.1111

if len(sys.argv) != 2:
  print("ERROR: local or remote?")
  exit()

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
