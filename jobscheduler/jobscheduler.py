from os import listdir
from os.path import isfile, join, isdir
from jsonSocket import JsonSocket
import json
import socket
import struct
import logging
import threading
import time

logger = logging.getLogger("jobscheduler")
logger.setLevel(logging.DEBUG)
FORMAT = '[%(asctime)-15s][%(levelname)s][%(funcName)s] %(message)s'
logging.basicConfig(format=FORMAT)

jobqueue = []

class JobServer(threading.Thread, JsonSocket):
	def __init__(self, conn, address, serverid, lock, port=8081):
		JsonSocket.__init__(self, address, port, conn)
		threading.Thread.__init__(self)
		self.severid = serverid
		self.socket = conn
		self.lock = lock
		print("serverid: %d" % self.severid)
	def run(self):
		global jobqueue
		try:
			msg = self.readObj()
			if "action" in msg and "jobid" in msg:
				if msg["action"]==0:
					file_option = False
					files = []
					mypath = msg["inputfolder"]
					number_of_nodes = msg["nodes"]
					if isdir(mypath):
						file_option = True
						files = [join(mypath, f) for f in listdir(mypath) if isfile(join(mypath, f))]
					else:
						files = list(range(number_of_nodes))
					print (files)
					state = ""
					if (len(files)>0):
						state = "active"
					else:
						state = "unactive"
					tq1 = list(range(len(files)))
					tq2 = []
					tq3 = []
					#print (msg["mapcode"])
					self.lock.acquire()
					jobqueue[int(msg["jobid"])]["job"]["id"] = int(msg["jobid"])
					jobqueue[int(msg["jobid"])]["job"]["mapcode"] = msg["mapcode"]
					jobqueue[int(msg["jobid"])]["job"]["reducecode"] = msg["reducecode"]
					jobqueue[int(msg["jobid"])]["job"]["state"] = state
					jobqueue[int(msg["jobid"])]["job"]["tq1"] = tq1
					jobqueue[int(msg["jobid"])]["job"]["tq2"] = tq2
					jobqueue[int(msg["jobid"])]["job"]["tq3"] = tq3
					jobqueue[int(msg["jobid"])]["job"]["files"] = files
					jobqueue[int(msg["jobid"])]["job"]["finalresult"] = {}
					jobqueue[int(msg["jobid"])]["job"]["fileoption"] = file_option
					jobqueue[int(msg["jobid"])]["job"]["starttime"] = 0
					self.lock.release()
					print(jobqueue[int(msg["jobid"])]["job"]["state"])
					self.sendObj({"result":0})
					return
				elif msg["action"]==1:
					self.lock.acquire()
					code = jobqueue[int(msg["jobid"])]["job"]["mapcode"]
					tq1 = jobqueue[int(msg["jobid"])]["job"]["tq1"]
					tq2 = jobqueue[int(msg["jobid"])]["job"]["tq2"]
					tq3 = jobqueue[int(msg["jobid"])]["job"]["tq3"]
					job = jobqueue[int(msg["jobid"])]
					files = jobqueue[int(msg["jobid"])]["job"]["files"]
					file_option = jobqueue[int(msg["jobid"])]["job"]["fileoption"]
					start_time = jobqueue[int(msg["jobid"])]["job"]["starttime"]
					if len(tq1)!=0 and job["job"]["state"]=="active":
						if start_time == 0:
							start_time = time.time()
							jobqueue[int(msg["jobid"])]["job"]["starttime"] = start_time
						index = tq1.pop()
						tq2.append(index)
						inputdata = ""
						if file_option:
							with open(files[index], 'r') as myfile:
								inputdata=myfile.read().replace('\n', '')
						job["input"] = inputdata
						job["taskid"] = index
						self.sendObj(job)
						self.lock.release()
						return
					if len(tq2)!=0 and job["job"]["state"]=="active":
						index = tq2[0]
						del tq2[0]
						tq2.append(index)
						inputdata = ""
						if file_option:
							with open(files[index], 'r') as myfile:
								inputdata=myfile.read().replace('\n', '')
						job["input"] = inputdata
						job["taskid"] = index
						self.sendObj(job)
						self.lock.release()
						print(tq2)
						return					
					else:
						self.lock.release()
						self.sendObj({"result":-1})
						return
				elif msg["action"]==2:
					taskid = msg["taskid"]
					taskoutput = msg["output"]
					self.lock.acquire()
					tq1 = jobqueue[int(msg["jobid"])]["job"]["tq1"]
					tq2 = jobqueue[int(msg["jobid"])]["job"]["tq2"]
					tq3 = jobqueue[int(msg["jobid"])]["job"]["tq3"]
					files = jobqueue[int(msg["jobid"])]["job"]["files"]
					if len(tq2)==0 and len(tq1)==0:
						jobqueue[int(msg["jobid"])]["job"]["state"] = "unactive"
						self.sendObj({"result":-1})
						self.lock.release()
						return
					if taskid in tq2:
						tq2.remove(taskid)
						tq3.append(taskid)
						jobqueue[int(msg["jobid"])]["job"]["tq2"] = tq2
						jobqueue[int(msg["jobid"])]["job"]["tq3"] = tq3
						reduce = jobqueue[int(msg["jobid"])]["job"]["reducecode"]
						final_result = jobqueue[int(msg["jobid"])]["job"]["finalresult"]
						print (" ")
						#print (reduce)
						#print (taskid)
						#print (tq1)
						#print (tq2)
						print (tq3)
						exec(reduce,globals())
						final_result = __reduce_function(final_result, taskoutput)
						#print (final_result)
						if not final_result:
							jobqueue[int(msg["jobid"])]["job"]["finalresult"] = final_result
						if len(tq3)==len(files):
							print ("The job has been finished.")							
							start_time = jobqueue[int(msg["jobid"])]["job"]["starttime"]							
							print ("--- %s seconds ---" % (time.time() - start_time))
							"""
							print (len(final_result))
							for key in final_result:
								print (str(key)+": "+str(final_result[key]))
							"""
					self.lock.release()
					
                                        
			else:
				self.sendObj({"result":-1})
				return
		except socket.timeout as e:
			logger.debug("server socket.timeout: %s" % e)

		except Exception as e:
			logger.error("server: %s" % e)			
		self.conn.close()
class JobServerHandler(JsonSocket):
	def __init__(self, address='127.0.0.1', port=8081):
		global jobqueue
		super(JobServerHandler, self).__init__(address, port)
		self._bind()
		self.servernum = 0
		self.lock = threading.Lock()
		job = json.loads('{"job":{"id":0,"mapcode":"","inputkey":0, "finalresult":{}}}')
		for i in range(20):
			jobqueue.append(job)
                
	
	def _bind(self):
		self.socket.bind( (self.address,self.port) )

	def _listen(self):
		self.socket.listen(1)
	
	def _accept(self):
		return self.socket.accept()
	
	def acceptConnection(self):
		self._listen()
		while 1:
				conn, addr = self._accept()
				self.conn.settimeout(self.timeout)
				server = JobServer(conn, addr, self.servernum, self.lock)
				server.start()                        
				logger.debug("connection accepted, conn socket (%s,%d)" % (addr[0],addr[1]))
				self.servernum = self.servernum+1
				print ("%d server assigned" %(self.servernum))

	
class JsonClient(JsonSocket):
	def __init__(self, address='127.0.0.1', port=5489):
		super(JsonClient, self).__init__(address, port)
		
	def connect(self):
		for i in range(20):
			try:
				self.socket.connect( (self.address, self.port) )
			except socket.error as msg:
				logger.error("SockThread Error: %s" % msg)
				time.sleep(3)
				continue
			logger.info("...Socket Connected")
			return True
		return False

class Add_Job_Client(threading.Thread, JsonSocket):
	def __init__(self, clientid, command, address='127.0.0.1', port=8081):
		JsonSocket.__init__(self, address, port)
		threading.Thread.__init__(self)
		self.clientid = clientid
		self.command = command
		print("clientid: %d" %clientid)
	def connect(self):
		for i in range(10):
			try:
				self.socket.connect((self.address, self.port))
			except socket.error as msg:
				logger.error("SockThread Error: %s" % msg)
				time.sleep(3)
				continue
			logger.info("...Socket Connected")
			return True
		return False
	def run(self):
		self.sendObj(self.command)
		msg = ""
		try:
			msg = self.readObj()
			logger.info("client received: %s" % msg)
		except socket.timeout as e:
			logger.debug("client socket.timeout: %s" % e)			
		except Exception as e:
			logger.error("client: %s" % e)			
		self.close()
		return msg
	
class Get_Job_Client(threading.Thread, JsonSocket):
	def __init__(self, clientid, command, address='127.0.0.1', port=8081):
		JsonSocket.__init__(self, address, port)
		threading.Thread.__init__(self)
		self.clientid = clientid
		self.command = command
		print("clientid: %d" %clientid)
	def connect(self):
		for i in range(10):
			try:
				self.socket.connect((self.address, self.port))
			except socket.error as msg:
				logger.error("SockThread Error: %s" % msg)
				time.sleep(3)
				continue
			logger.info("...Socket Connected")
			return True
		return False
	def run(self):
		self.sendObj(self.command)
		msg = ""
		try:
			msg = self.readObj()
			logger.info("client received: %s" % msg)
		except socket.timeout as e:
			logger.debug("client socket.timeout: %s" % e)			
		except Exception as e:
			logger.error("client: %s" % e)			
		self.close()
		return msg

if __name__ == "__main__":
        import jobscheduler
        a1 = jobscheduler.Add_Job_Client(0,json.loads('{"action":0,"jobid":0,"mapcode":"abc"}'))
        b1 = jobscheduler.Get_Job_Client(0,json.loads('{"action":1,"jobid":0,"mapcode":"abc"}'))
        s = jobscheduler.JobServerHandler()
        s.acceptConnection()
