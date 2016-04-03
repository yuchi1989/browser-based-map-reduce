__version__	 = "4.0.0"

import json
import socket
import struct
import logging
import threading
import time

logger = logging.getLogger("jsonSocket")
logger.setLevel(logging.DEBUG)
FORMAT = '[%(asctime)-15s][%(levelname)s][%(funcName)s] %(message)s'
logging.basicConfig(format=FORMAT)

class JsonSocket(object):
	def __init__(self, address='127.0.0.1', port=5489, conn=None):
		
		if conn==None:
				self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				self.conn = self.socket
		else:
				self.conn = conn
		self._timeout = None
		self._address = address
		self._port = port

	
	def sendObj(self, obj):
		msg = json.dumps(obj)
		if self.socket:
			frmt = "=%ds" % len(msg)
			msg = bytes(msg, encoding = "utf8")
			packedMsg = struct.pack(frmt, msg)
			packedHdr = struct.pack('=I', len(packedMsg))
			
			self._send(packedHdr)
			self._send(packedMsg)
			
	def _send(self, msg):
		sent = 0
		while sent < len(msg):
			sent += self.conn.send(msg[sent:])
			
	def _read(self, size):
		data = bytearray()
		while len(data) < size:
			dataTmp = self.conn.recv(size-len(data))
			data += dataTmp
			if len(dataTmp) == 0:
				raise RuntimeError("socket connection broken")
		return data

	def _msgLength(self):
		d = self._read(4)
		s = struct.unpack('=I', d)
		return s[0]
	
	def readObj(self):
		size = self._msgLength()
		data = self._read(size)
		frmt = "=%ds" % size
		msg = struct.unpack(frmt,data)
		msg = str(msg[0], encoding = "utf-8")
		msg = json.loads(msg)
		return msg
	
	def close(self):
		logger.debug("closing main socket")
		self._closeSocket()
		if self.socket is not self.conn:
			logger.debug("closing connection socket")
			self._closeConnection()
			
	def _closeSocket(self):
		self.socket.close()
		
	def _closeConnection(self):
		self.conn.close()
	
	def _get_timeout(self):
		return self._timeout
	
	def _set_timeout(self, timeout):
		self._timeout = timeout
		self.socket.settimeout(timeout)
		
	def _get_address(self):
		return self._address
	
	def _set_address(self, address):
		pass
	
	def _get_port(self):
		return self._port
	
	def _set_port(self, port):
		pass
			
	timeout = property(_get_timeout, _set_timeout,doc='Get/set the socket timeout')
	address = property(_get_address, _set_address,doc='read only property socket address')
	port = property(_get_port, _set_port,doc='read only property socket port')

class JsonEchoServer(threading.Thread, JsonSocket):
	def __init__(self, conn, address, serverid, port=5489):
		JsonSocket.__init__(self, address, port, conn)
		threading.Thread.__init__(self)
		self.severid = serverid
		self.socket = conn
		print("serverid: %d" % self.severid)
	def run(self):
		try:
			msg = self.readObj()
			logger.info("server received: %s" % msg)
			time.sleep(10)
			self.sendObj(msg)
		except socket.timeout as e:
			logger.debug("server socket.timeout: %s" % e)

		except Exception as e:
			logger.error("server: %s" % e)			
		self.conn.close()
class JsonServerHandler(JsonSocket):
	def __init__(self, address='127.0.0.1', port=5489):
		super(JsonServerHandler, self).__init__(address, port)
		self._bind()
		self.servernum = 0
	
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
				server = JsonEchoServer(conn, addr, self.servernum)
				server.start()                        
				logger.debug("connection accepted, conn socket (%s,%d)" % (addr[0],addr[1]))
				self.servernum = self.servernum+1
				print ("%d server assigned" %(self.servernum))
	def close(self):
		logger.debug("serverhandler closing main socket")
		self._closeSocket()
		if self.socket is not self.conn:
			logger.debug("closing connection socket")
			self._closeConnection()

class JsonEchoClient(threading.Thread, JsonSocket):
	def __init__(self, clientid, echo, address='127.0.0.1', port=5489):
		JsonSocket.__init__(self, address, port)
		threading.Thread.__init__(self)
		self.clientid = clientid
		self.echo = echo
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
		self.sendObj(self.echo)
		try:
			msg = self.readObj()
			logger.info("client received: %s" % msg)
		except socket.timeout as e:
			logger.debug("client socket.timeout: %s" % e)			
		except Exception as e:
			logger.error("client: %s" % e)			
		self.close()

if __name__ == "__main__":
	""" basic json echo server """
	import threading, time
	
	def serverThread():
		serverHandler = JsonServerHandler()
		serverHandler.acceptConnection()
		time.sleep(10)
		serverHandler.close()
			
	t = threading.Timer(1,serverThread)
	t.start()
	
	time.sleep(2)
	
	
	client0 = JsonEchoClient(0,{"id":0,"action":"addjob"})
	client0.connect()
	client0.start()
	client1 = JsonEchoClient(0,{"id":1,"action":"addjob"})
	client1.connect()
	client1.start()
	time.sleep(20)
	client0.close()
	client1.close()
