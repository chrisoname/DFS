###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	data node server for the DFS
#

from Packet import *

import sys
import socket
import SocketServer
import uuid
import os.path

def usage():
	print """Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0] 
	sys.exit(0)


def register(meta_ip, meta_port, data_ip, data_port):
	"""Creates a connection with the metadata server and
	   register as data node
	"""

	# Establish connection
	
	# Fill code	
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)	
	sock.connect((meta_ip, meta_port))

	try:
		response = "NAK"
		sp = Packet()
		while response == "NAK":
			sp.BuildRegPacket(data_ip, data_port)
			sock.sendall(sp.getEncodedPacket())
			response = sock.recv(1024)

			if response == "DUP":
				raise "Duplicate Registration"

		 	if response == "NAK":
				raise "Registratation ERROR"

	finally:
		sock.close()
	

class DataNodeTCPHandler(SocketServer.BaseRequestHandler):

	def handle_put(self, p):

		"""Receives a block of data from a copy client, and 
		   saves it with an unique ID.  The ID is sent back to the
		   copy client.
		"""

		fname, fsize = p.getFileInfo()

		self.request.send("OK")

		# Generates an unique block id.
		blockid = str(uuid.uuid1())

		try:
			print "datanode 69, filename: ", fname
			newFile = open(DATA_PATH + blockid, 'w')
			print "datanode71"
		# Open the file for the new data block.
			print "waiting for chunk"
			chunk = self.request.recv(1024)
			print "got chunk"  
			newFile.write(chunk)
			newFile.close()
			self.request.sendall(blockid)

		except Exception as e:
			print "Error datanode 77"
		#	print "I/O error({0}): {1}".format(e.errno, e.strerror)
		# Receive the data block.
	#	finally:
	#		newFile.close()

	
		# Send the block id back

		# Fill code

	def handle_get(self, p):
		
		# Get the block id from the packet
		blockid = p.getBlockID()
		rfile = open(DATA_PATH + blockid, 'r')
		self.request.sendall(rfile.read())

		# Read the file with the block id data
		# Send it back to the copy client.
		
		# Fill code

	def handle(self):
		msg = self.request.recv(1024)
		print msg, type(msg)

		p = Packet()
		p.DecodePacket(msg)

		cmd = p.getCommand()
		if cmd == "put":
			self.handle_put(p)

		elif cmd == "get":
			self.handle_get(p)
		

if __name__ == "__main__":

	META_PORT = 8000
	if len(sys.argv) < 4:
		usage()

	try:

		HOST = sys.argv[1]
		print "host done"
		PORT = int(sys.argv[2])
		print "port done"
		DATA_PATH = sys.argv[3]
		print "path done, ", DATA_PATH
		if len(sys.argv) > 4:
			META_PORT = int(sys.argv[4])
		print "first if done"
		if not os.path.isdir(DATA_PATH):
			print "Error: Data path %s is not a directory." % DATA_PATH
			usage()
		print "second if done"
		if DATA_PATH[len(DATA_PATH) - 1] != '/':
			DATA_PATH += '/'
	except:
		print "except"
		usage()


	register("localhost", META_PORT, HOST, PORT)
	print "registered node"
	server = SocketServer.TCPServer((HOST, PORT), DataNodeTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
 	server.serve_forever()
