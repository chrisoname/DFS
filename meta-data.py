###############################################################################
#
# Filename: meta-data.py
# Author: Jose R. Ortiz, Edwin Ramos and Christopher De Jesus
#
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#
# Please modify globals with appropiate info.

from mds_db import *
from Packet import *
import sys
import SocketServer

def usage():
	print """Usage: python %s <port, default=8000>""" % sys.argv[0] 
	sys.exit(0)


class MetadataTCPHandler(SocketServer.BaseRequestHandler):

	def handle_reg(self, db, p):
		#Register a new client to the DFS ACK if successfully REGISTERED
			#NAK if problem, DUP if the IP and port already registered
		try:
			if db.AddDataNode(p.getAddr(), p.getPort()) != 0:     
				self.request.sendall("ACK") 
				print "registered"
			else:
				self.request.sendall("DUP")
		except:
			self.request.sendall("NAK")

	def handle_list(self, db, p):
		#Get the file list from the database and send list to client
		try:
			files = db.GetFiles()
			
			p.BuildListResponse(files)
			response = p.getEncodedPacket()
			self.request.sendall(response)
			
		except:
			self.request.sendall("NAK")	

	def handle_put(self, db, p):
		#Insert new file into the database and send data nodes to save
		#the file.
		while 1:	
			info = p.getFileInfo()
			if db.InsertFile(info[0], info[1]):
				nodes = db.GetDataNodes()
				p.BuildPutResponse(nodes)
				response = p.getEncodedPacket()
				self.request.sendall(response)	
				break
			else:
				self.request.sendall("DUP")
				o = self.request.recv(1024)
				p.DecodePacket(o)
				
	def handle_get(self, db, p):
	
		#Get the file name from packet and then 
		# get the fsize and array of metadata server
		fname = p.getFileName()
		fsize, blockList = db.GetFileInode(fname)
		#Check if file is in database and return list of
		#nodes that contain the file.
		if fsize:
			p.BuildGetResponse(blockList, fsize)
			self.request.sendall(p.getEncodedPacket())
		else:
			self.request.sendall("NFOUND")

	def handle_blocks(self, db, p):
		# Add the data blocks to the file inode
		# Get file name and blocks from
		# packet
		# Adds blocks to file inode
		db.AddBlockToInode(p.getFileName(), p.getDataBlocks())
		

		
	def handle(self):

		# Establish a connection with the local database
		db = mds_db("dfs.db")
		db.Connect()

		# Define a packet object to decode packet messages
		p = Packet()

		# Receive a msg from the list, data-node, or copy clients
		#self.request es el socket
		msg = self.request.recv(1024)
		print msg, type(msg)
		
		# Decode the packet received
		p.DecodePacket(msg)
	

		# Extract the command part of the received packet
		cmd = p.getCommand()

		# Invoke the proper action 
		if   cmd == "reg":
			# Registration client
			self.handle_reg(db, p)

		elif cmd == "list":
			# Client asking for a list of files

			self.handle_list(db, p)

		elif cmd == "put":
			# Client asking for servers to put data

			self.handle_put(db, p)

		elif cmd == "get":
			# Client asking for servers to get data

			self.handle_get(db, p)

		elif cmd == "dblks":
			# Client sending data blocks for file

			self.handle_blocks(db, p)

		db.Close()

if __name__ == "__main__":
    HOST, PORT = "", 8000

    if len(sys.argv) > 1:
    	try:
    		PORT = int(sys.argv[1])
    	except:
    		usage()

    server = SocketServer.TCPServer((HOST, PORT), MetadataTCPHandler)

    
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
