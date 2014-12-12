###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz, Edwin Ramos and Christopher De Jesus
# Description:
# 	Copy client for the DFS
#
#

import socket
import sys
import os.path
from math import ceil

from Packet import *

def usage():
	print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo DFS: python %s <file to be sent> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
	sys.exit(0)

#paths include file name
def copyToDFS(address, serverPath, localPath):
	""" Contact the metadata server to ask to copy file fname,
	    get a list of data nodes. Open the file in path to read,
	    divide in blocks and send to the data nodes. 
	"""

	#Establish a connection to the metadata server
	media = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		media.connect((address[0], address[1]))
	except:
		print "Can't connect to metadata"
		sys.exit(0)


	p = Packet()
	
	# Read (open) file
	try:
		ofile = open(localPath, 'r')
	except:
		raise "FILE DOES NOT EXIST"
	


	ifile = ofile.read()
	
	# Getting size of file
	fsize = len(ifile)
	
	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 
	p.BuildPutPacket(serverPath, fsize)
	request = p.getEncodedPacket()
	media.sendall(request)


	modifier = 0
	#receives response and acts on the response which depends
	response = media.recv(1024)
	


	#Optional implementation when there is a duplicate file
	while(response == "DUP"):
		modifier += 1
		modserverPath = serverPath + "(" + str(modifier) + ")"
		p.BuildPutPacket(modserverPath, fsize)
		request = p.getEncodedPacket()
		media.sendall(request)
		response = media.recv(1024)
	#The implementation goes as follow:

		#When there's a duplicate file it will append a (#), where "#"
		#is how many copies it has created everytime he made DUP
		#if the response is DUP and there are exactly 2 copies of it inside
		#it will put "filename.example(1)(2)"

	p.DecodePacket(response)
	nodes = p.getDataNodes()
	
	#Algorithm for the division of the file and sending them to each node
	divsize = int(ceil(float(fsize) / len(nodes)))
	start = 0
	end = divsize
	#IDs format: [(<nodeID>, <chunkID>)]
	#List declaration which will get tuples
	IDs = []
	for i in range(len(nodes)):
		#Determine chunk's start and end points in file, send put request
		chunk = ifile[start:end]
		start = end
		end   = (i+2) * divsize
		p.BuildPutPacket(serverPath, divsize)
		nodePutRequest = p.getEncodedPacket()
	
		try:
			#Establish connection to send the chunk to the nodes
			nodeSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			nodeSocket.connect((nodes[i][0], nodes[i][1]))
			nodeSocket.sendall(nodePutRequest)
			response = nodeSocket.recv(1024)
			if response == "OK":
				print "Sending chunk size then chunk"
				nodeSocket.sendall(str(len(chunk)))
			#	print str(len(chunk))
				while response != "SEND_TO_ME":
					print "Waiting for SEND_TO_ME"
					response = nodeSocket.recv(1024)

				print "Sending chunk"
				try:
					nodeSocket.sendall(chunk)
				except Exception as e:
					print "Problem sending chunk: %s" %e
				print "Waiting for CID"
				chunkID = nodeSocket.recv(1024)
				IDs.append([nodes[i][0], nodes[i][1], chunkID])
				print "done"
			else:
				i += -1

		except Exception, e:
			print "error copy.py 79", "\tIteration ", i
		
		nodeSocket.close()
	print "IDs", IDs
	p.BuildDataBlockPacket(serverPath, IDs)
	

	try:
		#Establish a new connections to connect to the metadata server
		# Notify the metadata server where the blocks are saved.
		media = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		media.connect((address[0], address[1]))
	except:
		print "Can't connect to metadata for dblks command"
		media.close()
		ofile.close()
		sys.exit(0)

	media.sendall(p.getEncodedPacket())

	
	ofile.close()
	media.close()

	
def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""

   	# Contact the metadata server to ask for information of fname
	media = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		media.connect((address[0], address[1]))

	except:
		print "Metadata server down or problem with client-side network"
		sys.exit(0)

	p = Packet()
	p.BuildGetPacket(fname)
	request = p.getEncodedPacket()
	# If there is no error response Retreive the data blocks
	media.sendall(request)
	response = media.recv(1024)
	media.close()

	if response == "NFOUND":
		
		print "File does not exist."
		sys.exit(0)
	p.DecodePacket(response)
	nodes = p.getDataNodes()
	#Open the file
	try:
		wfile = open(path, 'w')
	except:
		print "Could not open file in write mode"
		sys.exit(0)
	data = ''

	for node in nodes:
			#iterate the nodes to get the data and then save it into a path
		try:
			nodeSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			nodeSocket.connect((node[0], node[1]))
			p.BuildGetDataBlockPacket(node[2])
			nodeSocket.sendall(p.getEncodedPacket())

			chunkSize = int(nodeSocket.recv(1024))
#			print "chunkSize: ", chunkSize
			nodeSocket.sendall("OK")
#			print "Sent OK"

			chunk = ''
#			print "Waiting for chunk"

			while 1:
				#receive data until chunk is complete
				if len(chunk) == chunkSize:
					break
				chunk += nodeSocket.recv(1024)
			#concat chunk to rest of data
			data += chunk
#			print "CHUNK RECEIVED, LENGTH = ", len(chunk)
			nodeSocket.close()
		
		except Exception as e:
			print "Problem with data nodes: %s" % e

	wfile.write(data)

	media.close()



if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")

	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print "Error: path %s is a directory.  Please name the file." % to_path
			usage()

		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]
		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print "Error: path %s is a directory.  Please name the file." % from_path
			usage()

		copyToDFS((ip, port), to_path, from_path)


