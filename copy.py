###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
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
	""" Contact the metadata server to ask to copu file fname,
	    get a list of data nodes. Open the file in path to read,
	    divide in blocks and send to the data nodes. 
	"""

	# Create a connection to the data server

	# Fill code
	media = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		media.connect((address[0], address[1]))
	except:
		print "Can't connect to metadata"
		sys.exit(0)

	p = Packet()
	try:
		ofile = open(localPath, 'r')
	except:
		raise "FILE DOES NOT EXIST"
	# Read (open) file
	ifile = ofile.read()
	# Fill code
	fsize = len(ifile)
	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 
	p.BuildPutPacket(serverPath, fsize)
	request = p.getEncodedPacket()
	media.sendall(request)


	modifier = 0
	response = media.recv(1024)
	print "\n response:", response, "\n"

	while(response == "DUP"):
		modifier += 1
		serverPath = serverPath + "(" + str(modifier) + ")"
		p.BuildPutPacket(serverPath, fsize)
		request = p.getEncodedPacket()
		media.sendall(request)
		response = media.recv(1024)
		#raise "manejar esto bien es opcional"
		#return
	
	#	print "\n", response, "\n"
	p.DecodePacket(response)
	nodes = p.getDataNodes()
	#divsize = int(ceil(fsize / float(len(nodes))))
	divsize = int(ceil(float(fsize) / len(nodes)))
	#divsize = ceil(divsize)
	print "\n\n DIVSIZE ", divsize, "\n\n"
	start = 0
	end = divsize
	#IDs format: [(<nodeID>, <chunkID>)]
	IDs = []
	for i in range(len(nodes)):

		chunk = ifile[start:end]
	#	print "\n\nstart:end", start,':', end#, "\n", chunk, "\n\n\n"
		start = end
		end   = (i+2) * divsize
		p.BuildPutPacket(serverPath, divsize)
		nodePutRequest = p.getEncodedPacket()
	
		try:
			
			nodeSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			nodeSocket.connect((nodes[i][0], nodes[i][1]))
			nodeSocket.sendall(nodePutRequest)
			response = nodeSocket.recv(1024)
			if response == "OK":
				nodeSocket.sendall(chunk)
				chunkID = nodeSocket.recv(1024)

				IDs.append([nodes[i][0], nodes[i][1], chunkID])
				print "done"
			else:
				print "Estoy en el else"
				i += -1

		except Exception, e:
			print "error copy.py 79", "\tIteration ", i
		
		nodeSocket.close()
	print "IDs", IDs
	p.BuildDataBlockPacket(serverPath, IDs)
	
	

	#No me gusta hacerlo asi, pero parece que asi lo quiere el prof
	#would rather call handle_dblks right here
	try:
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
#		p.BuildDataBlockPacket(serverPath, blockList)
#		request = p.getEncodedPacket()
	#	media.sendall(request)

	# If no error or file exists
	# Get the list of data nodes.
	# Divide the file in blocks
	# Send the blocks to the data servers

	# Fill code	

	# Notify the metadata server where the blocks are saved.

	# Fill code
	
def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""

   	# Contact the metadata server to ask for information of fname

	# Fill code
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
	print "NODES \n\n\n", nodes, "\n\n\nEND NODES" 
	try:
		wfile = open(path, 'w')
	except:
		print "Could not open file in write mode"
		sys.exit(0)
	write = ''
#	print "NODES = ", nodes
	for node in nodes:

		try:
	#		print "attempt connection"
		#	print "Node[0] - Node[1]", node[0], "\t", node[1]
			nodeSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			nodeSocket.connect((node[0], node[1]))
	#		print "connected"
			p.BuildGetDataBlockPacket(node[2])
			#print 'AQUIIIIII\n\nnode[2]: ', node[2], '\n\n'
			nodeSocket.sendall(p.getEncodedPacket())
			chunk = nodeSocket.recv(1024)
			write += chunk
			nodeSocket.close()
	#		print "\nCHUUUUUUUUUUUUUNK\n\n", chunk, "end chunk\n\n"
		except:
			print "Data Node socket had a problem"
	wfile.write(write)
###########################################################################



    # Save the file
	
	# Fill code


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


