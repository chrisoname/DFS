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
		raise 

	p = Packet()
	try:
		ifile = open(localPath, 'r')
	except:
		raise "FILE DOES NOT EXIST"
	# Read (open) file
	ifile = ifile.read()
	# Fill code
	fsize = len(ifile)
	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 
	p.BuildPutPacket(serverPath, fsize)
	request = p.getEncodedPacket()
	# Fill code
	media.sendall(request)
	response = media.recv(1024)

	if response == "DUP":
		raise "manejar esto bien es opcional"
		#return

	else:
		p.DecodePacket(response)
		nodes = p.getDataNodes()
		divsize = int(fsize / len(nodes))
		start = 0
		#IDs format: [(<nodeID>, <chunkID>)]
		IDs = []
		for i in range(nodes):
			block = ifile[start:i*divsize]
			start = i*divsize
			p.BuildPutPacket(serverPath, divsize)
			nodeRequest = p.getEncodedPacket()
			try:
				nodeSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				nodeSocket.connect((nodes[0], nodes[1]))
				nodeSocket.sendall(nodeRequest)
				chunkID = nodeSocket.recv(1024)
				IDs[i] = (i, chunkID)
			except Exception, e:
				raise e
			finally:
				nodeSocket.close()
		p.BuildDataBlockPacket(serverPath, IDs)
		dblksCommand = p.getEncodedPacket()
		media.sendall(dblksCommand)

	finally:
		ifile.close()
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
		raise

	# If there is no error response Retreive the data blocks

	# Fill code

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


