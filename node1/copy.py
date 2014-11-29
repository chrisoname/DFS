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
		ofile = open(localPath, 'r')
	except:
		raise "FILE DOES NOT 