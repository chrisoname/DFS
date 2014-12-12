###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz, Edwin Ramos y Christopher De Jesus
#
# Description:
# 	List client for the DFS
#


import sys
import socket

from Packet import *

def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % sys.argv[0] 
	sys.exit(0)

def client(ip, port):
	p = Packet()
	try:
		#Establish connection with the metadata server
		media = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
		media.connect((ip, port)) 
		#Create the packet list and send it
		p.BuildListPacket() 
		request = p.getEncodedPacket() 
		media.sendall(request)
		#Receive a response from it
		response = media.recv(1024) 
		
		if response == "NAK":
			raise "Problem in metadata server, can't complete operation"
			return
		
		p.DecodePacket(response) 
		
		ls = p.getFileArray()
		print ls

	except:
		raise 

	# Contacts the metadata server and ask for list of files.
if __name__ == "__main__":

	if len(sys.argv) < 2:
		usage()

	ip = None
	port = None 
	server = sys.argv[1].split(":")
	if len(server) == 1:
		ip = server[0]
		port = 8000
	elif len(server == 2):
		ip = server[0]
		port = int(server[1])

	if not ip:
		usage()

	client(ip, port)
