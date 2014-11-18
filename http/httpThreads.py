import threading
from socket import *
import sys

def individualConnection(clientSocket, clientAddress):
	message = clientSocket.recv(1024)
#	print "Message: ", message 

	message = message.split()
	

	#message[0] is the kind of request that was received, this server can only take GET
	if message[0] != "GET":
		print "Can only handle GET requests, received something else"
		clientSocket.close()
		return

	try:

		# message[1] is name of requested file, message[1][0] is '/', the directory, so it is ignored 
	#	print "Filename: ", message[1]
		if message[1][1:] == '':
			print "no name"
			with open('index.html', 'r') as reqFile:
				html = reqFile.read()
		
		else:
			print "name = ", message[1][1:]	
		#	print "name length =". str(len(message[1][1:]))
			with open(message[1][1:], 'r') as reqFile:
				html = reqFile.read()
		print "File: ", reqFile

		response = "HTTP/1.1 200 OK\nServer: Python/6.6.6\nContent-Type: text/html\n\n" + html
		print "RESPONSE; ", response
		clientSocket.send(response) 


	except IOError:
	#	print "File not found, sending 404"
		print "404 not found"
		clientSocket.send("HTTP/1.1 404 NOT_FOUND")

	clientSocket.close()

def dispatcher(serverSocket):
	while 1:
		try:
 			clientSocket, clientAddress =  serverSocket.accept()
 			t = threading.Thread(target=individualConnection, args=(clientSocket, clientAddress))
 			t.start()
 		except:
 			print "Socket is closed. Dispatcher shutting down"
 			break

 	serverSocket.close()

#set up socket and start listening
PORT = 1111
IP = ''


serverSocket = socket(AF_INET, SOCK_STREAM)
serverSocket.bind((IP, PORT))
serverSocket.listen(1)

print "Dispatcher begin"
while 1:
	try:
		clientSocket, clientAddress =  serverSocket.accept()
		t = threading.Thread(target=individualConnection, args=(clientSocket, clientAddress))
		t.start()
	except:
		print "Dispatcher shutting down, INTERRUPT signal received"
		break


serverSocket.close()
