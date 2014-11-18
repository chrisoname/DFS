from socket import * 
import sys
import ssl
import base64
msg = "\r\n I love computer networks!"
endmsg = "\r\n.\r\n"

# aspmx.l.google.com 
# vmail.uprrp.edu
# Choose a mail server (e.g. Google mail server) and call it mailserver 
mailserver = 'smtp.gmail.com' #Fill in start #Fill in end
emailFrom  = 'cs4205.networks@gmail.com'
emailTo	   = 'cs4205.networks@gmail.com'
# Create socket called clientSocket and establish a TCP connection with mailserver
#Fill in start
clientSocket = socket(AF_INET, SOCK_STREAM)
clientSocket = ssl.wrap_socket(clientSocket, ssl_version=ssl.PROTOCOL_SSLv23)
print "Connecting..."
clientSocket.connect((mailserver, 465)) #587 para google
print "Connected"
#Fill in end
recv = clientSocket.recv(1024)
print recv
if recv[:3] != '220':
    print '220 reply not received from server.'
    clientSocket.close()
    sys.exit()
# Send HELO command and print server response.
heloCommand = 'EHLO smtp.google.com\r\n'
clientSocket.send(heloCommand)
recv = clientSocket.recv(1024)
print "Response(HELO):", recv
if recv[:3] != '250':
    print '250 reply not received from server. (HELO) Exiting.'


#STARTTLS COMMAND
print "start"
tlsCommand = "STARTTLS\r\n"
clientSocket.send(tlsCommand)
print "sent"
recv = clientSocket.recv(1024)
print "Response(STARTTLS): ", recv
clientSocket.send('AUTH LOGIN\r\n')
clientSocket.send(base64.b64encode('cs4205.networks@gmail.com')+'\r\n')


recv = clientSocket.recv(1024)
print "Sent User, Response:", recv

clientSocket.send(base64.b64encode('ccom4205')+'\r\n')
recv = clientSocket.recv(1024)
print "Sent password, Response: ", recv
# Send MAIL FROM command and print server response.
# Fill in start
fromCommand = "MAIL FROM:<%s>\r\n"%(emailFrom)
print "Sending MAIL FROM..."
clientSocket.sendall(fromCommand)
print "Sent MAIL from..."

recv = clientSocket.recv(1024)
print "Response(FROM):", recv
if recv[:3] != '235':
	print "235 reply not received from server.(FROM) Exiting."
	clientSocket.close()
	sys.exit()
else:
	print "Authentication accepted"
# Fill in end


# Send RCPT TO command and print server response.
# Fill in start
rcptCommand = "RCPT TO:<%s>\r\n"%(emailTo)
clientSocket.sendall(rcptCommand)
recv = clientSocket.recv(1024)
print "Response(RCPT):", recv
if recv[:3] != '250':
	print "250 reply not received from server.(RCPT) Exiting."
	clientSocket.close()
	sys.exit()
# Fill in end

# Send DATA command and print server response.
# Fill in start
clientSocket.sendall("DATA\r\n")
recv = clientSocket.recv(1024)
print "Response(DATA):", recv
if recv[:3] != '250':
	print "250 reply not received from server.(DATA) Exiting."
	clientSocket.close()
	sys.exit()
# Fill in end

# Send message data.# Message ends with a single period.
# Fill in start
data = '''Hello Edwin test test\r\n.\r\n'''
clientSocket.sendall(data)
recv = clientSocket.recv(1024)
print "Response(DATA payload):", recv
if recv[:3] != '354':
	print "254 reply not received from server.(DATA payload) Exiting."
	clientSocket.close()
	sys.exit()
clientSocket.sendall("QUIT")
recv = clientSocket.recv(1024)
print "Response(QUIT):"
clientSocket.close()
# Fill in end


# Fill in start


# Fill in end

# Send QUIT command and get server response.
# Fill in start


# Fill in end
