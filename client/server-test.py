import socket
from threading import *


config = {}
execfile("client.conf", config) 

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = config["ip"]
port = config["port"]

file_path = config["file_path"]

print (file_path)

serversocket.bind((host, port))

class client(Thread):
    def __init__(self, socket, address):
        Thread.__init__(self)
        self.sock = socket
        self.addr = address
        self.start()

    def run(self):
        while 1:
            print('Client sent:', self.sock.recv(1024).decode())
            self.sock.send(b'Oi you sent something to me')

serversocket.listen(5)

print ('server started and listening')
while 1:
    clientsocket, address = serversocket.accept()
    client(clientsocket, address)


