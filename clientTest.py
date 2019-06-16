#######
# This client utility : use for test /check the ETLServer.py
# send messages to  server.
# usage:
# python clientTest.py  [n ] [close]
# where -
# n - is the number of required files to send to server (it is possible to send several request )
# close - send 'close' string to server (will cause server shutdown)
#
# Note - each request the client close the TCP connection so - new connection should be generates
#       (simulting new client request)
#
#Example :
#  python clientTest.py 15 30 67
# this would send 3 requests first with 15 files second with 30 files and third with 67 files


import socket
import message
import sys,os , random
#from time import sleep
from multiprocessing.connection import  Client

class clientDict:

    def __init__(self,host,port):
        self.address = (host,port,)


    def buildMessage(self,numberOfFiles):
        fList=os.listdir('.')
        msgObj = message.messageFactory({"%s.%d" % (fList[i % len(fList)], i): random.random() <= 0.11 for i in range(numberOfFiles)})
        return msgObj.asMeassgeStr()


    def send(self,num):
        cc = Client(self.address)
        if isinstance(num,str):
            cc.send(num)
        else :
            cc.send(self.buildMessage(num))



    def reconnect(self):
        print("Socket File No",self.server.fileno())
        if self.server.fileno() > 0 :
            print('there is connection')
            self.server.close()
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.connect(self.address)
        print("Socket File No second try:", self.server.fileno())


class clientTuple(clientDict):

    def buildMessage(self,numberOfFiles):
        fList = os.listdir('.')
        msgObj =message.messageFactory(
                [ (fList[i % len(fList)], random.random() <= 0.11 ,) for i in range(numberOfFiles) ]
                )
        return msgObj.asMeassgeStr()
        #return message.Message([fList[i % len(fList)] for i in range(numberOfFiles)])

if __name__ == '__main__':
    print("Runing Client ....",sys.argv)
    a = clientTuple('127.0.0.1',8081)
    b= clientDict('127.0.0.1',8081)
    default = sys.argv[1:] if len(sys.argv) > 1 else [5,17]
    messageClasses = [a,b]
    count = 0
    for param in default:
        try:
            sendParam = int(param)
        except ValueError as e:
            sendParam = param
            #print(type(e),e)
        #print("Number of files", sendParam)
        messageClasses[count % len(messageClasses)].send(sendParam)
        #c.send(sendParam)
        print("Retturn from send .....")
        count += 1
        #sleep(5)

        #c.reconnect()
