# Allot Task - Server Implementation.
# Usage:
# python ETLServer.py [port <n>] [host <str>] [timeOut <n>] [requests <n>] [beat <n>] [db <str>]
# where -
#   port - is the listening port of this server  <n> should be legal port number.
#   host - is the ip/hostname that this server should listen (it is relevant if there are several Eth or IPs)
#   timeOut - the maximum time allowed to single request. After this period the request would force terminate
#   beat - the period in seconds a keep alive request should be send.
#   db  - defined the type of Database. means how to connect ,hw to update the records etc ...
#         the syntax of this parameter depend on the database type - see more detais in dbHandler.py
#
# Note - if the server gets the string "close"  (from client)  it will shutdown the server



from multiprocessing.connection import Listener
from threading import  Timer , enumerate as threadList
import multiprocessing as mp
from ctypes import c_bool
import sys , os
import time

import message as msg
import dbHandler as DB


class ETLServer :

    def __init__(self,**config):
        self.port = int(config.get('port',8081))
        self.host = config.get('host','127.0.0.1')
        self.maxRequests = int(config.get('requests',4))
        self.timeOut = int(config.get('timeOut',40))
        self.beat = int(config.get('beat',60))
        dbDefault = { 'dbType' : 'Dummy' ,
                      'host' : '1.2.3.4' ,
                      'port' : 333 ,
                      'user' : 'anonymus' ,
                      'password' : '1234'}
        tmp = msg.Message(dbDefault)
        self.dbConnct = config.get('db',tmp.asMeassgeStr())
        self.runFlag = mp.Value(c_bool,False)


    def handleRequest(self,message):
        for fName in message.fileList():
            if not message.isFileCorrupted(fName):
                self.db.appendFile(fName)
                ## Note - insert here sleep to simulate Load in file handling
                #time.sleep(0.5)
            else :
                self.handleCorruptedFile(fName)
        self.wrLog("%s Finisihed succesfully" % mp.current_process().name )

    def handleCorruptedFile(self,fileName):
        self.wrLog("Error - file '%s' is corrupted" % fileName)

    def heartbeat(self):
        self.wrLog("Deabug - Heartbeat expired")
        self.keepAlive.keepalive()
        nextBeat = Timer(self.beat,self.heartbeat)
        nextBeat.setName("KeepAlive")
        nextBeat.start()

    def dbConnect(self):
        tmp = msg.Message(self.dbConnct)
        dbRec = tmp.asJson()
        db = DB.FactoryDB(dbRec,self.wrLog)
        self.db = db
        self.keepAlive = db
        self.db.connect()
        self.keepAlive.connect()

    def wrLog(self,*lines):
        tStr = time.ctime()
        first = True
        for line in lines:
            print('%-25s' % (tStr if first else '') ,line)
            first = False

    def newConnection(self,soc):
        self.wrLog('process "%s"' % mp.current_process().name)
        Msg = soc.recv()
        self.wrLog("Data:", Msg)
        if isinstance(Msg,str) and Msg == 'close' :
            self.wrLog("Close request")
            self.runFlag.value =False
            soc.close()
            self.listener.close()
        else :
            data = msg.messageFactory(Msg)
            self.handleRequest(data)

    def handleTimeOut(self,task):
        if task.is_alive() :
            self.wrLog(task.name + " Timed Out kill the process !!!!!!!!!!!!")
            task.terminate()
    def clearTimers(self,*timersName):
        for t in threadList():
            if t.__class__.__name__  == "Timer" :
                if (len(timersName) and t.name in timersName) or len(timersName) == 0:
                    self.wrLog("Terminat timer :     " + t.name , t)
                    t.cancel()

    def run(self):
        self.wrLog("connecting to database ...")
        self.dbConnect()
        self.heartbeat()
        soc = Listener(address=(self.host,self.port,))
        self.listener = soc
        self.wrLog("Server start to listing:", soc.address)
        self.runFlag.value = True
        tmp = mp.current_process()
        self.wrLog("current process",tmp.name)
        for t in threadList():
            name = t.name
            stat = type(t)
            self.wrLog(name + " > " + str(stat))
        while self.runFlag.value:
            while len(mp.active_children()) >= self.maxRequests :
                self.wrLog("Number of requests %d exceeded the avilable resources" % self.maxRequests )
                self.wrLog("running process", [pn.name for pn in mp.active_children()])
                time.sleep(1)
            conn = soc.accept()
            if not self.runFlag.value:
                self.wrLog("closing server ignore new request")
                continue
            add = soc.last_accepted
            self.wrLog("Connection accepted",add)
            tName = "ClientRequest-%s:%d" % (add[0], add[1])

            tmp = mp.Process(target=self.newConnection,args=(conn,),name=tName)
            tmp.start()
            ptimer = Timer(self.timeOut,self.handleTimeOut,args=(tmp,))
            ptimer.start()

        self.clearTimers("KeepAlive")

        #Gracefull shutdown
        while len(mp.active_children()):
            self.wrLog("wait all process to terminate",mp.active_children())
            time.sleep(1)
        #cancel timers
        self.clearTimers()
        while mp.active_children() :
            self.wrLog("There are still running process wait")
            time.sleep(1)
        self.db.close()
        self.keepAlive.close()


if __name__ == '__main__' :
    print("call Server from main")
    print(__file__)
    print(os.path.samefile(__file__,sys.argv[0]))
    conf={}
    last = None
    ## Read CLI Parameters
    for param in sys.argv:
        if last:
            conf[last] = param
            last = None
        else :
            if not ( os.path.exists(param) and os.path.samefile(__file__, param ) ):
                last = param

    if conf.keys():
        print("Start Server with following params:")
        for k,v in conf.items():
            print("%-10s:" % k, v )
        print('-' * 60)

    server = ETLServer(**conf)
    server.run()