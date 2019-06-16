#This file handle the databse interface.
# this module use the python db API to connect and execute DB request
# each databse hase different connection/configuration definition.
# I do not have a live DB so I coulden't test it - I used dummy db which is NOT connected to any DB
# to select DB run the ETLServer with 'db' option
# Example : use Dummy DB -
# python ETLServer.py db "{\"dbType\" : \"Dummy\" ,\"user\": \"Kreda\", \"password\": \"teta\", \"host\": \"local\", \"port\": 8888 }"
#
# Example use sqlite DB -
# python ETLServer.py db "{\"dbType\" : \"sqlite\" , \"dbName\" : \"worksOK\" }"
#
# Note -  DO Not Create directly the classes in this module ! use the  'FactoryDB' function
#

class baseDB:
    ####### Dummy DataBase - no Real connection

    def __init__(self,dbConfig,logger,**kwargs):
        self.fileTable = kwargs.get('fileTable', "Table_FileNames")
        self.fieldName = kwargs.get('fileField', 'Field_FileName')
        self.KATable = kwargs.get('KATable', "Table_KeepAlive")
        self.KAField = kwargs.get('KAField', 'Field_KeepAlive')
        self.connectParams = dbConfig
        self.wrLog = logger

    def connect(self):
        connStr = "dsn=%s:%d, user='%s', password='%s'" % (
            self.connectParams['host'],self.connectParams['port'],self.connectParams['user'],
            self.connectParams['password'] ,
        )
        self.wrLog("Debug - Connection String:",connStr)

    def close(self):
        self.wrLog('closing DB connection')

    def sqlAppendFile(self,fileName):
        return "INSERT INTO %s (%s) VALUES (%s);" % (self.fileTable, self.fieldName, fileName)

    def sqlKA(self):
        return "UPDATE %s SET %s = %s ;" % (self.KATable,self.KAField,'CURRENT_TIMESTAMP')

    def appendFile(self, filename):
        self.wrLog("sending to DB: '%s'" % self.sqlAppendFile(filename))

    def keepalive(self):
        self.wrLog("sending to DB: '%s'" % self.sqlKA())

class sqlite3DB(baseDB):

    ### Sqlite DataBase

    def connect(self):
        import sqlite3
        connStr = self.connectParams['dbName']
        self.wrLog("Debug - Coonection String:", connStr)
        self.conn = sqlite3.connect(connStr)



    def appendFile(self, filename):
        cursor = self.conn.cursor()
        cursor.execute(self.sqlAppendFile(filename))
        self.conn.commit()

    def keepalive(self):
        cursor = self.conn.cursor()
        cursor.execute(self.sqlKA())
        self.conn.commit()

    def close(self):
        self.conn.close()

def FactoryDB(dbconf,logger):

    dbType = dbconf.pop('dbType')

    if dbType == 'Dummy' :
        return baseDB(dbconf,logger)
    elif dbType == 'sqlite' :
        return sqlite3DB(dbconf,logger)
    else :
        raise RuntimeError('Unsupported db type %s' % dbType)

