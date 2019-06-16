# This file implement the message representation part
# it may be use bothe by the server and the client
#
# for safe use - do not create any object directly from the classes above use the function 'messageFactory'
# to create the proper class

import json

class Message :
    # Message simple file list with no corruption indication

    def __init__(self,Message):
        self.rowData= Message if isinstance(Message,str) else json.dumps(Message)

    def isMessageOK(self):
        try:
            tmp = self.asJson()
            return True
        except json.JSONDecodeError :
            return False

    def fileList(self):
        return [ fname for fname in self.asJson() ]

    def isFileCorrupted(self,fileName):
        return False

    def asMeassgeStr(self):
        return self.rowData

    def asJson(self):
        return json.loads(self.rowData)

class MessageWithCorFile(Message):
    # Message - as Object: Key is filename - value is boolean

    def isFileCorrupted(self,fileName):
        fDict = self.asJson()
        return fDict.get(fileName,True)

    def isMessageOK(self):
        try :
            for it in self.asJson().values():
                if not isinstance(it,bool):
                    return False
            return True
        except AttributeError :
            return False
        except json.JSONDecodeError :
            return False

class MessageTupple(Message):
    # Message as tupple : first item is filename second is boolean
    def fileList(self):
        return [ fname[0] for fname in self.asJson() ]

    def isFileCorrupted(self, fileName):
        for tup in self.asJson():
            if tup[0] == fileName:
                return tup[1]
        return True

    def isMessageOK(self):
        try:
            for it in self.asJson():
                # print("Json Passs")
                if not isinstance(it[1], bool ):
                    # print("Property of ",it , "is not boolean")
                    return False
            return True
        except json.JSONDecodeError:
            return False


def messageFactory(data):
    """" This function is 'factory' function that return the correct Message class acording to the
    input data.
    data - string that contains json message
    return Message class (or one of it inherir=ted classes)
    """

    for res in [  MessageWithCorFile , MessageTupple , Message ,  ] :
        tmp = res(data)
        print("Checking ",res)
        if tmp.isMessageOK():
            #print("\n\n\n\n\nDebug - Messsage class is :",res,"\n\n\n\n")
            return tmp