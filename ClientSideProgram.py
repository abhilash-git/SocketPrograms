#!/usr/bin/python
# Author : Abhilash C S (abhilashcs111@gmail.com)
# Client Side program : c1_p.py
# V 1
# python V 2.4

import os
import sys
import time
import copy
import socket

class ClientSocket:
    '''
    just like that
    '''
    def __init__(self, to_address='127.0.0.1', to_port=12345 ):
        self.to_address = to_address
        self.to_port = to_port
        try:
            self.obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.obj.connect((self.to_address, self.to_port))
            self.connected2Server = True
            time.sleep(0.25)
        except Exception:
            t, e = sys.exc_info()[:2]
            print "FAILED TO CONNECT"
            print e
    
    def __del__(self):
        print "Socket destroyed"
    
    def disconnectSocket(self):
        if self.connected2Server:
            self.obj.close()
            self.connected2Server = False
            time.sleep(0.25)
        else:
            print "No connection"
    
    def sendMessage(self,message):
        if self.connected2Server:
            print(message)
            self.obj.send(message)
            time.sleep(0.25)
        else:
            print "No connection"
    
    def receiveMessage(self):
        if self.connected2Server:
            self.obj.recv(1024)
            time.sleep(0.25)
        else:
            print "No connection"

# returns a tuple of (1020 character max lines , length of the line)
def yieldlines(file,fromposition):
    f = open(file,'rb')
    if fromposition > 0:
        f.seek(fromposition)
        
    l = f.read(1020)
    while (l):
        yield (l,len(l))
        l = f.read(1020)
    f.close()


# creates an index to a temp file so when recover it can refer to this and start
def updateCurrentIndex(val):
    f=open(indexFile,'w')
    f.write(str(val))
    f.close()


# get the current reference after a startup
def getCurrentIndex():
    if os.path.exists(indexFile):
        f=open(indexFile)
        try:
            idx = f.readline()
        except:
            idx=o
            updateCurrentIndex(idx)
        f.close()
        return int(idx)
    else:
        idx=0
        updateCurrentIndex(idx)
        return int(idx)

# -----------------------------------------------------------------------------------------------------------------------------------------------
# heart of the program
# --------------------
# first time reads just based on filename and location 
#   -> improvement to store the inode of the last read file in the temp file so to recover while startup after a crash from it last read.
#   on first read , returns lastModified, index position and the current Inode values
#   on second read it validates 
#       based on change in Inode -> if changed it means file rotated
#       when file rotated, find file with of that old inode. (here we assume the name of the file to be "similar not same" after rotate
#       and gets all files with similar name till the current one
#       read the oldest file from the last position till end
#       keep on reading till the latest file
#
#   on proper finding the new records to send to server , it creates socket object and send the message to server and close
#   i assumed the server is gonna over load if all the connections are on. may need to multi thread the server program 
#
#   on proper read return lastModified,lastLineIndex,lastInode,True (length =3)
#   when file roated return info,True   (length =2)
#   on error return False (length = 1)
# -----------------------------------------------------------------------------------------------------------------------------------------------
def readFile(loc,filename,lastModified=0, lastLineIndex=0, lastInode=0):
    try:
        file = str(loc) + "/" + str(filename)
        print file
        info = os.stat(file)
        print info
        if info[1] == lastInode or lastInode==0:            # Inode check to see if the file is rotated or not, first time it is 0
            while info[8] > lastModified:   # last modified always move forward
                lastModified = info[8]
                lastInode = info[1]
                t=ClientSocket()            # socket object created
                for newline, length_of_line in yieldlines(filename,lastLineIndex):
                    lastLineIndex += length_of_line
                    t.sendMessage(newline)
                updateCurrentIndex(lastLineIndex)
                t.disconnectSocket()        # disconnect from socket
                del t                       # remove the object
            return lastModified,lastLineIndex,lastInode,True
        else:
            return info,True                # if rotated, returns the inode of current
    except Exception:
        t, e = sys.exc_info()[:2]
        print("some error")
        print e
        return False


# -----------------------------------------------------------------------------------------------------------------------------------------------
# Kidney of the program :)
# ------------------------
# Cant work with just heart, yeah you need a filter too
# filters all the files from the location with our inode reference, ofcourse file name too, till the latest file and return a list
# in case the file we are looking for doesnot exist then its a gone case :( then u need a Dialysis 
# -----------------------------------------------------------------------------------------------------------------------------------------------

def pickFilesBetweenInodes(loc,filename, fromInode):
    '''
    Only during file rotation to find all the old files, excluding 
    '''
    temp_file_list=[]
    ret_file_list=[]
    for i in os.walk(loc):
        if i[0] == loc:                                                     # root
            for each_file in i[2]:                                          # list of files
                if filename in each_file:                                   # similar to the file in reference
                    file = str(loc) + "/" + str(each_file)  
                    temp_file_list.append(os.stat(file) +(each_file,))          # append name to the end of tuple
    temp_file_list = sorted(temp_file_list, key=lambda mod_date: mod_date[8])   # sort by mod_date, oldest first
    ret_file_list = copy.deepcopy(temp_file_list)
    print "-------"
    print ret_file_list
    for obj in temp_file_list:
        if obj[1] == int(fromInode):
            break
        else:
            print "popping",obj[1],fromInode
            ret_file_list.pop(0)                                            # pop the oldest file from the last referred file
    return ret_file_list




####### MAIN STARTS HERE ##############
# there is no going back!!!!!!!!!!!!!!!



# file info
loc = os.getcwd()
filename='animal'

global indexFile
indexFile='/tmp/.CurrentIndexPosition.txt'

lastModified=0
lastLineIndex=getCurrentIndex()
lastInode=0
print lastModified, lastLineIndex, lastInode

while True:
    try:
        time.sleep(10)
        ret = readFile(loc, filename,lastModified,lastLineIndex, lastInode)
        if len(ret) == 4:               # if inode are same which is the usual case
            lastModified = ret[0]
            lastLineIndex = ret[1]
            lastInode = ret[2]
            print lastModified, lastLineIndex, lastInode
        elif len(ret) == 2:             # if inode are different, but cant guarantee all files
            #newFileInfo= ret[1]
            files_object = pickFilesBetweenInodes(loc,filename,lastInode)
            #print(files_object)
            if len(files_object) >= 1:
                object_count=0
                for each_file in files_object:
                    object_count += 1
                    ret=readFile(loc, each_file[10],lastModified,lastLineIndex, each_file[1])
                    #readFile(loc, files_object[0][10],files_object[0][8],lastLineIndex, files_object[0][1])
                    lastLineIndex=ret[1]
                    if object_count < len(files_object):
                        lastLineIndex=0
                    lastModified = ret[0]
                    lastInode = ret[2]
                    print lastModified, lastLineIndex, lastInode, "in ret2"
            else:   # Dialysis
                lastModified = 0
                lastLineIndex = 0
                lastInode = 0
                updateCurrentIndex(lastLineIndex)
        elif ret ==  False:
            print("Failed to read file")
    except Exception:
        print "main error"
        t, e = sys.exc_info()[:2]
        print e