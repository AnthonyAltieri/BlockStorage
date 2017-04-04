import os
import sys
import calendar
import time

import BlockService

from shared.ttypes import *


'''
    Gets all of the file paths recursively from a directory. Directory
    path that is passed in MUST exist and be a directory.
'''
def getFiles(basedir):
    files = []
    for path in os.listdir(basedir):
        if os.path.isfile(basedir + path):
            files.append(basedir + path)
    return files

def getFilename(path):
    split = path.split("/")
    if len(split) == 0:
        filename = split[0]
    else:
        filename = split[len(split) - 1]
    return filename

def getFilepath(baseDir, filename):
    if baseDir[len(baseDir) - 1] != "/":
        baseDir += "/"
    return baseDir + getFilename(filename)

def create(path):
    split = path.split("/")
    if len(split) == 0:
        filename = split[0]
    else:
        filename = split[len(split) - 1]
    if (filename[0] == '/'):
        filename = filename[1:]
    f = File()
    f.filename = filename
    f.version = os.path.getmtime(path)
    f.hashList = []
    f.status = ResponseType.OK
    return f

def getData(path):
    data = ''
    with open(path, 'rb') as f:
        return f.read()
    #     while char:
    #         data += char
    #         char = f.read(1)
    # return data

def toBytes(bits):
    # Helper to turn an array of bits to a byte string
    def helper(byte):
        string = ''
        for b in byte:
            string += b
        return string
    bytes = []
    byte = []
    for i, bit in enumerate(bits):
        if len(byte) == 8:
            bytes.append(helper(byte))
            byte = []
        byte.append(bit)
        if i == (len(bits) - 1):
            while len(byte) < 8:
                byte.append('0')
            bytes.append(byte)
    return bytes

def validateBaseDir(baseDir):
    # Make sure that baseDir exists
    if not(os.path.exists(baseDir)):
        sys.stderr.write('Base directory does not exist\n')
        exit(-1)
    # Make sure that base directory is a directory
    if not(os.path.isdir(baseDir)):
        sys.stderr.write('Base directory is not a directory')
        exit(-1)






