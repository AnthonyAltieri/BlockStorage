from Utils import keys
import hashlib
import sys
import logging
logging.basicConfig(level=logging.DEBUG)

import FileService

from blockServer import BlockServerService
from blockServer.ttypes import *
from shared.ttypes import *

def create(h ='', data ='', status ='VALID'):
    hashBlock = HashBlock()
    hashBlock.hash = h
    hashBlock.data = data
    hashBlock.status = status
    # hashBlock.blockServers = []
    return hashBlock

def blockResponseNotFound():
    blockResponse = BlockResponse()
    blockResponse.status = BlockResponseStatus.NOT_FOUND
    return blockResponse

def blockResponseFound(hashBlock):
    blockResponse = BlockResponse()
    blockResponse.hashBlock = hashBlock
    blockResponse.status = BlockResponseStatus.FOUND
    return blockResponse

def createInvalid():
    hashBlock = HashBlock()
    hashBlock.status = 'INVALID'
    return hashBlock


def createHashBlocks(path):
    blockList = []
    with open(path, 'rb') as f:
        chunk = f.read(BLOCK_SIZE)
        while chunk:
            hash = hashlib.sha256(chunk).hexdigest()
            blockList.append(create(hash, chunk))
            chunk = f.read(BLOCK_SIZE)
    return blockList

def createHashBlockMap(path):
    hashBlocks = createHashBlocks(path)
    sys.stderr.write("created " + str(len(hashBlocks)) + " hashBlocks\n")
    map = {}
    for hashBlock in hashBlocks:
        map[hashBlock.hash] = hashBlock
    return map

def getHashList(path):
    hashList = []
    for hashBlock in createHashBlocks(path):
        hashList.append(hashBlock.hash)
    return hashList

def createHashList(blockList):
    hashList = []
    for b in blockList:
        hashList.append(b.hash)
    return hashList

def getNumMissingBlocks(hashList):
    if hashList == None:
        return 0
    num = 0
    for hash in hashList:
        if hash != "HAS_BLOCK":
            num += 1
    return num

# Block Size = 4Mib
BLOCK_SIZE = 4194304

