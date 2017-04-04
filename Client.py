#!/usr/bin/env python

import sys
import os
import logging
logging.basicConfig(level=logging.DEBUG)
from Utils import *

sys.path.append('gen-py')

# Thrift specific imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from shared.ttypes import *
from metadataServer.ttypes import *
from blockServer.ttypes import *

from metadataServer import MetadataServerService
from blockServer import BlockServerService

import ServerService
import FileService
import BlockService


# Add classes / functions as required here

# Prints `ERROR` to stdout and exits the program
def Error():
    print "ERROR"
    exit(-1)

# Prints `OK` to stdout and exits the program
def Ok():
    print "OK"
    exit(0)

# Write to stderr in the style of a Client log
def logClient(*args):
    lineStart = "CLIENT >> "
    for a in args:
        sys.stderr.write(lineStart + a + "\n")

# Write to stderr in the style of a debug log
def logDebug(*args):
    lineStart = "[DEBUG] "
    for a in args:
        sys.stderr.write(lineStart + a + "\n")


def storeMissingBlocks(missingHashes, hashBlockMap, bsClient):
    numberStored = 0
    # For every HashBlock missing upload it to the BlockServer
    for hash in missingHashes:
        # If hash is None that block is already stored
        if hash == "HAS_BLOCK":
            continue
        hashBlock = hashBlockMap.get(hash)
        # There shouldn't be a case where this happens but log it just in case
        if hashBlock == None:
            logClient("No hashBlock found for hash " + hash + " in hashBlockMap")
            Error()
        else:
            try:
                response = bsClient.storeBlock(hashBlock)
                numberStored += 1
            except Exception as e:
                Error()
    logClient("Stored " + str(numberStored) + " blocks in BlockServer")


def getLocalBlocks(baseDir):
    localBlocks = {}
    # Get file paths for all files in base directory
    filesPaths = FileService.getFiles(baseDir)

    logClient(str(len(filesPaths)) + " files in baseDir")

    # Upload the files to the Servers
    for path in filesPaths:
        blocks = BlockService.createHashBlocks(path)
        for block in blocks:
            localBlocks[block.hash] = block

    logClient("Stored all local blocks")
    return localBlocks


def upload(mode, baseDir, filename, msClient, bsClient, isBaseDirFile):
    filepath = FileService.getFilepath(baseDir, filename)

    logClient("Uploading file " + filepath)

    # If file doesn't exist error
    if not(os.path.isfile(filepath)):
        Error()

    # Create the File for the desired file
    file = FileService.create(filepath)
    file.hashList = BlockService.getHashList(filepath)

    logClient(str(file))

    logClient("Attempting to store file in MetadataServer: " + str(file))

    uploadResponse = msClient.storeFile(file)

    status = ServerService.uploadResponseTypeToString(uploadResponse.status)
    logClient("Upload response status: " + status)

    if uploadResponse.status == UploadResponseType.FILE_ALREADY_PRESENT \
            and mode == "HOMEWORK":
        logClient("File already present in MetadataServer")
        Error()

    # If all blocks of this file are already in the BlockServer, exit
    if uploadResponse.status != UploadResponseType.MISSING_BLOCKS:
        logClient("BlockServer already contains all blocks")
        if isBaseDirFile:
            return uploadResponse
        else:
            Ok()

    numMissingBlocks = BlockService.getNumMissingBlocks(uploadResponse.hashList)
    numFileBlocks = len(file.hashList)
    ratio = str(numMissingBlocks) + "/" + str(numFileBlocks)
    logClient("Missing " + ratio + " HashBlocks in BlockServer")

    # Create a map for the blocks in that file, and store missing blocks
    hashBlockMap = BlockService.createHashBlockMap(filepath)

    storeMissingBlocks(uploadResponse.hashList, hashBlockMap, bsClient)

    logClient("Attempting to store file in MetadataServer again: " + str(file))

    uploadResponse = msClient.storeFile(file)

    numMissingBlocks = BlockService.getNumMissingBlocks(uploadResponse.hashList)
    ratio = str(numMissingBlocks) + "/" + str(numFileBlocks)
    logClient("Missing " + ratio + " HashBlocks in BlockServer")

    # There should be no hashes not found in the BlockServer at this point
    if uploadResponse.status == UploadResponseType.MISSING_BLOCKS:
        Error()

    logClient("File successfully uploaded")
    return uploadResponse


def download(basedir, filename, msClient, bsClient, localBlocks):
    file = msClient.getFile(filename)

    logClient("download " + filename)

    # If no file was found for that filename, exit
    if file.status == ResponseType.ERROR:
        Error()

    logClient("About to download blocks missing on local machine")

    numBlocksDownloaded = 0

    # Get the blocks that are not local on the Client
    for h in file.hashList:
        if localBlocks.get(h) == None:
            blockResponse = bsClient.getBlock(h)
            numBlocksDownloaded += 1
            if blockResponse.status == BlockResponseStatus.NOT_FOUND:
                Error()
            hashBlock = blockResponse.hashBlock
            localBlocks[h] = hashBlock

    logClient("Downloaded " + str(numBlocksDownloaded) + " blocks")
    logClient("Writing to file")
    # Write the file
    with open(baseDir + filename, 'wb') as out:
        for h in file.hashList:
            hashBlock = localBlocks.get(h)
            out.write(hashBlock.data)
        out.close()
    logClient("Successfully downloaded " + filename)


def update(mode, basedir, filename, msClient, bsClient):
    filepath = FileService.getFilepath(baseDir, filename)

    file = FileService.create(filepath)
    file.hashList = BlockService.getHashList(filepath)

    logClient(str(file))

    logClient("Update " + filepath)

    # Create a map for the blocks in that file
    hashBlockMap = BlockService.createHashBlockMap(filepath)

    logClient("Calling update on MetadataServer")
    uploadResponse = msClient.updateFile(file)

    status = ServerService.uploadResponseTypeToString(uploadResponse.status)
    logClient("Upload response status: " + status)

    if uploadResponse.status == UploadResponseType.FILE_VERSION_OLD:
        logClient("File version is old, not uploading")
        Error()

    numMissingBlocks = BlockService.getNumMissingBlocks(uploadResponse.hashList)
    numFileBlocks = len(file.hashList)
    ratio = str(numMissingBlocks) + "/" + str(numFileBlocks)
    logClient("Missing " + ratio + " HashBlocks in BlockServer")

    if uploadResponse.status == UploadResponseType.FILE_VERSION_OLD:
        # File attempting to update with has lower version
        Error()
    else:
        logClient("Storing missing blocks")
        storeMissingBlocks(uploadResponse.hashList, hashBlockMap, bsClient)

    # Update File again to make sure that all blocks are present
    uploadResponse = msClient.updateFile(file)

    numMissingBlocks = BlockService.getNumMissingBlocks(uploadResponse.hashList)
    ratio = str(numMissingBlocks) + "/" + str(numFileBlocks)
    logClient("Missing " + ratio + " HashBlocks in BlockServer")

    # There should be no hashes not found in the BlockServer at this point
    if uploadResponse.hashList != None:
        if numMissingBlocks != 0:
            logClient("Hashlist present in UploadResponse but not length 0")
            logClient("Hashlist: " + str(uploadResponse.hashList))
            Error()

    logClient("File successfully updated")
    return uploadResponse


def delete(mode, basedir, filename, msClient):
    file = File()
    file.filename = filename
    response = msClient.deleteFile(file)
    if response.message == ResponseType.ERROR:
        Error()


def validateCommand(command):
    if not (command == 'upload' or command == 'download' or command == 'delete'
            or command == 'update'):
        logClient("Error: invalid command " + command + "\n")
        Error()


if __name__ == "__main__":

    if len(sys.argv) < 5:
        print("Invocation : <executable> <config_file> <base_dir> <command> <filename>")
        exit(-1)

    configpath = sys.argv[1]
    baseDir = sys.argv[2]
    command = sys.argv[3]
    filename = sys.argv[4]

    # Make sure that the command was either `upload` or `download`
    validateCommand(command)

    # Create a config dictionary (includes validation of config file)
    config = ServerService.getConfig(configpath)

    if len(config.get('metadata').keys()) == 1:
        mode = "PROJECT"
    else:
        mode = "HOMEWORK"

    logClient("Starting with mode: " + mode)

    # Make sure baseDir has a `/` at the end
    if baseDir[len(baseDir) - 1] != '/':
        baseDir += '/'

    # Verify that the base directory exists and is a directory
    FileService.validateBaseDir(baseDir)

    # Create the MetadataServer and BlockServer Clients and connect to the transport
    msClient = ServerService.findMetaClient(config).get('client')
    if msClient == None:
        logClient('Couldn\'t find an available MetadataServer')
        Error()

    blockPort = int(config.get('block'))
    bsClient = ServerService.getClient('block', blockPort).get('client')
    if bsClient == None:
        logClient('Error: Couldn\'t connect BlockServer Client')
        Error()

    # Keep a dictionary to keep track of which blocks the client has
    localBlocks = getLocalBlocks(baseDir)

    logClient("localBlocks: " + str(localBlocks))

    if command == 'upload':
        upload(mode, baseDir, filename, msClient, bsClient, False)
    elif command == 'download':
        download(baseDir, filename, msClient, bsClient, localBlocks)
    elif command == 'update':
        update(mode, baseDir, filename, msClient, bsClient)
    else:
        delete(mode, baseDir, filename, msClient)
    print "OK"
