#!/usr/bin/env python

import sys
import logging
import threading
from Utils import *
import sched
import time

logging.basicConfig(level=logging.DEBUG)

sys.path.append('gen-py')

# Thrift specific imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.server import TServer
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# Protocol specific imports
from metadataServer import MetadataServerService
from blockServer.ttypes import *
from metadataServer.ttypes import *
from shared.ttypes import *

import ServerService
import FileService

WRITE_QUORUM_SIZE = 2
READ_QUORUM_SIZE = 2
ALL_SERVERS = 3

class MetadataServerHandler():
    def __init__(self, config_path, my_id):
        # Initialize block
        self.config = ServerService.getConfig(config_path)
        print self.config

        # Set the mode that the Metadata Server should run in
        if len(self.config.get('metadata').keys()) == 1:
            self.mode = 'SINGLE_METADATA_SERVER'
        else:
            self.mode = 'MULTIPLE_METADATA_SERVER'

        print "Starting MetadataServer in mode: " + self.mode

        # Store the identification of the Metadata Server
        self.id = int(my_id)

        # Create a lock to eliminate race conditions
        self.lock = threading.Lock()

        # Establish a dictionary for other MetadataServers
        self.onlineMetaServers = {}

        # Establish a list of servers that went live but haven't been
        # connected to yet
        self.readyToConnect = []
        self.keyboardInterrupt = False

        # This dictionary will hold the [filename -> File] data
        self.data = {}

        # Create a BlockServe Client for the MetadataServer
        shuttle = ServerService.getClient('block', self.config['block'])
        if shuttle == None:
            self.blockServerConnected = False
            self.connectToBlockServer()
        else:
            self.bsClient = shuttle.get('client')
            self.blockServerConnected = True

        # Create Clients for the other MetadataServers
        for id in self.config['metadata'].keys():
            if id == self.id:
                continue
            shuttle = ServerService.getClient('metadata',
                                              self.config['metadata'][id])
            if shuttle != None:
                self.onlineMetaServers[id] = shuttle

        if self.mode == "MULTIPLE_METADATA_SERVER":
            self.establishGossip()

    def connectToBlockServer(self):
        shuttle = ServerService.getClient('block', self.config['block'])
        if shuttle == None:
            sys.stderr.write("NOT CONNECTED TO BLOCKSERVER\n")
        else:
            sys.stderr.write("CONNECTED TO BLOCKSERVER\n")
            self.bsClient = shuttle.get('client')
            self.blockServerConnected = True

        if not (self.blockServerConnected) and not (self.keyboardInterrupt):
            thread = threading.Timer(1.0, self.connectToBlockServer)
            thread.daemon = True
            thread.start()
            return

    def establishGossip(self):
        self.lock.acquire()
        print "online metaservers: " + str(len(keys(self.onlineMetaServers)))
        while len(self.readyToConnect) > 0:
            id = self.readyToConnect.pop(0)
            port = self.config.get('metadata').get(id)
            shuttle = ServerService.getClient('metadata', port)
            client = shuttle.get('client')
            if client != None:
                self.onlineMetaServers[id] = shuttle
        for id in keys(self.onlineMetaServers):
            shuttle = self.onlineMetaServers.get(id)
            if shuttle == None:
                continue
            client = shuttle.get('client')
            try:
                metadataResponse = client.getMetadata(self.id)
            except Exception as e:
                continue
            metadata = metadataResponse.metadata
            if metadataResponse.status != MetadataResponseStatus.NONE:
                self.compareMetadata(metadata)
        thread = threading.Timer(5.0, self.establishGossip)
        thread.daemon = True
        if self.lock.locked():
            self.lock.release()
        if not (self.keyboardInterrupt):
            thread.start()
            return
        else:
            for key in keys(self.onlineMetaServers):
                shuttle = self.onlineMetaServers.get(key)
                if shuttle == None:
                    continue
                transport = shuttle.get('transport')
                shuttle.get('client').closeMeta(self.id)
                try:
                    transport.close()
                except Exception as e:
                    pass

    def compareMetadata(self, metadata):
        for key in metadata.keys():
            localFile = self.data.get(key)
            if localFile == None or localFile.version < metadata[key].version:
                self.data[key] = metadata[key]

    def closeMeta(self, id):
        shuttle = self.onlineMetaServers[id]
        self.onlineMetaServers[id] = None
        if shuttle != None:
            shuttle.get('transport').close()

    def getFile(self, filename):
        self.lock.acquire()
        pretty(self.data)
        # Function to handle download request from file
        foundFile = self.data.get(filename)
        f = File()
        f.filename = filename
        if foundFile == None:
            f.status = ResponseType.ERROR
        else:
            f.hashList = foundFile.hashList
            f.status = ResponseType.OK
        self.lock.release()
        return f

    def getBlockServerHashes(self, file):
        r = UploadResponse()
        r.hashList = []
        r.status = UploadResponseType.OK
        for blockHash in file.hashList:
            blockResponse = self.bsClient.getBlock(blockHash)

            if blockResponse.status == BlockResponseStatus.NOT_FOUND:
                r.status = UploadResponseType.MISSING_BLOCKS
                r.hashList.append(blockHash)
            else:
                r.hashList.append("HAS_BLOCK")
        return r

    def storeFile(self, file):
        self.lock.acquire()

        if self.mode == "MULTIPLE_METADATA_SERVER":
            upResponse = self.hasFileLocally(file)
            if upResponse == UploadResponseType.FILE_ALREADY_PRESENT:
                return upResponse

            def failure(upResponse):
                return upResponse.status != UploadResponseType.OK

            def task(shuttle):
                client = shuttle.get('client')
                return client.handleStoreFile(file)

            # Check if file doesn't exist on other servers
            success = doQuorum(self.id, self.onlineMetaServers,
                               WRITE_QUORUM_SIZE, failure, task)

            # If file exists on other servers
            if not(success):
                upResponse = UploadResponse()
                upResponse.status = UploadResponseType.FILE_ALREADY_PRESENT
                self.lock.release()
                return upResponse


        # Check to see if the BlockServer has any missing blocks
        upResponse = self.getBlockServerHashes(file)

        # If it doesn't, store the file
        if upResponse.status != UploadResponseType.MISSING_BLOCKS:
            self.data[file.filename] = file

        self.lock.release()
        return upResponse

    def handleStoreFile(self, file):
        self.lock.acquire()

        if self.hasFileLocally(file):
            upResponse = UploadResponse()
            upResponse.status = UploadResponseType.FILE_ALREADY_PRESENT
            self.lock.release()
            return upResponse

        # Check to see if the BlockServer has any missing blocks
        upResponse = self.getBlockServerHashes(file)

        # If it doesn't, store the file
        if upResponse.status != UploadResponseType.MISSING_BLOCKS:
            self.data[file.filename] = file

        upResponse = UploadResponse()
        upResponse.status = UploadResponseType.OK

        self.lock.release()
        return upResponse

    def hasFileLocally(self, file):
        upResponse = UploadResponse()
        # If the file exists on this local Metadata Server
        if self.data.get(file.filename) != None:
            return True
        return False


    def deleteFile(self, file):
        self.lock.acquire()
        # Function to handle download request from file
        response = Response()
        foundFile = self.data.get(file.filename)
        if foundFile == None:
            response.message = ResponseType.ERROR
            self.lock.release()
            return response

        self.data[file.filename] = None

        # Delete the file on all Metadata Servers
        if self.mode == "MULTIPLE_METADATA_SERVER":

            def failure(response):
                return response.message == ResponseType.ERROR

            def task(shuttle):
                client = shuttle.get('client')
                return client.handleDeleteFile(file)

            success = doQuorum(self.id, self.onlineMetaServers,
                               READ_QUORUM_SIZE, failure, task)

            if not success:
                response.message = ResponseType.ERROR
                self.lock.release()
                return response

        response.message = ResponseType.OK

        self.lock.release()
        return response

    # Called by another MetadataServer as a request to delete a file
    def handleDeleteFile(self, file):
        self.lock.acquire()

        r = Response()

        foundFile = self.data.get(file.filename)
        if foundFile == None:
            r.message = ReponseType.ERROR
            self.lock.release()
            return r

        self.data[file.filename] = None

        r.message = ResponseType.OK

        self.lock.release()
        return r

    def updateFile(self, file):
        self.lock.acquire()

        upResponse = UploadResponse()

        localFile = self.data.get(file.filename)

        if self.mode == "MULTIPLE_METADATA_SERVER":
            def failure(upResponse):
                return upResponse.status == UploadResponseType.FILE_VERSION_OLD

            def task(shuttle):
                client = shuttle.get('client')
                return client.handleUpdateFile(file)

            success = doQuorum(self.id, self.onlineMetaServers,
                               WRITE_QUORUM_SIZE, failure, task)

            if not success:
                upResponse.status = UploadResponseType.FILE_VERSION_OLD
                self.lock.release()
                return upResponse


            if localFile != None:
                # If local file version is newer then file attempting to update
                if file.version <= localFile.version:
                    upResponse = UploadResponse()
                    upResponse.status = UploadResponseType.FILE_VERSION_OLD
                    self.lock.release()
                    return upResponse

        # Determine what hashes are in the BlockServer
        upResponse = self.getBlockServerHashes(file)

        # If the BlockServer has all of the blocks store the updated file
        if upResponse.status != UploadResponseType.MISSING_BLOCKS:
            self.data[file.filename] = file

        self.lock.release()
        return upResponse

    def handleUpdateFile(self, file):
        self.lock.acquire()

        upResponse = UploadResponse()

        localFile = self.data.get(file.filename)

        if self.mode == "MULTIPLE_METADATA_SERVER":
            if localFile != None:
                # If local file version is newer then file attempting to update
                if file.version <= localFile.version:
                    upResponse = UploadResponse()
                    upResponse.status = UploadResponseType.FILE_VERSION_OLD
                    self.lock.release()
                    return upResponse

        # Determine what hashes are in the BlockServer
        upResponse = self.getBlockServerHashes(file)

        # If the BlockServer has all of the blocks store the updated file
        if upResponse.status != UploadResponseType.MISSING_BLOCKS:
            self.data[file.filename] = file

        self.lock.release()
        return upResponse

    def getMetadata(self, id):
        self.lock.acquire()
        if self.onlineMetaServers.get(id) == None:
            self.readyToConnect.append(id)
        metadataResponse = MetadataResponse()

        def hasValue(list):
            hasValue = False
            for i in list:
                if i != None:
                    return True
            return False

        if len(self.data.values()) == 0 or not (hasValue(self.data.values())):
            metadataResponse.status = MetadataResponseStatus.NONE
        else:
            metadataResponse.status = MetadataResponseStatus.OK
            metadataResponse.metadata = self.data
        self.lock.release()
        return metadataResponse

    def readServerPort(self):
        # Get the server port from the config file.
        # id field will determine which metadata server it is 1, 2 or n
        # Your details will be then either metadata1, metadata2 ... metadatan
        # return the port
        return int(self.config.get('metadata').get(self.id))

def doQuorum(id, onlineServers, quorumSize, failure, task):
    # Assume that the local node is taken care of
    numRequired = quorumSize - 1
    numSuccess = 0
    for serverId in keys(onlineServers):
        # Don't send task to self
        if id == serverId:
            continue

        shuttle = onlineServers.get(serverId)
        # If there is no shuttle for this server it must be down
        if shuttle == None:
            continue

        result = task(shuttle)

        # Keep track of how many tasks were a success
        if not(failure(result)):
            numSuccess += 1

    result = numSuccess >= numRequired

    return numSuccess >= numRequired


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Invocation <executable> <config_file> <id>"
        exit(-1)

    config_path = sys.argv[1]
    my_id = sys.argv[2]

    print "Initializing metadata server"
    handler = MetadataServerHandler(config_path, my_id)
    port = handler.readServerPort()
    # Define parameters for thrift server
    processor = MetadataServerService.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    # Create a server object
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    print "Starting server on port : ", port

    try:
        server.serve()
    except (Exception, KeyboardInterrupt) as e:
        print "\nExecption / Keyboard interrupt occured: ", e
        handler.keyboardInterrupt = True
        if handler.lock.locked():
            try:
                handler.lock.release()
            except Exception as e:
                # Silently fail
                pass
        exit(0)
