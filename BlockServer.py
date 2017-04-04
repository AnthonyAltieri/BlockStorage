#!/usr/bin/env python

import sys
from Utils import *

sys.path.append('gen-py')

from blockServer import BlockServerService
from blockServer.ttypes import *
from shared.ttypes import *

import BlockService
import ServerService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.server import TServer
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


class BlockServerHandler():

    def __init__(self, configpath):
        # Initialize using config file, intitalize state etc
        # Get the config
        self.config = ServerService.getConfig(configpath)

        if len(self.config.get('metadata').keys()) == 1:
            self.mode = 'SINGLE_METADATA_SERVER'
        else:
            self.mode = 'MULTIPLE_METADATA_SERVER'

        # Initialize data store
        self.data = {}
        pass

    def storeBlock(self, hashBlock):
        # Store hash block, called by client during upload
        r = Response()
        if self.data.get(hashBlock.hash) == None:
            r.message = ResponseType.OK
            self.data[hashBlock.hash] = hashBlock
        else:
            r.message = ResponseType.ERROR
        return r

    def getBlock(self, hash):
        # Retrieve block using hash, called by client during download
        hashBlock = self.data.get(hash)
        if hashBlock == None:
            result = BlockService.blockResponseNotFound()
        else:
            result = BlockService.blockResponseFound(hashBlock)
        return result



    def deleteBlock(self, hash):
        # Delete the particular hash : block pair
        r = Response()
        data = self.data.get(hash)
        if data == None:
            r.message = ResponseType.ERROR
        else:
            self.data[hash] = None
            r.message = ResponseType.OK
        return r

    def readServerPort(self):
        # In this function read the configuration file and get the port number for the server
        return int(self.config.get('block'))


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Invocation <executable> <config_file>")
        exit(-1)

    config_path = sys.argv[1]

    print "Initializing block server"
    handler = BlockServerHandler(config_path)
    # Retrieve the port number from the config file so that you could strt the server
    port = handler.readServerPort()
    # Define parameters for thrift server
    processor = BlockServerService.Processor(handler)
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
        exit(1)
