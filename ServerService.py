import sys
import os

from blockServer import BlockServerService
from metadataServer import MetadataServerService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.server import TServer
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

def getConfig(configpath):
    # Make sure that config file exists
    if not(os.path.exists(configpath)):
        sys.stderr.write('Config file does not exist\n')
        exit(-1)
    # Make sure that config file is a file
    if not(os.path.isfile(configpath)):
        sys.stderr.write('Config file is not a file\n')
        exit(-1)
    configFile = open(configpath)
    lines = configFile.readlines()
    config = {}
    config['metadata'] = {}

    def isMetadataLabel(label):
        result = True
        result = result and (label[0] == 'm')
        result = result and (label[1] == 'e')
        result = result and (label[2] == 't')
        result = result and (label[3] == 'a')
        result = result and (label[4] == 'd')
        result = result and (label[5] == 'a')
        result = result and (label[6] == 't')
        result = result and (label[7] == 'a')
        return result

    for line in lines:
        split = line.split(':')
        label = split[0]
        number = int(split[1].split('\n')[0])
        if label == 'M':
            config['M'] = number
        elif isMetadataLabel(label):
            config['metadata'][int(label[8])] = number
        elif label == 'block':
            config['block'] = number

    return config



def getClient(serverType, port):
    transportSocket = TSocket.TSocket('localhost', port)
    transport = TTransport.TBufferedTransport(transportSocket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    if serverType == "metadata":
        client = MetadataServerService.Client(protocol)
    elif serverType == "block":
        client = BlockServerService.Client(protocol)
    else:
        raise KeyError

    try:
        transport.open()
    except Exception as e:
        return None
    shuttle = {}
    shuttle['transport'] = transport
    shuttle['client'] = client
    return shuttle

def findMetaClient(config):
    msClient = None
    for i in range(1, len(config.get('metadata').keys()) + 1):
        metaPort = int(config.get('metadata').get(i))
        shuttle = getClient('metadata', metaPort)
        if shuttle != None:
            return shuttle
    exit(-1)

def uploadResponseTypeToString(uploadResponseType):
    if uploadResponseType == 1:
        return "OK"
    elif uploadResponseType == 2:
        return "MISSING_BLOCKS"
    elif uploadResponseType == 3:
        return "FILE_ALREADY_PRESENT"
    elif uploadResponseType == 4:
        return "ERROR"
    elif uploadResponseType == 5:
        return "FILE_VERSION_OLD"
    else:
        print "ERROR"
        exit(-1)


