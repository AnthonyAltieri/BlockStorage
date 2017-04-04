include "shared.thrift"

namespace cpp blockServer
namespace py blockServer
namespace java blockServer

/* The status field can be used to communicate state information
	example, when the client requests a block that is not present
	you can set the status as ERROR */

//typedef shared.serverInfo serverInfo
typedef shared.Response Response


struct HashBlock {
	1: string hash,
	2: binary data,
	3: string status
//	4: list<string> blockServers
}

struct HashBlocks {
	1: list<HashBlock> blocks
}

enum BlockResponseStatus {
    NOT_FOUND,
    FOUND
}

struct BlockResponse {
    1: BlockResponseStatus status,
    2: HashBlock hashBlock
}

exception BlockNotFoundException {}

service BlockServerService {
	Response storeBlock(1: HashBlock hashBlock),
	BlockResponse getBlock(1: string hash)
	Response deleteBlock(1: string hash)

	// Add any procedure you need below

}
