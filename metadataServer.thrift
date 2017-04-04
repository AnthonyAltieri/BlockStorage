include "shared.thrift"

namespace cpp metadataServer
namespace py metadataServer
namespace java metadataServer

/* we can use shared.<datatype>, instead we could also typedef them for
	convenience */
typedef shared.Response Response
typedef shared.clusterInfo clusterInfo
typedef shared.File File
typedef shared.Files Files
typedef shared.serverInfo serverInfo
typedef shared.UploadResponse UploadResponse

exception FileNotFoundException {}

typedef map<string, File> Metadata
enum MetadataResponseStatus {
    OK,
    NONE
}
struct MetadataResponse {
    1: Metadata metadata,
    2: MetadataResponseStatus status,
}

service MetadataServerService {
	File getFile(1: string filename),
	UploadResponse storeFile(1: File f),
	UploadResponse handleStoreFile(1: File f),
	Response deleteFile(1: File f),
	Response handleDeleteFile(1: File f),
	UploadResponse handleUpdateFile(1: File f),
	UploadResponse updateFile(1: File f),
	MetadataResponse getMetadata(1: i32 id),
	void closeMeta(1: i32 id)
}
