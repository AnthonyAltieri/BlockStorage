/* This file contains data structures and methods common to all service */

namespace cpp shared
namespace py shared
namespace java shared

// Data type for common responses and ACK
enum ResponseType {
	OK = 1,
	ERROR
}

struct Response {
	1: ResponseType message
}

enum UploadResponseType {
	OK = 1,
    MISSING_BLOCKS,
    FILE_ALREADY_PRESENT,
    ERROR,
    FILE_VERSION_OLD,
}

struct UploadResponse {
	1: UploadResponseType status,
	2: list<string> hashList,
}


struct File {
	1: string filename,
	2: double version,
	3: list<string> hashList,
	5: ResponseType status
}


typedef list<File> Files

struct clusterInfo {

}

struct serverInfo {

}

// Add any data structure you need here
