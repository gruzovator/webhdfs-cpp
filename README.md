# C++ WebHDFS Client

Simple libcurl wrapper to access HDFS via REST API.

* WebHDFS: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html
* Libcurl: http://curl.haxx.se/libcurl/

## Build requirements
* C++11
* libcurl (https://github.com/bagder/curl)
* jsoncpp (https://github.com/open-source-parsers/jsoncpp)
* boost (for demo application)

## Build
To build static lib and demo application (*webhdfs-client*) run next commands:
```shell
mkdir build
cd build
cmake ..
make
```

## Lib usage example
```c++
#include <iostream>
#include "WebHdfsClient.h"

int main(int argc, char** argv) {
    // print remote file to stdout
    WebHDFS::Client client("hd0-dev", 
                            WebHDFS::ClientOptions().setConnectTimeout(10).setUserName("alex"));
    client.readFile("/tmp/test.txt", std::cout);
    return 0;
}
```
## Demo application *webhdfs-client* 

Copy local file to hdfs:
```bash
./webhdfs-client cp /tmp/test.txt hdfs://hd0-dev/tmp/test.txt
```
Copy file from hdfs:
```bash
./webhdfs-client cp hdfs://hd0-dev/tmp/test.txt /tmp/test-copy.txt
```
List hdfs dir:
```bash
./webhdfs-client ls hdfs://hd0-dev/
```

Print hdfs file to stdout:
```bash
./webhdfs-client cat hdfs://hd0-dev/tmp/test.txt
```

