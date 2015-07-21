/**
 * @file
 * @brief  WebHDFS client
 * @author gruzovator
 * @date   2015-07-15
 */
#ifndef WEBHDFS_CLIENT_H
#define WEBHDFS_CLIENT_H

#include <stdexcept>
#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <memory>

namespace WebHDFS
{

/** WebHDFS client error */
struct Exception : std::runtime_error
{
    Exception(const std::string &error);
};

/** @defgroup WebHDFS operations options
 * See WebHDFS doc for detailed options info.
 * @{
 */

class OptionsBase
{
public:
    std::string toQueryString() const;

protected:
    ~OptionsBase() {}
    std::map<std::string, std::string> m_options;
};

struct WriteOptions : OptionsBase
{
    WriteOptions &setOverwrite(bool overwrite);
    WriteOptions &setBlockSize(size_t blockSize);
    WriteOptions &setReplication(int replication);
    WriteOptions &setPermission(int permission);
    WriteOptions &setBufferSize(size_t bufferSize);
};

struct AppendOptions : OptionsBase
{
    AppendOptions &setBufferSize(size_t bufferSize);
};

struct ReadOptions : OptionsBase
{
    ReadOptions &setOffset(long offset);
    ReadOptions &setLength(long length);
    ReadOptions &setBufferSize(size_t bufferSize);
};

struct MakeDirOptions : OptionsBase
{
    MakeDirOptions &setPermission(int permission);
};

struct RemoveOptions : OptionsBase
{
    RemoveOptions &setRecursive(bool recursive);
};

/** @}*/


/** File or Dir info (see FileStatus desription in WebHDFS docs)*/
struct FileStatus
{
    long accessTime;
    size_t blockSize;
    std::string group;
    size_t length;
    long modificationTime;
    std::string owner;
    std::string pathSuffix;
    std::string permission;
    int replication;
    enum class PathObjectType
    {
        FILE,
        DIRECTORY
    };
    PathObjectType type;
};

class Client;

/**
 * @brief client configuration
 *
 * Call 'set' functions to change an option, otherwise default value will be used.
 */
class ClientOptions
{
public:
    ClientOptions();

    /**
     * @brief set connection timeout (default timeout is 300 seconds)
     * @param seconds
     * @return
     */
    ClientOptions &setConnectTimeout(int seconds);

    /**
     * @brief set data transfer timeout (default timeout is inf)
     * @param seconds
     * @return
     */
    ClientOptions &setDataTransferTimeout(int seconds);

    /**
     * @brief set user name for authentication
     * @param username
     * @return
     */
    ClientOptions &setUserName(const std::string &username);

private:
    friend class Client;
    int m_connectionTimeout;
    int m_dataTransferTimeout;
    std::string m_userName;
};



/**
 * @brief WebHDFS client class
 * TODO: usage example
 */
class Client
{
public:
    /**
     * @brief create WebHDFS client
     * @param host - WebHDFS service hostname
     * @param port - WebHDFS service port
     * @param opts - client settings
     */
    Client(const std::string &host, int port, const ClientOptions &opts = ClientOptions());

    /**
     * @brief create WebHDFS client
     * @param host - WebHDFS service hostname (using default service port)
     * @param opts - client options
     */
    Client(const std::string &host, const ClientOptions &opts = ClientOptions());

    ~Client();

    Client(const Client &) = delete;

    Client &operator=(const Client &) = delete;


    /** @defgroup HDFS operations
     *
     * @{
     */

    void writeFile(std::istream &dataSource,
                   const std::string &remoteFilePath,
                   const WriteOptions &opts = WriteOptions());

    void readFile(const std::string &remoteFilePath,
                  std::ostream &dataSink,
                  const ReadOptions &opts = ReadOptions());

    void makeDir(const std::string &remoteDirPath, const MakeDirOptions &opts = MakeDirOptions());

    std::vector<FileStatus> listDir(const std::string &remoteDirPath);

    void remove(const std::string &remotePath, const RemoveOptions &opts = RemoveOptions());

    void rename(const std::string &remotePath, const std::string &newRemotePath);

    /** @} */

private:
    class UrlBuilder;
    std::unique_ptr<UrlBuilder> m_urlBuilder;
    class HttpClient;
    std::unique_ptr<HttpClient> m_httpClient;
};


} // namespace WebHDFS

#endif
