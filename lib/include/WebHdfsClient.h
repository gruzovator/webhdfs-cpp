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

/** @brief WebHDFS client namespace */
namespace WebHDFS
{

/** @brief Client operations exceptions type */
struct Exception : std::runtime_error
{
    Exception(const std::string &error);
};

/** @defgroup Options Client operations options
 *
 *  See %WebHDFS project docs for detailed options info.
 *  @{
 */

namespace details
{

class OptionsBase
{
public:
    std::string toQueryString() const;

protected:
    ~OptionsBase() {}
    std::map<std::string, std::string> m_options;
};

}

struct WriteOptions : details::OptionsBase
{
    WriteOptions &setOverwrite(bool overwrite);
    WriteOptions &setBlockSize(size_t blockSize);
    WriteOptions &setReplication(int replication);
    WriteOptions &setPermission(int permission);
    WriteOptions &setBufferSize(size_t bufferSize);
};

struct AppendOptions : details::OptionsBase
{
    AppendOptions &setBufferSize(size_t bufferSize);
};

struct ReadOptions : details::OptionsBase
{
    ReadOptions &setOffset(long offset);
    ReadOptions &setLength(long length);
    ReadOptions &setBufferSize(size_t bufferSize);
};

struct MakeDirOptions : details::OptionsBase
{
    MakeDirOptions &setPermission(int permission);
};

struct RemoveOptions : details::OptionsBase
{
    RemoveOptions &setRecursive(bool recursive);
};

/** @} */


/** @brief HDFS filesystem item info
 *
 *  See %FileStatus object desription in %WebHDFS project docs.
 */
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

/** @brief Client options
 *
 *  Call on of 'set' methods to change an option,otherwise default value will be used.
 */
class ClientOptions
{
public:
    ClientOptions();

    /** @brief Set connection timeout (default timeout is 300 seconds) */
    ClientOptions &setConnectTimeout(int seconds);

    /** @brief Set data transfer timeout (default timeout is infinite) */
    ClientOptions &setDataTransferTimeout(int seconds);

    /** @brief Set user name for authentication */
    ClientOptions &setUserName(const std::string &username);

private:
    friend class Client;
    int m_connectionTimeout;
    int m_dataTransferTimeout;
    std::string m_userName;
};

/** @brief %WebHDFS client class
 *
 *  @attention Client is not thread safe
 *
 *  Usage:
 *  @code{.cpp}
 *
 *  const std::string remoteHost("webhdfs.server.local");
 *  const std::string remotePath("/tmp/test.txt");
 *
 *  WebHDFS::ClientOptions clientOptions;
 *  clientOptions.setConnectTimeout(10)
 *               .setDataTransferTimeout(6000)
 *               .setUserName("webhdfs-client");
 *
 *  WebHDFS::Client client(remoteHost, clientOptions);
 *  client.readFile(remotePath, std::cout);
 *
 *  @endcode
 *
 */
class Client
{
public:
    /**
     * @brief Create client
     * @param host %WebHDFS service hostname
     * @param port %WebHDFS service port
     * @param opts Client options
     */
    Client(const std::string &host, int port, const ClientOptions &opts = ClientOptions());

    /**
     * @brief Create client
     * @param host %WebHDFS service hostname (using default service port)
     * @param opts Client options
     */
    Client(const std::string &host, const ClientOptions &opts = ClientOptions());

    ~Client();

    Client(const Client &) = delete;

    Client &operator=(const Client &) = delete;


    /** @name %WebHDFS operations */
    /** @{ */

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
