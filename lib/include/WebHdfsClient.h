/**
 * @file
 * @brief  WebHDFS client
 * @author gruzovator@gmail.com
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
class Exception : public std::runtime_error
{
public:
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
    ~OptionsBase() = default;
    std::map<std::string, std::string> m_options;
};

}

class WriteOptions : public details::OptionsBase
{
public:
    WriteOptions &setOverwrite(bool overwrite);
    WriteOptions &setBlockSize(size_t blockSize);
    WriteOptions &setReplication(int replication);
    WriteOptions &setPermission(int permission);
    WriteOptions &setBufferSize(size_t bufferSize);
};

class AppendOptions : public details::OptionsBase
{
public:
    AppendOptions &setBufferSize(size_t bufferSize);
};

class ReadOptions : public details::OptionsBase
{
public:
    ReadOptions &setOffset(long offset);
    ReadOptions &setLength(long length);
    ReadOptions &setBufferSize(size_t bufferSize);
};

class MakeDirOptions : public details::OptionsBase
{
public:
    MakeDirOptions &setPermission(int permission);
};

class RemoveOptions : public details::OptionsBase
{
public:
    RemoveOptions &setRecursive(bool recursive);
};

/** @} */


/** @brief HDFS filesystem item info
 *
 *  See %FileStatus object desription in %WebHDFS project docs.
 */
struct FileStatus
{
    long accessTime = 0;
    size_t blockSize = 0;
    std::string group;
    size_t length = 0;
    long modificationTime = 0;
    std::string owner;
    std::string pathSuffix;
    std::string permission;
    int replication = 0;
    enum class PathObjectType
    {
        FILE,
        DIRECTORY
    };
    PathObjectType type = PathObjectType::FILE;
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
 *               .setDataTransferTimeout(600)
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
    explicit Client(const std::string &host, const ClientOptions &opts = ClientOptions());

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
