/**
 * @file
 * @brief  WebHDFS client
 * @author gruzovator@gmail.com
 * @date   2015-07-15
 */
#include <vector>
#include <sstream>
#include <cctype>
#include <iomanip>
#include <mutex>
#include <memory>
#include <curl/curl.h>
#include <jsoncpp/json/json.h>
#include "WebHdfsClient.h"


namespace WebHDFS
{

Exception::Exception(const std::string &error)
    : std::runtime_error(std::string("WebHDFS client error: ") + error)
{
}


std::string details::OptionsBase::toQueryString() const
{
    std::string query;
    for (const auto &item : m_options)
    {
        query.append(item.first);
        query.append(item.second);
    }
    return query;
}

WriteOptions &WriteOptions::setOverwrite(bool overwrite)
{
    m_options["&overwrite="] = (overwrite ? "true" : "false");
    return *this;
}

WriteOptions &WriteOptions::setBlockSize(size_t blockSize)
{
    m_options["&blocksize="] = std::to_string(blockSize);
    return *this;
}

WriteOptions &WriteOptions::setReplication(int replication)
{
    m_options["&replication="] = std::to_string(replication);
    return *this;
}

WriteOptions &WriteOptions::setPermission(int permission)
{
    m_options["&permission="] = std::to_string(permission);
    return *this;
}

WriteOptions &WriteOptions::setBufferSize(size_t bufferSize)
{
    m_options["&buffersize="] = std::to_string(bufferSize);
    return *this;
}

AppendOptions &AppendOptions::setBufferSize(size_t bufferSize)
{
    m_options["&buffersize="] = std::to_string(bufferSize);
    return *this;
}

ReadOptions &ReadOptions::setOffset(long offset)
{
    m_options["&offset="] = std::to_string(offset);
    return *this;
}

ReadOptions &ReadOptions::setLength(long length)
{
    m_options["&length="] = std::to_string(length);
    return *this;
}

ReadOptions &ReadOptions::setBufferSize(size_t bufferSize)
{
    m_options["&buffersize="] = std::to_string(bufferSize);
    return *this;
}

MakeDirOptions &MakeDirOptions::setPermission(int permission)
{
    m_options["&permission="] = std::to_string(permission);
    return *this;
}

RemoveOptions &RemoveOptions::setRecursive(bool recursive)
{
    m_options["&recursive="] = (recursive ? "true" : "false");
    return *this;
}

ClientOptions::ClientOptions()
    : m_connectionTimeout(0)
    , m_dataTransferTimeout(0)
{
}

ClientOptions &ClientOptions::setConnectTimeout(int seconds)
{
    m_connectionTimeout = seconds;
    return *this;
}

ClientOptions &ClientOptions::setDataTransferTimeout(int seconds)
{
    m_dataTransferTimeout = seconds;
    return *this;
}

ClientOptions &ClientOptions::setUserName(const std::string &username)
{
    m_userName = username;
    return *this;
}


namespace
{

/* check libcurl function call result */
inline void checkCurl(CURLcode code)
{
    if (code != CURLE_OK)
    {
        const char *errInfo = curl_easy_strerror(code);
        throw Exception(errInfo ? errInfo : "Unknown");
    }
}

/* make CURL handle and throw Exception if can't */
std::shared_ptr<CURL> createCurlEaseHandle()
{
    // BTW, this doesn't guard against calling libcurl init from other curl-based libs.
    static std::once_flag curlInitFlag;

    std::call_once(curlInitFlag, []
                   {
                       if (curl_global_init(CURL_GLOBAL_ALL) != 0)
                       {
                           throw Exception("libcurl init failed");
                       }
                   });
    auto curlHandle = std::shared_ptr<CURL>(curl_easy_init(), curl_easy_cleanup);
    if (curlHandle.get() == nullptr)
    {
        throw Exception("libcurl easy object creation failed");
    }
    return curlHandle;
}

/* try parse string to json object, return true if string was parsed, otherwise return false */
bool tryParseJson(const std::string &s, Json::Value &v)
{
    Json::Reader reader;
    constexpr auto collectComments = false;
    return reader.parse(s, v, collectComments);
}

/* type to keep fields of server error reply */
struct RemoteError
{
    std::string type;
    std::string message;
};

/* try to parse server error reply */
bool tryParseRemoteError(const std::string &s, RemoteError &remoteError)
{
    Json::Value remoteErrorValue;
    if (tryParseJson(s, remoteErrorValue) && remoteErrorValue.isMember("RemoteException"))
    {
        const auto exceptionValue = remoteErrorValue["RemoteException"];
        remoteError.type = exceptionValue.get("exception", "Unknown").asString();
        remoteError.message = exceptionValue.get("message", "").asString();
        return true;
    }
    return false;
}

} // namesapce

/* class to build WebHDFS operations URLs */
class Client::UrlBuilder
{
public:
    UrlBuilder(const std::string host, int port, const std::string &userName)
        : m_prefix(std::string("http://") + host + ":" + std::to_string(port) + "/webhdfs/v1")
        , m_userName(userName)
    {
    }

    std::string makeUrl(const std::string &remotePath, const std::string &operation)
    {
        std::stringstream oss;
        oss << m_prefix << urlEncode(remotePath);
        if (m_userName.empty())
        {
            oss << "?op=";
        }
        else
        {
            oss << "?user.name=" << m_userName << "&op=";
        }
        oss << operation;
        return oss.str();
    }

    std::string makeUrl(const std::string &remotePath, const std::string &operation,
                        const details::OptionsBase &opts)
    {
        return makeUrl(remotePath, operation) + opts.toQueryString();
    }

private:
    static std::string urlEncode(const std::string &value)
    {
        std::ostringstream escaped;
        escaped.fill('0');
        escaped << std::hex;
        for (const auto &c : value)
        {
            // Keep alphanumeric and other accepted characters intact
            if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' || c == '/')
            {
                escaped << c;
            }
            else
            {
                escaped << '%' << std::setw(2) << static_cast<int>(static_cast<unsigned char>(c));
            }
        }
        return escaped.str();
    }

private:
    std::shared_ptr<CURL> m_curl; // curl for remote path escaping
    const std::string m_prefix;
    const std::string m_userName;
};

/* class to implement http i/o */
class Client::HttpClient
{
    std::shared_ptr<CURL> m_curlHanlde;
    CURL *m_curl;                                    // just a raw ptr handled by m_curlHandle
    std::shared_ptr<curl_slist> m_activeHttpHeaders; // we must handle allocated headers

public:
    HttpClient()
        : m_curlHanlde(createCurlEaseHandle())
        , m_curl(m_curlHanlde.get())
        , m_activeHttpHeaders()
    {
        checkCurl(curl_easy_setopt(m_curl, CURLOPT_NOSIGNAL, 1));
        checkCurl(curl_easy_setopt(m_curl, CURLOPT_USERAGENT, "libcurl-agent/1.0"));
    }

    void setConnectTimeout(int seconds)
    {
        checkCurl(curl_easy_setopt(m_curl, CURLOPT_CONNECTTIMEOUT, seconds));
    }

    void setDataTranfserTimeout(int seconds)
    {
        checkCurl(curl_easy_setopt(m_curl, CURLOPT_TIMEOUT, seconds));
    }

    struct Reply
    {
        static const long RESPONSE_CODE_CLIENT_ERROR = -1L; // special code to indicate client error
        long responseCode = 0L;
        std::string unexpectedResponseContent; // for error or other unexpected reply
        std::string clientError;               // to put an error occured in callback
        std::string redirectUrl;
    };

    struct Request
    {
        enum class Type
        {
            GET,
            PUT,
            POST,
            DELETE
        };
        Type type = Type::GET;
        std::string url;
        bool followRedirect = false;
        std::ostream *pDataSink = nullptr;
        std::istream *pDataSource = nullptr;
        long expectedResponseCode = 0L;
    };

    Reply make(const Request &req)
    {
        Reply reply;
        ReplyHandler replyHandler{reply, req.expectedResponseCode, req.pDataSink, m_curl};

        checkCurl(curl_easy_setopt(m_curl, CURLOPT_URL, req.url.c_str()));
        checkCurl(curl_easy_setopt(m_curl, CURLOPT_FOLLOWLOCATION, req.followRedirect ? 1L : 0L));
        switch (req.type)
        {
        case Request::Type::GET:
            checkCurl(curl_easy_setopt(m_curl, CURLOPT_HTTPGET, 1L));
            setHttpHeaders({"Expect:"});
            break;
        case Request::Type::PUT:
            checkCurl(curl_easy_setopt(m_curl, CURLOPT_UPLOAD, 1L));
            if (req.pDataSource == nullptr)
            {
                checkCurl(curl_easy_setopt(m_curl, CURLOPT_INFILESIZE, 0));
                setHttpHeaders({"Expect:", "Transfer-Encoding:"});
            }
            else
            {
                checkCurl(curl_easy_setopt(m_curl, CURLOPT_INFILESIZE, -1));
                setHttpHeaders({"Expect:", "Transfer-Encoding: chunked"});
            }
            break;
        case Request::Type::POST:
            throw Exception("Post requests not implemented");
            break;
        case Request::Type::DELETE:
            checkCurl(curl_easy_setopt(m_curl, CURLOPT_CUSTOMREQUEST, "DELETE"));
            setHttpHeaders({"Expect:", "Transfer-Encoding:"});
            break;
        }
        checkCurl(curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, ReplyHandler::writeCallback));
        checkCurl(curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, &replyHandler));
        if (req.pDataSource != nullptr)
        {
            checkCurl(curl_easy_setopt(m_curl, CURLOPT_READFUNCTION, streamReadCallback));
            checkCurl(curl_easy_setopt(m_curl, CURLOPT_READDATA, req.pDataSource));
        }

        auto curlCode = curl_easy_perform(m_curl);

        if (curlCode != CURLE_OK)
        {
            // special errors handling to catch errors in client callbacks, indicated by
            // RESPONSE_CODE_CLIENT_ERROR response code.
            if (reply.responseCode == Reply::RESPONSE_CODE_CLIENT_ERROR)
            {
                throw Exception(reply.clientError);
            }
            // main error handling
            else
            {
                checkCurl(curlCode);
            }
        }

        checkCurl(curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &reply.responseCode));
        if (!req.followRedirect)
        {
            const char *redirectUrl;
            checkCurl(curl_easy_getinfo(m_curl, CURLINFO_REDIRECT_URL, &redirectUrl));
            if (redirectUrl)
            {
                reply.redirectUrl = redirectUrl;
            }
        }

        // check remote error
        if (reply.responseCode != req.expectedResponseCode)
        {
            RemoteError remoteError;
            if (tryParseRemoteError(reply.unexpectedResponseContent, remoteError))
            {
                throw Exception("remote error: " + remoteError.message);
            }
            else
            {
                std::stringstream err;
                err << "unexpected server response code: " << reply.responseCode;
                if (!reply.unexpectedResponseContent.empty())
                {
                    err << " (" << reply.unexpectedResponseContent << ")";
                }
                throw Exception(err.str());
            }
        }
        return reply;
    }


private:
    void setHttpHeaders(const std::vector<std::string> &headers = std::vector<std::string>())
    {
        // reset headers to default ones
        checkCurl(curl_easy_setopt(m_curl, CURLOPT_HTTPHEADER, nullptr));

        curl_slist *headersRaw = NULL;
        for (const auto &header : headers)
        {
            headersRaw = curl_slist_append(headersRaw, header.c_str());
            if (headersRaw == nullptr)
            {
                throw Exception("libcurl request headers setup failed");
            }
        }
        m_activeHttpHeaders.reset(headersRaw, curl_slist_free_all);
        checkCurl(curl_easy_setopt(m_curl, CURLOPT_HTTPHEADER, m_activeHttpHeaders.get()));
    }

    static size_t streamReadCallback(char *buffer, size_t size, size_t nitems, void *userdata)
    {
        auto pIStream = static_cast<std::istream *>(userdata);
        const auto dataSize = size * nitems;
        pIStream->read(buffer, dataSize);
        return pIStream->gcount();
    }

    struct ReplyHandler
    {
        Reply &reply;
        const long expectedResponseCodes;
        std::ostream *pDataSink;
        CURL *curl;

        static size_t writeCallback(char *buffer, size_t size, size_t nitems, void *userData)
        {
            auto self = static_cast<ReplyHandler *>(userData);
            auto &reply = self->reply;
            auto pDataSink = self->pDataSink;
            const auto dataSize = size * nitems;

            if (reply.responseCode == 0 &&
                curl_easy_getinfo(self->curl, CURLINFO_RESPONSE_CODE, &reply.responseCode) !=
                    CURLE_OK)
            {
                reply.responseCode = Reply::RESPONSE_CODE_CLIENT_ERROR;
                reply.clientError = "libcurl getinfo failed";
                return 0;
            }

            if (self->expectedResponseCodes != reply.responseCode)
            {
                reply.unexpectedResponseContent.append(buffer, dataSize);
            }
            else if (pDataSink != nullptr)
            {
                pDataSink->write(buffer, dataSize);
                if (!pDataSink->good())
                {
                    return 0;
                }
            }
            return dataSize;
        }
    };
};

Client::Client(const std::string &host, int port, const ClientOptions &opts)
    : m_urlBuilder(new UrlBuilder(host, port, opts.m_userName))
    , m_httpClient(new HttpClient)

{
    if (opts.m_connectionTimeout > 0)
    {
        m_httpClient->setConnectTimeout(opts.m_connectionTimeout);
    }

    if (opts.m_dataTransferTimeout > 0)
    {
        m_httpClient->setDataTranfserTimeout(opts.m_dataTransferTimeout);
    }
}

Client::Client(const std::string &host, const ClientOptions &opts)
    : Client(host, 50070, opts)
{
}


Client::~Client() {}

void Client::writeFile(std::istream &dataSource, const std::string &remotePath,
                       const WriteOptions &opts)
{
    using Request = HttpClient::Request;
    // Step 1. Get dataNodeUrl.
    Request req1;
    req1.type = Request::Type::PUT;
    req1.url = m_urlBuilder->makeUrl(remotePath, "CREATE", opts);
    req1.expectedResponseCode = 307L;
    auto reply = m_httpClient->make(req1);
    if (reply.redirectUrl.empty())
    {
        throw Exception("protocol error: no redirection to data node");
    }

    // Step 2. Put data
    Request req2;
    req2.type = Request::Type::PUT;
    req2.url = reply.redirectUrl;
    req2.pDataSource = &dataSource;
    req2.expectedResponseCode = 201L;
    m_httpClient->make(req2);
}

void Client::readFile(const std::string &remotePath, std::ostream &dataSink,
                      const ReadOptions &opts)
{
    HttpClient::Request req;
    req.type = HttpClient::Request::Type::GET;
    req.url = m_urlBuilder->makeUrl(remotePath, "OPEN", opts);
    req.followRedirect = true;
    req.pDataSink = &dataSink;
    req.expectedResponseCode = 200L;
    m_httpClient->make(req);
}

void Client::makeDir(const std::string &remoteDirPath, const MakeDirOptions &opts)
{
    HttpClient::Request req;
    req.type = HttpClient::Request::Type::PUT;
    req.url = m_urlBuilder->makeUrl(remoteDirPath, "MKDIRS", opts);
    req.expectedResponseCode = 200L;
    std::ostringstream oss;
    req.pDataSink = &oss;
    auto reply = m_httpClient->make(req);
    if (reply.responseCode != req.expectedResponseCode || oss.str() != "{\"boolean\":true}")
    {
        std::stringstream err;
        err << "can't create dir " << remoteDirPath << ", reply:" << oss.str();
        throw Exception(err.str());
    }
}

std::vector<FileStatus> Client::listDir(const std::string &remoteDirPath)
{
    HttpClient::Request req;
    req.type = HttpClient::Request::Type::GET;
    req.url = m_urlBuilder->makeUrl(remoteDirPath, "LISTSTATUS");
    req.expectedResponseCode = 200L;
    req.followRedirect = true;
    std::ostringstream oss;
    req.pDataSink = &oss;
    m_httpClient->make(req);
    std::vector<FileStatus> files;
    Json::Value listingValue;
    if (tryParseJson(oss.str(), listingValue))
    {
        auto items = listingValue["FileStatuses"]["FileStatus"];
        for (auto it = items.begin(); it != items.end(); ++it)
        {
            FileStatus status;
            const auto &statusValue = *it;
            status.accessTime = statusValue["accessTime"].asInt64();
            status.blockSize = statusValue["blockSize"].asUInt64();
            status.group = statusValue["group"].asString();
            status.length = statusValue["length"].asUInt64();
            status.modificationTime = statusValue["modificationTime"].asInt64();
            status.owner = statusValue["owner"].asString();
            status.pathSuffix = statusValue["pathSuffix"].asString();
            status.permission = statusValue["permission"].asString();
            status.replication = statusValue["replication"].asInt();
            auto typeStr = statusValue["type"].asString();
            status.type = typeStr.compare("FILE") == 0 ? FileStatus::PathObjectType::FILE
                                                       : FileStatus::PathObjectType::DIRECTORY;
            files.push_back(status);
        }
    }
    else
    {
        throw Exception("Can't parse dir listing");
    }
    return files;
}

void Client::remove(const std::string &remotePath, const RemoveOptions &opts)
{
    HttpClient::Request req;
    req.type = HttpClient::Request::Type::DELETE;
    req.url = m_urlBuilder->makeUrl(remotePath, "DELETE", opts);
    req.expectedResponseCode = 200L;
    std::ostringstream oss;
    req.pDataSink = &oss;
    m_httpClient->make(req);
    if (oss.str() != "{\"boolean\":true}")
    {
        throw Exception("Can't delete " + remotePath);
    }
}

void Client::rename(const std::string &remotePath, const std::string &newRemotePath)
{
    HttpClient::Request req;
    req.type = HttpClient::Request::Type::PUT;
    req.url = m_urlBuilder->makeUrl(remotePath, "RENAME") + "&destination=" + newRemotePath;
    req.expectedResponseCode = 200L;
    std::ostringstream oss;
    req.pDataSink = &oss;
    m_httpClient->make(req);
    if (oss.str() != "{\"boolean\":true}")
    {
        throw Exception("Can't rename " + remotePath);
    }
}


} // namespace WebHDFS
