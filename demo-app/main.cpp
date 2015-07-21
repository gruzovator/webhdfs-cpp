/**
 * @file
 * @brief  WebHDFS Client demo application
 * @author gruzovator
 * @date   2015-07-15
 *
 * Usage example: ./webhdfs cat hdfs://hd0-dev/tmp/webhdfs-test.txt
 */
#include <fstream>
#include <stdexcept>

#include <boost/regex.hpp>
#include "boost/date_time/posix_time/posix_time.hpp"

#include "utils.h"
#include "WebHdfsClient.h"


using utils::log_info;
using utils::log_err;


/** try to match a string to hdfs://<host><remotePath> pattern */
bool parseRemotePath(const std::string &path, std::string &host, std::string &remotePath)
{
    boost::smatch matches;
    static const boost::regex REMOTE_PATH_PATTERN("^hdfs:\\/\\/(.*?)(\\/.*?)$");
    if (boost::regex_match(path, matches, REMOTE_PATH_PATTERN))
    {
        host = matches[1];
        remotePath = matches[2];
        return true;
    }
    return false;
}

int main(int argc, const char *argv[]) try
{
    using namespace std;
    std::string remoteHost;
    std::string remotePath;

    WebHDFS::ClientOptions clientOptions;
    clientOptions.setConnectTimeout(10).setDataTransferTimeout(6000).setUserName(
        "webhdfs-client");

    auto throwWrongRemotePathFormat = [](const std::string &op)
    {
        throw std::runtime_error(op + " command remote path argument has wrong format");
    };

    if (argc == 3 && argv[1] == std::string("cat"))
    {
        std::string target(argv[2]);
        if (!parseRemotePath(target, remoteHost, remotePath))
        {
            throwWrongRemotePathFormat("cat");
        }
        log_info("Printing", target, "...");
        WebHDFS::Client client(remoteHost, clientOptions);
        client.readFile(remotePath, std::cout);
    }
    else if (argc == 4 && argv[1] == std::string("cp"))
    {
        const std::string src(argv[2]);
        const std::string dest(argv[3]);

        if (parseRemotePath(src, remoteHost, remotePath))
        {
            // remote to local
            log_info("Copying", src, "to", dest, "...");
            std::ofstream ofs(dest);
            if (!ofs.is_open())
            {
                throw std::runtime_error(str::concat_ws("Can't open file", dest));
            }
            WebHDFS::Client client(remoteHost, clientOptions);
            client.readFile(remotePath, ofs);
        }
        else if (parseRemotePath(dest, remoteHost, remotePath))
        {
            // local to remote
            log_info("Copying", src, "to", dest, "...");
            std::ifstream ifs(src);
            if (!ifs.is_open())
            {
                throw std::runtime_error(str::concat_ws("Can't open file", src));
            }
            WebHDFS::Client client(remoteHost, clientOptions);
            client.writeFile(ifs, remotePath, WebHDFS::WriteOptions().setOverwrite(true));
        }
        else
        {
            throwWrongRemotePathFormat("cp");
        }
    }
    else if (argc == 3 && argv[1] == std::string("rm"))
    {
        std::string target(argv[2]);
        if (parseRemotePath(target, remoteHost, remotePath))
        {
            log_info("Removing", target, "...");
            WebHDFS::Client client(remoteHost, clientOptions);
            client.remove(remotePath);
        }
        else
        {
            throwWrongRemotePathFormat("rm");
        }
    }
    else if (argc == 3 && argv[1] == std::string("ls"))
    {
        std::string target(argv[2]);
        if (parseRemotePath(target, remoteHost, remotePath))
        {
            log_info(target, "directory listing:");
            WebHDFS::Client client(remoteHost, clientOptions);
            const auto items = client.listDir(remotePath);
            for (auto item : items)
            {
                std::ostringstream oss;
                oss << std::setw(20) << std::left;
                if (item.pathSuffix.length() > 16)
                {
                    oss << item.pathSuffix.substr(0, 16) + "...";
                }
                else
                {
                    oss << item.pathSuffix;
                }
                oss << std::setw(10)
                    << (item.type == WebHDFS::FileStatus::PathObjectType::FILE ? "file" : "dir")
                    << std::setw(20) << item.owner << std::setw(20)
                    << boost::posix_time::from_time_t(item.modificationTime / 1000);
                cout << oss.str() << '\n';
            }
        }
        else
        {
            throwWrongRemotePathFormat("ls");
        }
    }
    else if (argc == 3 && argv[1] == std::string("mkdir"))
    {
        std::string target(argv[2]);
        if (parseRemotePath(target, remoteHost, remotePath))
        {
            log_info("Creating", target, "directory ...");
            WebHDFS::Client client(remoteHost, clientOptions);
            client.makeDir(remotePath);
        }
        else
        {
            throwWrongRemotePathFormat("mkdir");
        }
    }
    else if (argc == 4 && argv[1] == std::string("rename"))
    {
        const std::string from(argv[2]);
        const std::string to(argv[3]);

        if (parseRemotePath(from, remoteHost, remotePath))
        {
            // remote to local
            log_info("Renaming", from, "to", to, "...");
            WebHDFS::Client client(remoteHost, clientOptions);
            client.rename(remotePath, to);
        }
        else
        {
            throwWrongRemotePathFormat("rename");
        }
    }
    else
    {
        std::string app = argv[0];
        auto sepPos = app.rfind('/');
        if (sepPos != std::string::npos)
        {
            app = app.substr(sepPos + 1);
        }
        std::cerr << "*** WebHDFS client demo ***\n"
                  << "Usage: " << app << " COMMAND OPTIONS\n\t"
                  << app << " cat <hdfs path>\n\t"
                  << app << " cp <local file> <hdfs file path>\n\t"
                  << app << " cp <hdfs file path> <local file>\n\t"
                  << app << " rm <hdfs path>\n\t"
                  << app << " ls <hdfs dir path>\n\t"
                  << app << " rename <hdfs path> <new path>\n"
                  << "Example:\n\t"
                  << app << " cat hdfs://hd0-dev/tmp/webhdfs-test.txt\n";
        return 1;
    }
    log_info("Done");
    return 0;
}
catch (const std::runtime_error &ex)
{
    log_err("Exception:", ex.what());
}
