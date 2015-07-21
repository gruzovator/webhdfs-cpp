################################################################################
# REQUIRED LIBS:
# * boost
# * curl
# * jsoncpp
################################################################################

cmake_minimum_required(VERSION 2.8)
project(webhdfs-cpp)

set(FLAGS "-W -Wall -std=c++11")
set(CMAKE_CXX_FLAGS "${FLAGS}")
set(CMAKE_CXX_FLAGS "${FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

option(BUILD_DEMO_APP "Set to ON to build demo application." ON)

# WenHDFS client lib
include_directories(lib/include)
add_library(webhdfs lib/include/WebHdfsClient.h lib/src/WebHdfsClient.cpp )

# DEMO APP
if(BUILD_DEMO_APP)
    SET(DEMO_APP webhdfs-client)
    add_executable(${DEMO_APP} demo-app/utils.h demo-app/main.cpp)
    target_link_libraries(${DEMO_APP} webhdfs curl jsoncpp boost_regex)
endif()

