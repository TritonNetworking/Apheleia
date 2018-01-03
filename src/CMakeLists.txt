cmake_minimum_required(VERSION 2.8)

project(Apheleia)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

enable_testing()

add_subdirectory(proto)
add_subdirectory(main)
add_subdirectory(test)
