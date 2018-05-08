#cmake_minimum_required(VERSION 3.5)
#file(GLOB SRCS *.cpp)

set(SRC_DIR ${CMAKE_CURRENT_LIST_DIR}/../../main)
MESSAGE( STATUS "wamos SRC_DIR=" ${SRC_DIR})

include_directories(${SRC_DIR})
#ADD_EXECUTABLE(testsockets ${SRCS})
add_executable(testsockets main.cpp ${SRC_DIR}/BaseBuffer.cpp ${SRC_DIR}/KeyValueRecord.cpp ${SRC_DIR}/BufferSorter.cpp ${SRC_DIR}/BucketTable.cpp)


TARGET_LINK_LIBRARIES(testsockets
    libgtest
    libgmock
)

add_test(NAME testsockets
         COMMAND testsockets)
