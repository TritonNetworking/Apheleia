#ifndef ASYNC_READ_H
#define ASYNC_READ_H

#include <string>
#include "IOOperation.h"
#include "Socket.h"
#include "BufferMsg.h"
#include "tbb/flow_graph.h"
#include "tbb/compat/thread"
#include <cstdint>
#include <time.h>
#include <vector>
#include "spdlog/spdlog.h"

typedef tbb::flow::async_node< tbb::flow::continue_msg, BufferMsg > async_file_reader_node;
//typedef tbb::flow::async_node< BufferMsg, tbb::flow::continue_msg > async_file_writer_node;

class AsyncReader {
public:

    AsyncReader(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd);

    ~AsyncReader();

    void threadJoin();

    void submitRead(async_file_reader_node::gateway_type& gateway);

    void setNetworkConfig(std::string ip, std::string port);

    void setRecvConfig(uint32_t num, uint32_t iter);
    std::vector<uint64_t> getRecvStats();
    std::vector<uint64_t> getReadStats();
    inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
    }


private:

    void readingLoop(async_file_reader_node::gateway_type& gateway);

    void receiveLoop(async_file_reader_node::gateway_type& gateway);

    void sendLastMessage(async_file_reader_node::gateway_type& gateway);

    IOOperations& m_io;
    uint32_t num_record;
    uint32_t num_iter;
    uint32_t num_host;
    int64_t total_expected_bytes;
    uint32_t record_size=100;
    int node_status; //-1= error 0=send 1= recv
    std::thread m_ReaderThread;
    //std::thread m_ReaderThread0;
    std::vector<uint64_t> recv_stats;
    std::vector<uint64_t> read_stats;
    std::shared_ptr<spdlog::logger> _logger;

    // Networking configs
    uint64_t timeoutInMicros= 1000*1000*60;
    int backlogSize=10;    
    Socket* tcp_socket;
    std::string server_IP="127.0.0.1";
    std::string server_port="80";
    //std::string server_port1="81";
};

#endif