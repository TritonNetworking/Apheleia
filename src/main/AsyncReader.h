#ifndef ASYNC_READ_H
#define ASYNC_READ_H

#include <string>
#include "IOOperation.h"
#include "Socket.h"
#include "BufferMsg.h"
#include "tbb/flow_graph.h"
#include "tbb/compat/thread"
#include <cstdint>

typedef tbb::flow::async_node< tbb::flow::continue_msg, BufferMsg > async_file_reader_node;
//typedef tbb::flow::async_node< BufferMsg, tbb::flow::continue_msg > async_file_writer_node;

class AsyncReader {
public:

    AsyncReader(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd);

    ~AsyncReader();

    void threadJoin();

    void submitRead(async_file_reader_node::gateway_type& gateway);

    void setNetworkConfig(std::string ip, std::string port);

private:

    void readingLoop(async_file_reader_node::gateway_type& gateway);

    void receiveLoop(async_file_reader_node::gateway_type& gateway);

    void sendLastMessage(async_file_reader_node::gateway_type& gateway);

    IOOperations& m_io;
    uint32_t m_num_record;
    int node_status; //-1= error 0=send 1= recv
    std::thread m_ReaderThread;

    // Networking configs
    uint64_t timeoutInMicros= 1000*1000*60;
    int backlogSize=10;    
    Socket* tcp_socket;
    std::string server_IP="127.0.0.1";
    std::string server_port="80";
};

#endif