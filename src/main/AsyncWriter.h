#ifndef ASYNC_WRITE_H
#define ASYNC_WRITE_H

#include <string>
#include "IOOperation.h"
#include "Socket.h"
#include "BufferMsg.h"
#include "tbb/flow_graph.h"
#include "tbb/compat/thread"
#include "tbb/concurrent_queue.h"
#include <cstdint>

typedef tbb::flow::async_node< BufferMsg, tbb::flow::continue_msg > async_file_writer_node;

class AsyncWriter {
public:
    AsyncWriter(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd);
    ~AsyncWriter(); 
    
    void startWriterThread();

    void submitWrite(const BufferMsg& bufferMsg);

    void setNetworkConfig(std::string ip, std::string port);

private:
    void writingLoop();

    void sendLoop();

    IOOperations& m_io;
    int count=0;
    uint32_t m_num_record;
    int node_status; //-1= error 0=send 1= recv

    uint64_t Retry=5;
    uint64_t retryDelayInMicros=1000;
    Socket* tcp_socket;
    std::string server_IP;
    std::string server_port;

    tbb::concurrent_bounded_queue<BufferMsg> m_writeQueue;
    std::thread m_WriterThread;
};
#endif //NET_WORKER_H
