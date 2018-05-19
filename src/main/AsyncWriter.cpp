#include "AsyncWriter.h"
#include <iostream>
#include <memory>

AsyncWriter::AsyncWriter(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd)
        : m_io(io), m_num_record(num_record), tcp_socket(sock_ptr), node_status(rcv_or_snd){
    _logger = spdlog::get("async_writer_logger");
}

AsyncWriter::~AsyncWriter() {
        m_WriterThread.join();
        std::cout<< "WriterThread join" << "\n";
        _logger->info("writer thread joins");
}

void AsyncWriter::startWriterThread(){
        if(node_status == 0){
            //std::cout<< "sendLoop" << "\n";
            m_WriterThread= std::thread(&AsyncWriter::sendLoop, this);
            _logger->info("send thread starts");
        }
        else if(node_status == 1){
            //std::cout<< "writingLoop" << "\n";
            m_WriterThread= std::thread(&AsyncWriter::writingLoop, this);
            _logger->info("write thread starts");
        }
        else{
            std::cerr << "Network Thread Error: unknown node status\n";
        }
        std::cout << "write thread id:" << m_WriterThread.get_id() << "\n";
}

void AsyncWriter::submitWrite(const BufferMsg& bufferMsg) {
        m_writeQueue.push(bufferMsg);
}

void AsyncWriter::setNetworkConfig(std::string ip, std::string port){
        server_IP=ip;
        server_port=port;
}

void AsyncWriter::writingLoop() {
    int loop_counter = 0;
    struct timespec ts1,ts2, ts3;
    uint64_t write_start, write_end, queue_end;
    BufferMsg buffer;
    _logger->info("write loop starts");
    m_writeQueue.pop(buffer);
    while (!buffer.isLast) {
        write_start = AsyncWriter::getNanoSecond(ts1);
        m_io.writeChunk(buffer.outputBuffer);
        write_end = AsyncWriter::getNanoSecond(ts2);
        _logger->info("write_time  {0:d}", write_end-write_start);
        write_end = AsyncWriter::getNanoSecond(ts2);
        m_writeQueue.pop(buffer);
        queue_end = AsyncWriter::getNanoSecond(ts3);
        _logger->info("write_queue {0:d}", queue_end-write_end);
        loop_counter++;
    }
    _logger->info("write loop ends");
    std::cout << "write loop counter:" <<  loop_counter << "\n";
}

void AsyncWriter::sendLoop(){
        BufferMsg buffer;
        struct timespec ts1,ts2, ts3, ts4;
        uint64_t write_start, write_end, queue_end;
        _logger->info("send loop starts");
        m_writeQueue.pop(buffer); //a waiting pop operation that waits until it can pop an item
        _logger->info("send queue pop");
        std::cout << "this_thread, thread id:" << std::this_thread::get_id() << "\n";
        tcp_socket->tcpConnect(server_IP, server_port, buffer.outputBuffer.len, retryDelayInMicros, Retry);
        int64_t total_bytes = 0;
        while(!buffer.isLast){
            write_start = AsyncWriter::getNanoSecond(ts1);
            int64_t sent_bytes = tcp_socket->tcpSend(buffer.outputBuffer.b, buffer.outputBuffer.len);
            if(sent_bytes <= 0){
               std::cerr << "Network Thread Error: tcp send error\n"; 
            }
            total_bytes += sent_bytes;
            write_end = AsyncWriter::getNanoSecond(ts2);
            //std::cout << "total send bytes:" << total_bytes << "\n";
            _logger->info("send_time  {0:d}", write_end - write_start); 
            m_writeQueue.pop(buffer);
            queue_end = AsyncWriter::getNanoSecond(ts3);
            _logger->info("send_queue {0:d}", queue_end - write_end); 
        }
        std::cout <<"send ends, server ip"<< server_IP;
        std::cout <<", server port"<< server_port << "\n";
        std::cout << "total send bytes:" << total_bytes << "\n";
        tcp_socket->tcpClose();
        _logger->info("send loop ends");   
}

