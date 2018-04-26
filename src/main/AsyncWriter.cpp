#include "AsyncWriter.h"
#include <iostream>

AsyncWriter::AsyncWriter(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd)
        : m_io(io), m_num_record(num_record), tcp_socket(sock_ptr), node_status(rcv_or_snd){}

AsyncWriter::~AsyncWriter() {
        m_WriterThread.join();
        std::cout<< "WriterThread join" << "\n";
}

void AsyncWriter::startWriterThread(){
        if(node_status == 0){
            std::cout<< "sendLoop" << "\n";
            m_WriterThread= std::thread(&AsyncWriter::sendLoop, this);
        }
        else if(node_status == 1){
            std::cout<< "writingLoop" << "\n";
            m_WriterThread= std::thread(&AsyncWriter::writingLoop, this);
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
        BufferMsg buffer;
        m_writeQueue.pop(buffer);
        while (!buffer.isLast) {
            m_io.writeChunk(buffer.outputBuffer);
            m_writeQueue.pop(buffer);
        }
}

void AsyncWriter::sendLoop(){
        BufferMsg buffer;
        m_writeQueue.pop(buffer); //a waiting pop operation that waits until it can pop an item
        std::cout << "this_thread, thread id:" << std::this_thread::get_id() << "\n";
        tcp_socket->tcpConnect(server_IP, server_port, buffer.outputBuffer.len, retryDelayInMicros, Retry);
        int64_t total_bytes = 0;
        while(!buffer.isLast){
            int64_t sent_bytes = tcp_socket->tcpSend(buffer.outputBuffer.b, buffer.outputBuffer.len);
            if(sent_bytes <= 0){
               std::cerr << "Network Thread Error: tcp send error\n"; 
            }
            total_bytes += sent_bytes;
            //std::cout << "total send bytes:" << total_bytes << "\n";
            m_writeQueue.pop(buffer);
        }
        std::cout <<"send ends, server ip"<< server_IP;
        std::cout <<", server port"<< server_port << "\n";
        std::cout << "total send bytes:" << total_bytes << "\n";
        tcp_socket->tcpClose();   
}

