#include "AsyncReader.h"
#include <iostream>

AsyncReader::AsyncReader(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd)
        : m_io(io), m_num_record(num_record), tcp_socket(sock_ptr), node_status(rcv_or_snd){}

AsyncReader::~AsyncReader() {
        m_ReaderThread.join();
        std::cout<< "ReaderThread join:" <<"\n";
}

void AsyncReader::threadJoin(){
    if(m_ReaderThread.joinable())
        m_ReaderThread.join();  
}

void AsyncReader::submitRead(async_file_reader_node::gateway_type& gateway) {
        gateway.reserve_wait();
        if(node_status == 0){
            std::thread(&AsyncReader::readingLoop, this, std::ref(gateway)).swap(m_ReaderThread);
        }
        else if(node_status == 1){
            std::thread(&AsyncReader::receiveLoop, this, std::ref(gateway)).swap(m_ReaderThread);
        }
        std::cout << "read thread id:" << std::this_thread::get_id() << "\n";
}

void AsyncReader::setNetworkConfig(std::string ip, std::string port){
        server_IP=ip;
        server_port=port;
}


void AsyncReader::readingLoop(async_file_reader_node::gateway_type& gateway) {
       std::cout << "reading loop num_record:" <<  m_num_record << "\n";
       int loop_counter = 0;
       int iters = (int) m_io.getReadmIters();
        //while (m_io.hasDataToRead()) {
       std::cout << "read iters:" << iters << "\n";
        for(int i = 0; i< iters; i++){
            BufferMsg bufferMsg = BufferMsg::createBufferMsg(m_io.chunksRead(), m_io.chunkSize());
            m_io.readChunk(bufferMsg.inputBuffer);
            gateway.try_put(bufferMsg);
            loop_counter++;
        }
        std::cout << "read loop counter:" <<  loop_counter << "\n";
        sendLastMessage(gateway);
        gateway.release_wait();
}

void AsyncReader::receiveLoop(async_file_reader_node::gateway_type& gateway){
        int64_t recv_bytes = 0;
        int64_t total_bytes=0;
        tcp_socket->tcpListen(server_port, backlogSize);
        while (total_bytes < 64000000) {
            Socket* new_socket = tcp_socket->tcpAccept(timeoutInMicros, m_io.chunkSize());
            int64_t loop_bytes = 0;
            while (loop_bytes < 16000000) {
                BufferMsg bufferMsg = BufferMsg::createBufferMsg(m_io.chunksRead(), m_io.chunkSize());
                recv_bytes= new_socket->tcpReceive(bufferMsg.inputBuffer.b, m_io.chunkSize());
                total_bytes += recv_bytes;
                loop_bytes  += recv_bytes;
                std::cout << "recv bytes:" << total_bytes << "\n";
                bufferMsg.inputBuffer.len = (size_t) recv_bytes;
                m_io.incrementChunks();
                gateway.try_put(bufferMsg);
            }
            std::cout << "per loop bytes:" << loop_bytes << "\n";
            new_socket->tcpClose();
            std::cout << "client addr" << new_socket->getAddress() << "\n";
            std::cout << "recv socket close" << new_socket << "\n";
        }

        std::cout << "total recv bytes:" << total_bytes << "\n";
        sendLastMessage(gateway);
        gateway.release_wait();
}

void AsyncReader::sendLastMessage(async_file_reader_node::gateway_type& gateway) {
        BufferMsg lastMsg;
        lastMsg.markLast(m_io.chunksRead());
        lastMsg.inputBuffer.b=NULL;
        lastMsg.inputBuffer.len=0;
        lastMsg.outputBuffer.b=NULL;
        lastMsg.outputBuffer.len=0;
        //lastMsg.basebuf=nullptr;
        gateway.try_put(lastMsg);
}
    /*
    IOOperations& m_io;
    uint32_t m_num_record;
    int node_status; //-1= error 0=send 1= recv
    std::thread m_ReaderThread;

    // Networking configs
    uint64_t timeoutInMicros= 1000*1000*60;
    int backlogSize=10;    
    Socket* tcp_socket;
    std::string server_IP="127.0.0.1";
    std::string server_port="80";*/