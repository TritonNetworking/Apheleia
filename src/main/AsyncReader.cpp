#include "AsyncReader.h"
#include <iostream>
#include <memory>

AsyncReader::AsyncReader(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd)
        : m_io(io), num_record(num_record), tcp_socket(sock_ptr), node_status(rcv_or_snd){
    _logger = spdlog::get("async_reader_logger");
}

AsyncReader::~AsyncReader() {
        m_ReaderThread.join();
        std::cout<< "ReaderThread join:" <<"\n";
        _logger->info("read thread joins");
}

void AsyncReader::threadJoin(){
    if(m_ReaderThread.joinable())
        m_ReaderThread.join();  
}

void AsyncReader::submitRead(async_file_reader_node::gateway_type& gateway) {
        gateway.reserve_wait();
        if(node_status == 0){
            std::thread(&AsyncReader::readingLoop, this, std::ref(gateway)).swap(m_ReaderThread);
            _logger->info("reader thread starts");
        }
        else if(node_status == 1){
            std::thread(&AsyncReader::receiveLoop, this, std::ref(gateway)).swap(m_ReaderThread);
            _logger->info("receiver thread starts");
        }
        std::cout << "read thread id:" << std::this_thread::get_id() << "\n";
}

void AsyncReader::setNetworkConfig(std::string ip, std::string port){
        server_IP=ip;
        server_port=port;
}

void AsyncReader::setRecvConfig(uint32_t num, uint32_t iter){
    num_host = num;
    num_iter = iter;
    int64_t rounds = (int64_t) num_iter;
    int64_t sizes =(int64_t) num_record*record_size;
    total_expected_bytes = rounds * sizes;
    std::cout << "total expected bytes:" <<  +total_expected_bytes << "\n";
}


/*std::vector<uint64_t> AsyncReader::getRecvStats(){
    return recv_stats;
}

std::vector<uint64_t> AsyncReader::getReadStats(){
    return read_stats;
}*/



void AsyncReader::readingLoop(async_file_reader_node::gateway_type& gateway) {
       std::cout << "reading loop num_record:" <<  +num_record << "\n";
       int loop_counter = 0;
       struct timespec ts1,ts2, ts3;
       uint64_t read_start, read_end, gateway_end;
       int iters = (int) m_io.getReadmIters();
        //while (m_io.hasDataToRead()) {
       _logger->info("read loop starts");
       std::cout << "read iters:" << iters << "\n";
        for(int i = 0; i< iters; i++){
            read_start = AsyncReader::getNanoSecond(ts1);
            BufferMsg bufferMsg = BufferMsg::createBufferMsg(m_io.chunksRead(), m_io.chunkSize());
            m_io.readChunk(bufferMsg.inputBuffer);
            read_end = AsyncReader::getNanoSecond(ts2);
            _logger->info("read_time {0:d}", read_end-read_start);
            gateway.try_put(bufferMsg);
            gateway_end = AsyncReader::getNanoSecond(ts3);
            _logger->info("gateway_time {0:d}", gateway_end-read_end);
            loop_counter++;
        }
        _logger->info("read loop ends");
        std::cout << "read loop counter:" <<  loop_counter << "\n";
        sendLastMessage(gateway);
        gateway.release_wait();
        _logger->info("read ends");
}

void AsyncReader::receiveLoop(async_file_reader_node::gateway_type& gateway){
        int64_t recv_bytes = 0;
        int64_t total_bytes=0;
        struct timespec ts1,ts2, ts3, ts4;
        uint64_t recv_start, recv_end, t_start, t_end;
        int64_t perhost_bytes = total_expected_bytes/num_host;
        std::shared_ptr<spdlog::logger> sock_logger = spdlog::get("socket_logger");
        tcp_socket->tcpListen(server_port, backlogSize);
        _logger->info("recv thread starts");
        t_start = AsyncReader::getNanoSecond(ts3);
        while (total_bytes < total_expected_bytes){ //while (total_bytes < 64000000) {
            Socket* new_socket = tcp_socket->tcpAccept(timeoutInMicros, m_io.chunkSize());
            int64_t loop_bytes = 0;

            sock_logger->info("sock_start");
            std::cout << "socket bytes:" << new_socket->getSocketBytes() << "\n";

            while (loop_bytes < perhost_bytes){ //while (loop_bytes < 16000000) {
                BufferMsg bufferMsg = BufferMsg::createBufferMsg(m_io.chunksRead(), m_io.chunkSize());
                recv_start = AsyncReader::getNanoSecond(ts1);
                recv_bytes= new_socket->tcpReceive(bufferMsg.inputBuffer.b, m_io.chunkSize());
                recv_end = AsyncReader::getNanoSecond(ts2);
                total_bytes += recv_bytes;
                loop_bytes  += recv_bytes;
                uint64_t recv_time= recv_end - recv_start;
                //std::cout << "recv bytes:" << total_bytes << "\n";
                //_logger->info("recv bytes {0:d} recv time {1:d}", recv_bytes, recv_time); 
                _logger->info("recv_time {0:d}", recv_time); 
                bufferMsg.inputBuffer.len = (size_t) recv_bytes;
                m_io.incrementChunks();
                gateway.try_put(bufferMsg);
                recv_stats.push_back(recv_time);
            }
            std::cout << "per loop bytes:" << loop_bytes << "\n";
            new_socket->tcpClose();

            sock_logger->info("sock_close");

            std::cout << "client addr" << new_socket->getAddress() << "\n";
            std::cout << "recv socket close" << new_socket << "\n";
        }
        t_end = AsyncReader::getNanoSecond(ts4);
        _logger->info("recv thread ends ");
        _logger->info("recv_thread time {0:d} ", t_end - t_start);

        std::cout << "total recv bytes:" << total_bytes << "\n";
        sendLastMessage(gateway);
        gateway.release_wait();
        _logger->info("recv ends ");
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