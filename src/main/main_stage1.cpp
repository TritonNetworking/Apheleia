#include "../utility/utility.h"
//if __TBB_PREVIEW_ASYNC_MSG && __TBB_CPP11_LAMBDAS_PRESENT

#include <iostream>
#include <fstream>
#include <string>
#include <memory>
#include <queue>
#include <cstdint>
#include <functional>
//#include <stdint.h>
//#include <inttypes.h>
#include "tbb/flow_graph.h"
#include "tbb/tick_count.h"
#include "tbb/compat/thread"
#include "tbb/concurrent_queue.h"

#include "KeyValueRecord.h"
#include "BucketTable.h"
#include "BaseBuffer.h"
#include "BufferMsg.h"
//#include "BufferSorter.h"
#include "Socket.h"
#include "IOOperation.h"
#include "MapperOperation.h"
#include "json.hpp"
#include <vector>

typedef tbb::flow::async_node< tbb::flow::continue_msg, BufferMsg > async_file_reader_node;
typedef tbb::flow::async_node< BufferMsg, tbb::flow::continue_msg > async_file_writer_node;
using json = nlohmann::json;

class AsyncNodeActivity {
public:

    AsyncNodeActivity(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd)
        : m_io(io), m_num_record(num_record), tcp_socket(sock_ptr), node_status(rcv_or_snd){}

    ~AsyncNodeActivity() {
        m_ReaderThread.join();
        std::cout<< "fileReaderThread join:" <<  count <<"\n";
        count++;
        m_WriterThread.join();
        std::cout<< "BufferWriterThread join" << "\n";
    }

    void startWriterThread(){
        if(node_status == 0){
            m_WriterThread= std::thread(&AsyncNodeActivity::sendLoop, this);
        }
        else if(node_status == 1){
            m_WriterThread= std::thread(&AsyncNodeActivity::writingLoop, this);
        }
        else{
            std::cerr << "Network Thread Error: unknown node status\n";
        }
    }

    void submitRead(async_file_reader_node::gateway_type& gateway) {
        gateway.reserve_wait();
        if(node_status == 0){
            std::thread(&AsyncNodeActivity::readingLoop, this, std::ref(gateway)).swap(m_ReaderThread);
        }
        else if(node_status == 1){
            std::thread(&AsyncNodeActivity::receiveLoop, this, std::ref(gateway)).swap(m_ReaderThread);
        }
    }

    void submitWrite(const BufferMsg& bufferMsg) {
        m_writeQueue.push(bufferMsg);
    }

    void setNetworkConfig(std::string ip, std::string port){
        server_IP=ip;
        server_port=port;
    }

private:

    void readingLoop(async_file_reader_node::gateway_type& gateway) {
        while (m_io.hasDataToRead()) {
            BufferMsg bufferMsg = BufferMsg::createBufferMsg(m_io.chunksRead(), m_io.chunkSize(), m_num_record);
            m_io.readChunk(bufferMsg.inputBuffer);
            gateway.try_put(bufferMsg);
        }
        sendLastMessage(gateway);
        gateway.release_wait();
    }

    void writingLoop() {
        BufferMsg buffer;
        m_writeQueue.pop(buffer);
        while (!buffer.isLast) {
            m_io.writeChunk(buffer.outputBuffer);
            m_writeQueue.pop(buffer);
        }
    }

    void sendLoop(){
        BufferMsg buffer;
        m_writeQueue.pop(buffer); //a waiting pop operation that waits until it can pop an item
        tcp_socket->tcpConnect(server_IP, server_port, buffer.outputBuffer.len, retryDelayInMicros, Retry);
        uint64_t total_bytes = 0;
        while(!buffer.isLast){
            uint64_t sent_bytes=tcp_socket->tcpSend(buffer.outputBuffer.b, buffer.outputBuffer.len);
            if(sent_bytes == 0){
               std::cerr << "Network Thread Error: tcp send error\n"; 
            }
            total_bytes += sent_bytes;
            std::cout << total_bytes << "\n";
            m_writeQueue.pop(buffer);
        }
        tcp_socket->tcpClose();   
    }

    void receiveLoop(async_file_reader_node::gateway_type& gateway){
        tcp_socket->tcpListen(server_port, backlogSize);
        Socket* new_socket=tcp_socket->tcpAccept(timeoutInMicros, m_io.chunkSize());
        uint64_t recv_bytes = 1;
        while (recv_bytes > 0) {
            BufferMsg bufferMsg = BufferMsg::createBufferMsg(m_io.chunksRead(), m_io.chunkSize(), m_num_record);
            uint64_t recv_bytes= new_socket->tcpReceive(bufferMsg.inputBuffer.b, m_io.chunkSize());
            bufferMsg.inputBuffer.len = (size_t) recv_bytes;
            m_io.incrementChunks();
            gateway.try_put(bufferMsg);
        }
        sendLastMessage(gateway);
        gateway.release_wait();
        tcp_socket->tcpClose();
    }

    void sendLastMessage(async_file_reader_node::gateway_type& gateway) {
        BufferMsg lastMsg;
        lastMsg.markLast(m_io.chunksRead());
        lastMsg.inputBuffer.b=NULL;
        lastMsg.inputBuffer.len=0;
        lastMsg.outputBuffer.b=NULL;
        lastMsg.outputBuffer.len=0;
        lastMsg.basebuf=nullptr;
        gateway.try_put(lastMsg);
    }

    IOOperations& m_io;
    int count=0;
    uint32_t m_num_record;
    int node_status; //-1= error 0=send 1= recv

    uint64_t timeoutInMicros= 1000*1000*60;
    int backlogSize=10;    
    uint64_t Retry=5;
    uint64_t retryDelayInMicros=1000;
    Socket* tcp_socket;
    std::string server_IP;
    std::string server_port;

    tbb::concurrent_bounded_queue<BufferMsg> m_writeQueue;

    std::thread m_ReaderThread;
    std::thread m_WriterThread;
    //std::thread m_networkThread;
};

//void fgCompressionAsyncNode( IOOperations& io_0, IOOperations& io_1, IOOperations& io_2, IOOperations& io_3, 
/*void fgCompressionAsyncNode( IOOperations& io_0, uint32_t num_rcrd, int sort_type, int rcv_or_snd) {

    tbb::flow::graph g;

    Socket* sock=new Socket();
    //AsyncNodeActivity(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd)
    AsyncNodeActivity asyncNodeActivity_0(io_0, num_rcrd, sock, rcv_or_snd);
    asyncNodeActivity_0.startNetworkThread();
    //AsyncNodeActivity asyncNodeActivity_1(io_1, num_rcrd);
    //AsyncNodeActivity asyncNodeActivity_2(io_2, num_rcrd);
    //AsyncNodeActivity asyncNodeActivity_3(io_3, num_rcrd);

    //async_file_reader_node file_reader(g, tbb::flow::unlimited, [&asyncNodeActivity](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
     //   asyncNodeActivity.submitRead(gateway);
    //});

    async_file_reader_node file_reader_0(g, 1, [&asyncNodeActivity_0](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
        asyncNodeActivity_0.submitRead(gateway);
    });

    async_file_reader_node file_reader_1(g, 1, [&asyncNodeActivity_1](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
        asyncNodeActivity_1.submitRead(gateway);
    });
    async_file_reader_node file_reader_2(g, 1, [&asyncNodeActivity_2](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
        asyncNodeActivity_2.submitRead(gateway);
    });

    async_file_reader_node file_reader_3(g, 1, [&asyncNodeActivity_3](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
        asyncNodeActivity_3.submitRead(gateway);
    });

    //tbb::flow::function_node< BufferMsg, BufferMsg > compressor(g, tbb::flow::unlimited, BufferCompressor(num_rcrd, sort_type));
    //tbb::flow::function_node< BufferMsg, BufferMsg > compressor(g, 40, BufferCompressor(num_rcrd, sort_type));
    Mapper mapper_0(g, 1, MapperOperation(num_rcrd));
    //Mapper mapper_1(g, 1, MapperOperation(num_rcrd));
    //Mapper mapper_2(g, 1, MapperOperation(num_rcrd));
    //Mapper mapper_3(g, 1, MapperOperation(num_rcrd));

    //tbb::flow::function_node<BufferMsg, BufferMsg > sorter_0(g, 4, BufferSorter(num_rcrd, sort_type));
    //tbb::flow::function_node<BufferMsg, BufferMsg > sorter_1(g, 4, BufferSorter(num_rcrd, sort_type));
    //tbb::flow::function_node<BufferMsg, BufferMsg > sorter_2(g, 4, BufferSorter(num_rcrd, sort_type));
    //tbb::flow::function_node<BufferMsg, BufferMsg > sorter_3(g, 4, BufferSorter(num_rcrd, sort_type));

    tbb::flow::sequencer_node< BufferMsg > ordering_0 (g, [](const BufferMsg& bufferMsg)->size_t {
        return bufferMsg.seqId;
    });
    tbb::flow::sequencer_node< BufferMsg > ordering_1 (g, [](const BufferMsg& bufferMsg)->size_t {
        return bufferMsg.seqId;
    });
    tbb::flow::sequencer_node< BufferMsg > ordering_2 (g, [](const BufferMsg& bufferMsg)->size_t {
        return bufferMsg.seqId;
    });
    tbb::flow::sequencer_node< BufferMsg > ordering_3 (g, [](const BufferMsg& bufferMsg)->size_t {
        return bufferMsg.seqId;
    });

    // The node is serial to preserve the right order of buffers set by the preceding sequencer_node
    async_file_writer_node output_writer_0(g, tbb::flow::serial, [&asyncNodeActivity_0](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
        asyncNodeActivity_0.submitWrite(bufferMsg);
        //std::cout << "fw0 " << "\n";
    });
    async_file_writer_node output_writer_1(g, tbb::flow::serial, [&asyncNodeActivity_1](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
        asyncNodeActivity_1.submitWrite(bufferMsg);
        //std::cout << "fw1 " << "\n";
    });

    
    async_file_writer_node output_writer_2(g, tbb::flow::serial, [&asyncNodeActivity_2](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
        asyncNodeActivity_2.submitWrite(bufferMsg);
        //std::cout << "fw2 " << "\n";
    });

    async_file_writer_node output_writer_3(g, tbb::flow::serial, [&asyncNodeActivity_3](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
        asyncNodeActivity_3.submitWrite(bufferMsg);
        //std::cout << "fw3 " << "\n";
    });

    make_edge(file_reader_0, mapper_0);
    make_edge(file_reader_1, mapper_1);
    make_edge(file_reader_2, mapper_2);
    make_edge(file_reader_3, mapper_3);

    make_edge(tbb::flow::output_port<0>(mapper_0), output_writer_0);
    make_edge(tbb::flow::output_port<1>(mapper_0), output_writer_1);
    make_edge(tbb::flow::output_port<2>(mapper_0), output_writer_2);
    make_edge(tbb::flow::output_port<3>(mapper_0), output_writer_3);

    make_edge(tbb::flow::output_port<0>(mapper_1), output_writer_0);
    make_edge(tbb::flow::output_port<1>(mapper_1), output_writer_1);
    make_edge(tbb::flow::output_port<2>(mapper_1), output_writer_2);
    make_edge(tbb::flow::output_port<3>(mapper_1), output_writer_3);

    make_edge(tbb::flow::output_port<0>(mapper_2), output_writer_0);
    make_edge(tbb::flow::output_port<1>(mapper_2), output_writer_1);
    make_edge(tbb::flow::output_port<2>(mapper_2), output_writer_2);
    make_edge(tbb::flow::output_port<3>(mapper_2), output_writer_3);

    make_edge(tbb::flow::output_port<0>(mapper_3), output_writer_0);
    make_edge(tbb::flow::output_port<1>(mapper_3), output_writer_1);
    make_edge(tbb::flow::output_port<2>(mapper_3), output_writer_2);
    make_edge(tbb::flow::output_port<3>(mapper_3), output_writer_3);

    file_reader_0.try_put(tbb::flow::continue_msg());
    //file_reader_1.try_put(tbb::flow::continue_msg());
    //file_reader_2.try_put(tbb::flow::continue_msg());
    //file_reader_3.try_put(tbb::flow::continue_msg());

    g.wait_for_all();
}
*/

uint64_t GetNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
}

int main(int argc, char* argv[]) {
    try {
        tbb::tick_count mainStartTime = tbb::tick_count::now();

        const std::string archiveExtension = ".out";
        bool verbose = true;
        uint32_t iteration;
        int sort_type;
        int node_status = 1;
        std::string self_IP = "127.0.0.1";
        //std::string server_port = "8000";
        json config_json;
        //asyncType == "async_node";
        std::string inputFileName;
        std::string configFileName;
        int blockSizeIn1000KB = 1; // block size in 100KB chunks
        uint32_t Kbytes =1;
        size_t memoryLimitIn1MB = 1; // memory limit for compression in megabytes granularity

        utility::parse_cli_arguments(argc, argv,
            utility::cli_argument_pack()
            //"-h" option for displaying help is present implicitly
            .arg(Kbytes, "-b", "\t buffer size in KB")
            .arg(sort_type, "-s", "\t numbers of iterations that our sort run")
            .arg(node_status, "-n", "the node is either sender or receiver")
            .arg(self_IP, "-ip", "the node is either sender or receiver")
            //.arg(server_port, "-port", "the node is either sender or receiver")
            //.arg(verbose, "-v", "verbose mode")
            //.arg(memoryLimitIn1MB, "-l", "used memory limit for compression algorithm in 1MB (minimum) granularity")
            //.arg(asyncType, "-a", "name of the used graph async implementation - can be async_node or async_msg")
            //.positional_arg(server_port, "server_port", "server port")
            //.positional_arg(server_IP, "server_ip", "server ip addr")
            //.positional_arg(inputFileName, "input_filename", "input file name")
            .positional_arg(configFileName, "config_filename", "config file name")
        );

        uint32_t keysize = 10;
        uint32_t valuesize = 90;
        uint32_t num_record = 1000*Kbytes/(keysize+valuesize);

        std::cout << Kbytes << "\n";

        if (verbose) std::cout << "Input file name: " << inputFileName << std::endl;    

        std::ifstream inputStream(inputFileName.c_str(), std::ios::in | std::ios::binary);
        //std::ifstream inputStream1(inputFileName.c_str(), std::ios::in | std::ios::binary);
        //std::ifstream inputStream2(inputFileName.c_str(), std::ios::in | std::ios::binary);
        //std::ifstream inputStream3(inputFileName.c_str(), std::ios::in | std::ios::binary);

        if (!inputStream.is_open()) {
            throw std::invalid_argument("Cannot open " + inputFileName + " file.");
        }

        //std::string outputFileName(inputFileName + archiveExtension);
        std::string outputname[4];
        for(int i=0; i < 4; i++){
            std::string outputFileName(inputFileName + "_" + std::to_string(i)+ archiveExtension);
            outputname[i] = outputFileName;
        }

        std::ofstream outputStream0(outputname[0].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);    
        std::ofstream outputStream1(outputname[1].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);    
        //std::ofstream outputStream2(outputname[2].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);    
        //std::ofstream outputStream3(outputname[3].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);   
        //std::ofstream outputStream(outputFileName.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
        //if (!outputStream.is_open()) {
        //    throw std::invalid_argument("Cannot open " + outputFileName + " file.");
        //}

        std::ifstream configStream(configFileName.c_str(), std::ios::in | std::ios::binary);
        configStream >> config_json;
        for (json::iterator it = config_json["hosts"].begin(); it != config_json["hosts"].end(); ++it) {
             json j= *it;
             std::cout << j << "\n";
             for (json::iterator itr = j.begin(); itr != j.end(); ++itr) {
                //std::cout << itr.key() << '\n';
                std::string key = itr.key();
                std::cout << key<< '\n';
                std::string val = itr.value();
                std::cout << val<< '\n';
            }
        }

        // General interface to work with I/O buffers operations
        size_t chunkSize = Kbytes * 1000;
        IOOperations io_0(inputStream, outputStream0, chunkSize);
        IOOperations io_1(inputStream, outputStream1, chunkSize);
        //IOOperations io_2(inputStream, outputStream2, chunkSize);
        //IOOperations io_3(inputStream, outputStream3, chunkSize);
        //if (verbose) 
        //std::cout << "Running flow graph based compression algorithm with async_node based asynchronious IO operations." << std::endl;    
        //fgCompressionAsyncNode(io_0, io_1, io_2, io_3, num_record, sort_type);

        /*tbb::flow::graph g;

        Socket* sock=new Socket();
        //AsyncNodeActivity(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd)
        AsyncNodeActivity asyncNodeActivity_0(io_0, num_record, sock, node_status);
        //AsyncNodeActivity asyncNodeActivity_1(io_1, num_record, sock, node_status);
        asyncNodeActivity_0.startWriterThread(); 
        //asyncNodeActivity_1.startWriterThread(); 
        asyncNodeActivity_0.setNetworkConfig(server_IP, server_port);
        //asyncNodeActivity_1.setNetworkConfig(server_IP, server_port);

        async_file_reader_node file_reader_0(g, 1, [&asyncNodeActivity_0](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
            asyncNodeActivity_0.submitRead(gateway);
        });

        //async_file_reader_node file_reader_1(g, 1, [&asyncNodeActivity_1](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
        //    asyncNodeActivity_1.submitRead(gateway);
        //});

        Mapper mapper_0(g, 1, MapperOperation(num_record));

        tbb::flow::sequencer_node< BufferMsg > ordering_0 (g, [](const BufferMsg& bufferMsg)->size_t {
            return bufferMsg.seqId;
        });

        async_file_writer_node output_writer_0(g, tbb::flow::serial, [&asyncNodeActivity_0](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
            asyncNodeActivity_0.submitWrite(bufferMsg);
        //std::cout << "fw0 " << "\n";
        });
        //async_file_writer_node output_writer_1(g, tbb::flow::serial, [&asyncNodeActivity_1](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
        //    asyncNodeActivity_1.submitWrite(bufferMsg);
        //});

        if(node_status == 1){
            make_edge(file_reader_0, output_writer_0);
            //make_edge(tbb::flow::output_port<0>(mapper_0), output_writer_0);
        }
        else if(node_status == 0){
            make_edge(file_reader_0, mapper_0);
            make_edge(tbb::flow::output_port<0>(mapper_0), output_writer_0);
            //make_edge(tbb::flow::output_port<1>(mapper_0), output_writer_1);
        }

        //trigger the file reader and 
        file_reader_0.try_put(tbb::flow::continue_msg());
        g.wait_for_all();


        inputStream.close();
        outputStream0.close();  
        //outputStream1.close();  
        //outputStream2.close();  
        //outputStream3.close();  

        utility::report_elapsed_time((tbb::tick_count::now() - mainStartTime).seconds());

        std::ofstream std_overall_file, radix_overall_file;

        uint64_t sorted_bits= 8*Kbytes*1000;
        double sort_rate=0;
        uint64_t sum_radix_time=0;
        uint64_t sum_std_time=0; 

        std::string extension = ".txt";
        if(sort_type == 0){
            std::string std_overall_str = "split_overall_buffer_" + std::to_string(Kbytes) + extension;
            std_overall_file.open(std_overall_str, std::ofstream::out | std::ofstream::app);
            std_overall_file << (tbb::tick_count::now() - mainStartTime).seconds()<< "\n";
        }
        else{
            std::string radix_overall_str = "split_overall_buffer_" + std::to_string(Kbytes) + extension;
            radix_overall_file.open(radix_overall_str, std::ofstream::out | std::ofstream::app);
            radix_overall_file << (tbb::tick_count::now() - mainStartTime).seconds() << "\n";
        } 
        std_overall_file.close();
        radix_overall_file.close();
        */
        return 0;
    } catch (std::exception& e) {
        std::cerr << "Error occurred. Error text is : \"" << e.what() << "\"\n";
        return -1;
    }
}


