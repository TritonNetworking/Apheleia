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
//#include <pair>
#include <utility> 

typedef tbb::flow::async_node< tbb::flow::continue_msg, BufferMsg > async_file_reader_node;
typedef tbb::flow::async_node< BufferMsg, tbb::flow::continue_msg > async_file_writer_node;
using json = nlohmann::json;
//int node_status; //-1= error 0=send 1= recv

class AsyncReader {
public:

    AsyncReader(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd)
        : m_io(io), m_num_record(num_record), tcp_socket(sock_ptr), node_status(rcv_or_snd){}

    ~AsyncReader() {
        m_ReaderThread.join();
        std::cout<< "ReaderThread join:" <<"\n";
    }

    void submitRead(async_file_reader_node::gateway_type& gateway) {
        gateway.reserve_wait();
        if(node_status == 0){
            std::thread(&AsyncReader::readingLoop, this, std::ref(gateway)).swap(m_ReaderThread);
        }
        else if(node_status == 1){
            std::thread(&AsyncReader::receiveLoop, this, std::ref(gateway)).swap(m_ReaderThread);
        }
    }

    void setNetworkConfig(std::string ip, std::string port){
        server_IP=ip;
        server_port=port;
    }

private:

    void readingLoop(async_file_reader_node::gateway_type& gateway) {
        while (m_io.hasDataToRead()) {
            BufferMsg bufferMsg = BufferMsg::createBufferMsg(m_io.chunksRead(), m_io.chunkSize() );
            m_io.readChunk(bufferMsg.inputBuffer);
            gateway.try_put(bufferMsg);
        }
        sendLastMessage(gateway);
        gateway.release_wait();
    }

    void receiveLoop(async_file_reader_node::gateway_type& gateway){
        tcp_socket->tcpListen(server_port, backlogSize);
        Socket* new_socket=tcp_socket->tcpAccept(timeoutInMicros, m_io.chunkSize());
        uint64_t recv_bytes = 1;
        while (recv_bytes > 0) {
            BufferMsg bufferMsg = BufferMsg::createBufferMsg(m_io.chunksRead(), m_io.chunkSize());
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
        gateway.try_put(lastMsg);
    }

    IOOperations& m_io;
    int count=0;
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

class AsyncWriter {
public:
    AsyncWriter(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd)
        : m_io(io), m_num_record(num_record), tcp_socket(sock_ptr), node_status(rcv_or_snd){}

    ~AsyncWriter() {
        m_WriterThread.join();
        std::cout<< "WriterThread join" << "\n";
    }

    void startWriterThread(){
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

    void submitWrite(const BufferMsg& bufferMsg) {
        m_writeQueue.push(bufferMsg);
    }

    void setNetworkConfig(std::string ip, std::string port){
        server_IP=ip;
        server_port=port;
        //std::cout <<"server ip"<< server_IP << "\n";
        //std::cout <<"server port"<< server_port << "\n";
    }

private:
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

    IOOperations& m_io;
    int count=0;
    uint32_t m_num_record;
    int node_status; //-1= error 0=send 1= recv

    uint64_t Retry=5;
    uint64_t retryDelayInMicros=1000;
    Socket* tcp_socket;
    std::string server_IP;
    std::string server_port;

    //std::pair<std::string, std::string>& dst_port;
    tbb::concurrent_bounded_queue<BufferMsg> m_writeQueue;
    std::thread m_WriterThread;
};



uint64_t GetNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
}

int main(int argc, char* argv[]) {
    try {
        tbb::tick_count mainStartTime = tbb::tick_count::now();

        const std::string archiveExtension = ".sort";
        bool verbose = true;
        uint32_t iteration;
        int sort_type;
        int node_status = 0;
        std::string self_IP = "127.0.0.1";
        std::string machine_name = "b09_27";
        std::string in_port = "8000";
        std::string out_port = "8010";
        std::map<std::string, std::string> dst_port;
        json config_json;
        //asyncType == "async_node";
        std::string inputFileName;
        std::string outputFilePrefix;
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
            .arg(machine_name, "-m", "the machine name")
            .arg(configFileName, "-cfg", "config file name")
            //.arg(server_port, "-port", "the node is either sender or receiver")
            //.arg(verbose, "-v", "verbose mode")
            //.arg(memoryLimitIn1MB, "-l", "used memory limit for compression algorithm in 1MB (minimum) granularity")
            //.arg(asyncType, "-a", "name of the used graph async implementation - can be async_node or async_msg")
            //.positional_arg(server_port, "server_port", "server port")
            //.positional_arg(server_IP, "server_ip", "server ip addr")
            //.positional_arg(inputFileName, "input_filename", "input file name")
        );

        uint32_t keysize = 10;
        uint32_t valuesize = 90;
        uint32_t num_record = 1000*Kbytes/(keysize+valuesize);

        //std::cout << Kbytes << "\n";

        std::ifstream configStream(configFileName.c_str(), std::ios::in | std::ios::binary);
        configStream >> config_json;
        for (json::iterator it = config_json[machine_name].begin(); it != config_json[machine_name].end(); ++it) {
            //json j= *it; //for nested json, create another json to iterate the next level
            //std::cout << it.key()<<":"<< it.value()<< "\n";
            if(it.key() == "ip"){
                self_IP = it.value();
            }
            else if(it.key() == "infile"){
                inputFileName = it.value();
            }
            else if(it.key() == "inport"){
                in_port = it.value();
            }
            else if(it.key() == "outfile"){
                outputFilePrefix = it.value();
            }
            else{
               std::cerr << "Config Parsing Error"<< "\"\n";    
            }
        }
        for (json::iterator it = config_json.begin(); it != config_json.end(); ++it) {
            json json_obj = *it;
            std::string itr_ip;
            std::string itr_port;
            for (json::iterator itr = json_obj.begin(); itr != json_obj.end(); ++itr) {
                //std::cout << itr.key()<<":"<< itr.value()<< "\n";
                if(itr.key() == "ip"){
                    itr_ip = itr.value();
                }
                else if(itr.key() == "inport"){
                    itr_port = itr.value();
                }
            }
            std::cout << itr_ip<<":"<< itr_port<< "\n";
            dst_port.emplace(itr_ip, itr_port); //insert the key-value pair in-place
        }   


        if (verbose) std::cout << "Input file name: " << inputFileName << std::endl;    

        std::string inputName("/tmp/"+inputFileName);
        std::ifstream inputStream(inputName.c_str(), std::ios::in | std::ios::binary);

        if (!inputStream.is_open()) {
            throw std::invalid_argument("Cannot open " + inputFileName + " file.");
        }

        std::string outputname[4];
        for(int i=0; i < 4; i++){
            std::string outputFileName("/tmp/"+ outputFilePrefix + "_" + std::to_string(i)+ archiveExtension);
            outputname[i] = outputFileName;
        }

        std::ofstream outputStream0(outputname[0].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);        
        std::ofstream outputStream1(outputname[1].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);        
        std::ofstream outputStream2(outputname[2].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);        
        std::ofstream outputStream3(outputname[3].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);        

        // General interface to work with I/O buffers operations
        size_t chunkSize = Kbytes * 1000;
        IOOperations io_0(inputStream, outputStream0, chunkSize);
        IOOperations io_1(inputStream, outputStream1, chunkSize);
        IOOperations io_2(inputStream, outputStream2, chunkSize);
        IOOperations io_3(inputStream, outputStream3, chunkSize);

        // Sender Section, node_status == 0
        tbb::flow::graph g;

        Socket* r_sock;
        //TODO make these async_writers and output_writers as array if we want to scale
        /*std::vector<Socket*> socketArray;
        std::vector<AsyncWriter> asyncWtrArray;
        for (auto it = dst_port.begin(); it != dst_port.end(); ++it) {
            //std::pair <std::string, std::string> dst_pair = std::make_pair (it->first,it->second);
            std::string server_ip= it->first;
            std::string server_port= it->second;
            Socket* sock = new Socket();
            AsyncWriter asyncwriter(io_1, num_record, sock,  node_status);
            asyncwriter.setNetworkConfig(server_ip, server_port);
            asyncwriter.startWriterThread();
            asyncWtrArray.push_back(asyncwriter);
        }
        std::vector<async_file_writer_node> asyncFwArray;
        for (int i = 0; i < asyncWtrArray.size(); i++) {
            AsyncWriter aw = asyncWtrArray[i];
            async_file_writer_node output_writer(g, 4, [&afw](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
                afw.submitWritcreateBufferMsge(bufferMsg);
            });
            asyncFwArray.push_back(output_writer);
        }*/     

        //AsyncNodeActivity(IOOperations& io, uint32_t num_record, Socket* sock_ptr, int rcv_or_snd)
        AsyncReader asyncreader_0(io_0, num_record, r_sock, node_status);

        // to write locally, node_status = 1;
        //TODO make these async_writers and output_writers as array if we want to scale
        Socket* w_sock_0 = new Socket();
        Socket* w_sock_1 = new Socket();
        Socket* w_sock_2 = new Socket();
        Socket* w_sock_3 = new Socket();
        AsyncWriter asyncwriter_0(io_1, num_record, w_sock_0,  node_status);
        AsyncWriter asyncwriter_1(io_1, num_record, w_sock_1,  node_status);
        AsyncWriter asyncwriter_2(io_1, num_record, w_sock_2,  node_status);
        AsyncWriter asyncwriter_3(io_1, num_record, w_sock_3,  node_status);
        auto it = dst_port.begin();
        asyncwriter_0.setNetworkConfig(it->first,it->second);
        //std::cout << "port:" << it->second << "\n";
        ++it;
        asyncwriter_1.setNetworkConfig(it->first,it->second);
        //std::cout << "port:" << it->second << "\n";
        ++it;
        asyncwriter_2.setNetworkConfig(it->first,it->second);
        //std::cout << "port:" << it->second << "\n";
        ++it;
        asyncwriter_3.setNetworkConfig(it->first,it->second);
        //std::cout << "port:" << it->second << "\n";

        asyncwriter_0.startWriterThread(); 
        asyncwriter_1.startWriterThread(); 
        asyncwriter_2.startWriterThread(); 
        asyncwriter_3.startWriterThread(); 

        async_file_reader_node file_reader_0(g, 4, [&asyncreader_0](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
            asyncreader_0.submitRead(gateway);
        });


        Mapper mapper_0(g, 1, MapperOperation(num_record));

        /*tbb::flow::sequencer_node< BufferMsg > ordering_0 (g, [](const BufferMsg& bufferMsg)->size_t {
            return bufferMsg.seqId;
        });*/

        async_file_writer_node output_writer_0(g, 1, [&asyncwriter_0](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
            asyncwriter_0.submitWrite(bufferMsg);
        });
        async_file_writer_node output_writer_1(g, 1, [&asyncwriter_1](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
            asyncwriter_1.submitWrite(bufferMsg);
        });
        
        async_file_writer_node output_writer_2(g, 1, [&asyncwriter_2](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
            asyncwriter_2.submitWrite(bufferMsg);
        });
        async_file_writer_node output_writer_3(g, 1, [&asyncwriter_3](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
            asyncwriter_3.submitWrite(bufferMsg);
        });

        //node_status=0;
       if(node_status == 0){
            //make_edge(file_reader_0, output_writer_0);
            make_edge(file_reader_0, mapper_0);
            make_edge(tbb::flow::output_port<0>(mapper_0), output_writer_0);
            make_edge(tbb::flow::output_port<1>(mapper_0), output_writer_1);
            make_edge(tbb::flow::output_port<2>(mapper_0), output_writer_2);
            make_edge(tbb::flow::output_port<3>(mapper_0), output_writer_3);
            //trigger the file reader and 
            file_reader_0.try_put(tbb::flow::continue_msg());
            g.wait_for_all();
        }
        
        inputStream.close();
        outputStream0.close();  
        outputStream1.close();  
        outputStream2.close();  
        outputStream3.close();  

        utility::report_elapsed_time((tbb::tick_count::now() - mainStartTime).seconds());

        std::ofstream std_overall_file, radix_overall_file;

        uint64_t sorted_bits= 8*Kbytes*1000;
        double sort_rate=0;
        uint64_t sum_radix_time=0;
        uint64_t sum_std_time=0; 

        /*std::string extension = ".txt";
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


