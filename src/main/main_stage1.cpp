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
#include "IOOperation.h"
#include "MapperOperation.h"
#include <vector>

typedef tbb::flow::async_node< tbb::flow::continue_msg, BufferMsg > async_file_reader_node;
typedef tbb::flow::async_node< BufferMsg, tbb::flow::continue_msg > async_file_writer_node;

class AsyncNodeActivity {
public:

    AsyncNodeActivity(IOOperations& io, uint32_t num_record)
        : m_io(io), m_fileWriterThread(&AsyncNodeActivity::writingLoop, this), m_num_record(num_record) {}

    ~AsyncNodeActivity() {
        m_fileReaderThread.join();
        std::cout<< "fileReaderThread join:" <<  count <<"\n";
        count++;
        m_fileWriterThread.join();
        std::cout<< "fileWriterThread join" << "\n";
    }

    void submitRead(async_file_reader_node::gateway_type& gateway) {
        gateway.reserve_wait();
        std::thread(&AsyncNodeActivity::readingLoop, this, std::ref(gateway)).swap(m_fileReaderThread);
    }

    void submitWrite(const BufferMsg& bufferMsg) {
        m_writeQueue.push(bufferMsg);
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

    tbb::concurrent_bounded_queue< BufferMsg > m_writeQueue;

    std::thread m_fileReaderThread;
    std::thread m_fileWriterThread;
};

void fgCompressionAsyncNode( IOOperations& io_0, IOOperations& io_1, IOOperations& io_2, IOOperations& io_3, 
    uint32_t num_rcrd, int sort_type) {

    tbb::flow::graph g;

    AsyncNodeActivity asyncNodeActivity_0(io_0, num_rcrd);
    AsyncNodeActivity asyncNodeActivity_1(io_1, num_rcrd);
    AsyncNodeActivity asyncNodeActivity_2(io_2, num_rcrd);
    AsyncNodeActivity asyncNodeActivity_3(io_3, num_rcrd);

    /*async_file_reader_node file_reader(g, tbb::flow::unlimited, [&asyncNodeActivity](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
        asyncNodeActivity.submitRead(gateway);
    });*/

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
    Mapper mapper_1(g, 1, MapperOperation(num_rcrd));
    Mapper mapper_2(g, 1, MapperOperation(num_rcrd));
    Mapper mapper_3(g, 1, MapperOperation(num_rcrd));

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
    file_reader_1.try_put(tbb::flow::continue_msg());
    file_reader_2.try_put(tbb::flow::continue_msg());
    file_reader_3.try_put(tbb::flow::continue_msg());

    g.wait_for_all();
}

//-----------------------------------------------------------------------------------------------------------------------
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
        std::string asyncType;
        asyncType == "async_node";
        std::string inputFileName;
        int blockSizeIn1000KB = 1; // block size in 100KB chunks
        uint32_t Kbytes =1;
        size_t memoryLimitIn1MB = 1; // memory limit for compression in megabytes granularity

        utility::parse_cli_arguments(argc, argv,
            utility::cli_argument_pack()
            //"-h" option for displaying help is present implicitly
            .arg(Kbytes, "-b", "\t buffer size in KB")
            .arg(sort_type, "-s", "\t numbers of iterations that our sort run")
            //.arg(verbose, "-v", "verbose mode")
            .arg(memoryLimitIn1MB, "-l", "used memory limit for compression algorithm in 1MB (minimum) granularity")
            .arg(asyncType, "-a", "name of the used graph async implementation - can be async_node or async_msg")
            .positional_arg(inputFileName, "filename", "input file name")
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
        std::ofstream outputStream2(outputname[2].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);    
        std::ofstream outputStream3(outputname[3].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);    

        /*
        std::ofstream outputStream(outputFileName.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
        if (!outputStream.is_open()) {
            throw std::invalid_argument("Cannot open " + outputFileName + " file.");
        }*/

        //using IOWrapper = std::reference_wrapper<IOOperations>;

        // General interface to work with I/O buffers operations
        size_t chunkSize = Kbytes * 1000;
        IOOperations io_0(inputStream, outputStream0, chunkSize);
        IOOperations io_1(inputStream, outputStream1, chunkSize);
        IOOperations io_2(inputStream, outputStream2, chunkSize);
        IOOperations io_3(inputStream, outputStream3, chunkSize);


        if (verbose) 
            //std::cout << "Running flow graph based compression algorithm with async_node based asynchronious IO operations." << std::endl;    
        fgCompressionAsyncNode(io_0, io_1, io_2, io_3, num_record, sort_type);

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
        return 0;
    } catch (std::exception& e) {
        std::cerr << "Error occurred. Error text is : \"" << e.what() << "\"\n";
        return -1;
    }
}


