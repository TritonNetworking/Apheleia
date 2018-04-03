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

std::vector<uint64_t> std_lantency_list;
std::vector<uint64_t> radix_lantency_list;
std::vector<double> std_rate_list;
std::vector<double> radix_rate_list;

class BufferSorter {
public:

BufferSorter(uint32_t num_rcrd, int _sorttype) : num_record(num_rcrd), sort_type(_sorttype), counter(0) {}

    //KeyValueRecord* operator()(BufferMsg buffer){
    //BufferMsg operator()(BufferMsg* buffer_ptr){
BufferMsg operator()(BufferMsg buffer){
        //std::cout << "Sorter"<< "-";
        //std::cout << "counter: " << counter << "\n";   
        // the next line is simple bugless testing line for the whole TBB flow graph
        //buffer.outputBuffer.b = buffer.inputBuffer.b;
        //std::cout << "num_record: " << +num_record << "\n";
        //BaseBuffer* buf = new BaseBuffer();
        //buf->createRecords(num_record);
        //buf->setAllRecords(buffer.inputBuffer.b);
        //buffer.basebuf
    if(!buffer.isLast){
        struct timespec ts1,ts2;
        if(sort_type == 0){
                std::vector<KeyValueRecord*> ptr_list;

                for(uint32_t i = 0; i < num_record; i++){
                    KeyValueRecord* kvr = buffer.basebuf->getRecords(i);
                    ptr_list.push_back(kvr);
                }

                uint64_t t1 =getNanoSecond(ts1);
                std::sort(ptr_list.begin(), ptr_list.end(), compareKey);
                uint64_t t2 =getNanoSecond(ts2);
                std_lantency_list.push_back(t2-t1);

                uint32_t ksize = buffer.basebuf->getKeySize();
                uint32_t vsize = buffer.basebuf->getValueSize();

                for(uint32_t index = 0; index < num_record; index++){           
                    KeyValueRecord* kvr =ptr_list.at(index);
                    char* kptr= kvr->getKeyBuffer();
                    char* vptr = kvr->getValueBuffer();
                    //ouput ptrs
                    char* kbase=buffer.outputBuffer.b+100*index;
                    char* vbase=kbase+10;
                    //memcpy
                    std::memcpy(kbase, kptr, ksize);
                    std::memcpy(vbase, vptr, vsize);    
                }
        }
        else{
                BaseBuffer* output = new BaseBuffer();
                output->createRecords(num_record);     
                //radix_sort(&buffer.basebuf, output, num_record);
                uint32_t ksize = buffer.basebuf->getKeySize();
                uint32_t vsize = buffer.basebuf->getValueSize();

                for(uint32_t index = 0; index < num_record; index++){           
                    KeyValueRecord* kvr = output->getRecords(index);
                    //input ptrs
                    char* kptr= kvr->getKeyBuffer();
                    char* vptr = kvr->getValueBuffer();
                    //ouput ptrs
                    char* kbase=buffer.outputBuffer.b+100*index;
                    char* vbase=kbase+10;
                    //memcpy
                    std::memcpy(kbase, kptr, ksize);
                    std::memcpy(vbase, vptr, vsize); 
                }
        }
        //delete buf; 
        //delete output;      
        //update counters and stats
        counter++;            
        //counter++;
    }
    return buffer;  
}

private:
    uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
    }


    void radix_sort(BaseBuffer* buf, BaseBuffer* output, uint32_t num_record){
    //#ifndef RADIX
    uint32_t keysize = 10;
    uint32_t valuesize = 90;
    struct timespec ts1,ts2;

    // simple sort test with Bucket and BT.
    Histogram histo;
    uint32_t radix = 256;
    BucketTable* BT = new BucketTable(&histo, radix);
    BT->setKeySize(keysize);
    BT->setValueSize(valuesize);
    BT->allocBuckets();

    for(uint32_t i = 0; i < num_record; i++){
        KeyValueRecord* kvr = buf->getRecords(i);
        BT->distributeBuffer(kvr); // digit = keysize-1 used here
    }

    uint64_t t1 =getNanoSecond(ts1);  

    for(int digit = keysize - 2; digit >= 0; digit--){ 
    //std::cout << "digit:" << digit << "\n";
        for(uint32_t b_index=0; b_index < radix; b_index++){
            Bucket* bckt = BT->getBucket(b_index);
            bckt->setRecordCount();
            uint32_t num= bckt->getRecordCount();
            // Iterate over Records in a bucket
            for(uint32_t r_index=0; r_index < num; r_index++){
                KeyValueRecord* pop_kvr = bckt->popEntry();
                uint8_t key = pop_kvr->getKey(digit);
                Bucket* new_bckt = BT->getBucket(key);
                new_bckt->pushEntry(pop_kvr);
            }
        }
    }

    uint64_t t2 =getNanoSecond(ts2);   

    int index=0;
    for(uint32_t b_index=0; b_index < radix; b_index++){
        Bucket* bckt = BT->getBucket(b_index);
        bckt->setRecordCount();
        uint32_t num= bckt->getRecordCount();
        // Iterate over Records in a bucket
        for(uint32_t r_index=0; r_index < num; r_index++){
            KeyValueRecord* pop_kvr = bckt->getEntry(r_index);
            output->setRecord(pop_kvr, index);
            index++;
        }
    }
    radix_lantency_list.push_back(t2-t1);
    //delete BT;
    //return t2-t1;
    }

    static bool compareKey(KeyValueRecord* kvr1, KeyValueRecord* kvr2){
    uint32_t keysize = 10;
    for(uint32_t j = 0; j < keysize; j++){
      uint8_t k1 = kvr1->getKey(j);
      uint8_t k2 = kvr2->getKey(j);
      if(k1 < k2){
        return true;
      }
      else if (k1 > k2){
        return false;
      }
      else{ // k1 == k2 on this byte
        continue;
      }
      //printf("%" PRIu8 " ",k);
      //std::cout << +k << " ";
    }
    return false; // false for k1 <= k2
    }


    uint32_t num_record;
    int sort_type;
    uint32_t counter;
};

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
    //Mapper mapper_0(g, 1, MapperOperation(num_rcrd));
    /*Mapper mapper_1(g, 1, MapperOperation(num_rcrd));
    Mapper mapper_2(g, 1, MapperOperation(num_rcrd));
    Mapper mapper_3(g, 1, MapperOperation(num_rcrd));*/

    tbb::flow::function_node<BufferMsg, BufferMsg > sorter_0(g, 4, BufferSorter(num_rcrd, sort_type));
    tbb::flow::function_node<BufferMsg, BufferMsg > sorter_1(g, 4, BufferSorter(num_rcrd, sort_type));
    tbb::flow::function_node<BufferMsg, BufferMsg > sorter_2(g, 4, BufferSorter(num_rcrd, sort_type));
    tbb::flow::function_node<BufferMsg, BufferMsg > sorter_3(g, 4, BufferSorter(num_rcrd, sort_type));

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

    make_edge(file_reader_0, sorter_0);
    make_edge(file_reader_1, sorter_1);
    make_edge(file_reader_2, sorter_2);
    make_edge(file_reader_3, sorter_3);

    //make_edge(mapper, sorter);
    make_edge(sorter_0, ordering_0);
    make_edge(sorter_1, ordering_1);
    make_edge(sorter_2, ordering_2);
    make_edge(sorter_3, ordering_3);
    //make_edge(ordering, output_writer);
    make_edge(ordering_0, output_writer_0);
    make_edge(ordering_1, output_writer_1);
    make_edge(ordering_2, output_writer_2);
    make_edge(ordering_3, output_writer_3);

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

        const std::string archiveExtension = ".sort";
        const std::string inputExtension = ".out";
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

        std::string inputname[4];
        for(int i=0; i < 4; i++){
            std::string inFileName(inputFileName + "_" + std::to_string(i)+ inputExtension);
            std::cout << inFileName << "\n";
            inputname[i] = inFileName;
        }
        std::ifstream inputStream0(inputname[0].c_str(), std::ios::in | std::ios::binary);
        std::ifstream inputStream1(inputname[1].c_str(), std::ios::in | std::ios::binary);
        std::ifstream inputStream2(inputname[2].c_str(), std::ios::in | std::ios::binary);
        std::ifstream inputStream3(inputname[3].c_str(), std::ios::in | std::ios::binary);

        /*if (!inputStream.is_open()) {
            throw std::invalid_argument("Cannot open " + inputFileName + " file.");
        }*/

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
        IOOperations io_0(inputStream0, outputStream0, chunkSize);
        IOOperations io_1(inputStream1, outputStream1, chunkSize);
        IOOperations io_2(inputStream2, outputStream2, chunkSize);
        IOOperations io_3(inputStream3, outputStream3, chunkSize);


        if (verbose) 
            //std::cout << "Running flow graph based compression algorithm with async_node based asynchronious IO operations." << std::endl;    
        fgCompressionAsyncNode(io_0, io_1, io_2, io_3, num_record, sort_type);

        inputStream0.close();
        inputStream1.close();
        inputStream2.close();
        inputStream3.close();
        outputStream0.close();  
        outputStream1.close();  
        outputStream2.close();  
        outputStream3.close();  

        utility::report_elapsed_time((tbb::tick_count::now() - mainStartTime).seconds());

        std::ofstream std_time_file, std_rate_file, radix_time_file, radix_rate_file;
        std::ofstream std_overall_file, radix_overall_file;

        uint64_t sorted_bits= 8*Kbytes*1000;
        double sort_rate=0;
        uint64_t sum_radix_time=0;
        uint64_t sum_std_time=0; 

        std::string extension = ".txt";
    if(sort_type == 0){
        /*std::string std_time_str = "std_time_iter_" + std::to_string(iteration) + 
            "_buffer_" + std::to_string(Kbytes) + extension;

        std::string std_rate_str = "std_rate_iter_" + std::to_string(iteration) + 
            "_buffer_" + std::to_string(Kbytes) + extension; */
        std::string std_overall_str = "std_overall_buffer_" + std::to_string(Kbytes) + extension;

        //std_time_file.open(std_time_str, std::ofstream::out | std::ofstream::app);
        //std_rate_file.open(std_rate_str, std::ofstream::out | std::ofstream::app);
        std_overall_file.open(std_overall_str, std::ofstream::out | std::ofstream::app);
        
        /*for(int i=0; i < std_lantency_list.size(); i++){
            uint64_t sort_time = std_lantency_list.at(i);
            //std::cout << "std sort time:" << sort_time << "\n";
            std_time_file << sort_time << "\n";
            sum_std_time = sum_std_time + sort_time;
            double sort_rate=(double)sorted_bits/(double)sort_time; 
            std_rate_file << sort_rate << "\n";
            //std_rate_list.push_back(sort_rate)
        }*/   
        //double mean_std_time = (double) sum_std_time / (double) std_lantency_list.size();
        //std::cout<< "mean std sort time: " << mean_std_time << "\n";

        std_overall_file << (tbb::tick_count::now() - mainStartTime).seconds()<< "\n"; 

        //std_time_file.close();
        //std_rate_file.close();
        std_overall_file.close();
    }
    else{
        /*std::string radix_time_str = "radix_time_iter_" + std::to_string(iteration) + 
            "_buffer_" + std::to_string(Kbytes) + extension; 
        std::string radix_rate_str = "radix_rate_iter_" + std::to_string(iteration) + 
            "_buffer_" + std::to_string(Kbytes) + extension; */ 
        std::string radix_overall_str = "radix_overall_buffer_" + std::to_string(Kbytes) + extension;

        radix_overall_file.open(radix_overall_str, std::ofstream::out | std::ofstream::app);
        /*radix_time_file.open(radix_time_str, std::ofstream::out | std::ofstream::app);
        radix_rate_file.open(radix_rate_str, std::ofstream::out | std::ofstream::app);*/


        /*for(int i=0; i < radix_lantency_list.size(); i++){
            uint64_t sort_time = radix_lantency_list.at(i);
            //std::cout << "radix sort time:" << sort_time << "\n";
            sum_radix_time = sum_radix_time + sort_time;
            radix_time_file << sort_time << "\n";
            double sort_rate=(double)sorted_bits/(double)sort_time; 
            //std_rate_list.push_back(sort_rate)
            radix_rate_file << sort_rate << "\n";
        }
        double mean_radix_time = (double) sum_radix_time / (double) radix_lantency_list.size();*/
        //std::cout<< "mean radix sort time: " << mean_radix_time << "\n";

        radix_overall_file << (tbb::tick_count::now() - mainStartTime).seconds() << "\n"; 

        //radix_time_file.close();
        //radix_rate_file.close();
        radix_overall_file.close();
    }

    return 0;
    } catch (std::exception& e) {
        std::cerr << "Error occurred. Error text is : \"" << e.what() << "\"\n";
        return -1;
    }
}


