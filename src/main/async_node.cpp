/*
    Copyright (c) 2005-2017 Intel Corporation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.




*/

//#define TBB_PREVIEW_FLOW_GRAPH_FEATURES 1
//#include "tbb/tbb_config.h"
#include "../utility/utility.h"
//if __TBB_PREVIEW_ASYNC_MSG && __TBB_CPP11_LAMBDAS_PRESENT

#include <iostream>
#include <fstream>
#include <string>
#include <memory>
#include <queue>
#include <cstdint>
//#include <stdint.h>
//#include <inttypes.h>
//#include "bzlib.h"
#include "tbb/flow_graph.h"
#include "tbb/tick_count.h"
#include "tbb/compat/thread"
#include "tbb/concurrent_queue.h"

#include "KeyValueRecord.h"
#include "Histogram.h"
#include "BucketTable.h"
    #include <vector>

std::vector<uint64_t> std_lantency_list;
std::vector<uint64_t> radix_lantency_list;
std::vector<double> std_rate_list;
std::vector<double> radix_rate_list;

class BaseBuffer{
  public: 
    BaseBuffer(uint32_t num){
      buf_size= num;
      buffer = new KeyValueRecord[num];
    }

    uint32_t getBufferSize(){
      return buf_size;
    }

    void createRecords(){ //(uint32_t _ks, uint32_t _vs){
      //ksize = _ks;
      //vsize = _vs;
      ksize = 10;
      vsize= 90;
      for(uint32_t i = 0; i < buf_size; i++){
        buffer[i].initRecord(ksize, vsize);
      }
    }

    void setRecord(KeyValueRecord* kvr, int index){
      //for(int j=0; j< buf_size; j++){
      buffer[index]=*kvr;
      //}
    }

    void setAllRecords(char* rawBuffer){
      
      for(int j=0; j< buf_size; j++){
        // TODO avoid memory copy
        //char* key;
        //char* value;
        //key = new char[ksize];
        //value = new char[vsize];
        //std::memcpy(key, kbase, ksize);
        //std::memcpy(value, vbase, vsize);
        char* kbase=rawBuffer+100*j;
        char* vbase=kbase+10;
        buffer[j].setKey(kbase);
        buffer[j].setValue(vbase);
      }
    }

    /*void dumpSingleRecord(char* key, char* value, char* rawBuffer){
        char* kbase=rawBuffer+100*j;
        char* vbase=kbase+10;
        buffer[j].setKey(kbase);
        buffer[j].setValue(vbase);
    }*/

    KeyValueRecord* getRecords(uint32_t index){
      return &(buffer[index]);
    }

    uint32_t getKeySize(){
        return ksize;
    }

    uint32_t getValueSize(){
        return vsize;
    }

  private:
  KeyValueRecord* buffer;
  uint32_t ksize;
  uint32_t vsize;  
  uint32_t buf_size;
};

// TODO: change memory allocation/deallocation to be managed in constructor/destructor
struct Buffer {
    size_t len;
    char* b;
};

struct BufferMsg {

    BufferMsg() {}
    BufferMsg(Buffer& inputBuffer, Buffer& outputBuffer, size_t seqId, bool isLast = false)
        : inputBuffer(inputBuffer), outputBuffer(outputBuffer), seqId(seqId), isLast(isLast) {}

    static BufferMsg createBufferMsg(size_t seqId, size_t chunkSize) {
        Buffer inputBuffer;
        inputBuffer.b = new char[chunkSize];
        inputBuffer.len = chunkSize;

        Buffer outputBuffer;
        outputBuffer.b = new char[chunkSize];
        outputBuffer.len = chunkSize;

        return BufferMsg(inputBuffer, outputBuffer, seqId);
    }

    static void destroyBufferMsg(const BufferMsg& destroyMsg) {
        delete[] destroyMsg.inputBuffer.b;
        delete[] destroyMsg.outputBuffer.b;
    }

    void markLast(size_t lastId) {
        isLast = true;
        seqId = lastId;
    }

    size_t seqId;
    Buffer inputBuffer;
    Buffer outputBuffer;
    bool isLast;
};

class BufferCompressor {
public:

    BufferCompressor(uint32_t num_rcrd, int _sorttype) : num_record(num_rcrd), sort_type(_sorttype), counter(0) {}

    BufferMsg operator()(BufferMsg buffer){
        //std::cout << "counter: " << counter << "\n";   
        if (!buffer.isLast) {
            // the next line is simple bugless testing line for the whole TBB flow graph
            //buffer.outputBuffer.b = buffer.inputBuffer.b;
            //std::cout << "num_record: " << +num_record << "\n";
            BaseBuffer* buf = new BaseBuffer(num_record);
            buf->createRecords();
            buf->setAllRecords(buffer.inputBuffer.b);

            //if(method==std_sort)
            struct timespec ts1,ts2;
            if(sort_type == 0){
                std::vector<KeyValueRecord*> ptr_list;

                for(uint32_t i = 0; i < num_record; i++){
                    KeyValueRecord* kvr = buf->getRecords(i);
                    ptr_list.push_back(kvr);
                }

                uint64_t t1 =getNanoSecond(ts1);
                std::sort(ptr_list.begin(), ptr_list.end(), compareKey);
                uint64_t t2 =getNanoSecond(ts2);
                std_lantency_list.push_back(t2-t1);

                uint32_t ksize = buf->getKeySize();
                uint32_t vsize = buf->getValueSize();

                for(uint32_t index = 0; index < num_record; index++){           
                    KeyValueRecord* kvr =ptr_list.at(index);
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
            else{
                BaseBuffer* output = new BaseBuffer(num_record);
                output->createRecords();     
                radix_sort(buf, output, num_record);
                uint32_t ksize = buf->getKeySize();
                uint32_t vsize = buf->getValueSize();

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
            //update counters and stats
            counter++;            
        }
        //counter++;
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

class IOOperations {
public:

    IOOperations(std::ifstream& inputStream, std::ofstream& outputStream, size_t chunkSize)
        : m_inputStream(inputStream), m_outputStream(outputStream), m_chunkSize(chunkSize), m_chunksRead(0) {}

    void readChunk(Buffer& buffer) {
        m_inputStream.read(buffer.b, m_chunkSize);
        buffer.len = static_cast<size_t>(m_inputStream.gcount());
        m_chunksRead++;
    }

    void writeChunk(const Buffer& buffer) {
        m_outputStream.write(buffer.b, buffer.len);
    }

    size_t chunksRead() const {
        return m_chunksRead;
    }

    size_t chunkSize() const {
        return m_chunkSize;
    }

    bool hasDataToRead() const {
        return m_inputStream.is_open() && !m_inputStream.eof();
    }

private:

    std::ifstream& m_inputStream;
    std::ofstream& m_outputStream;

    size_t m_chunkSize;
    size_t m_chunksRead;
};

//-----------------------------------------------------------------------------------------------------------------------
//---------------------------------------Compression example based on async_node-----------------------------------------
//-----------------------------------------------------------------------------------------------------------------------

typedef tbb::flow::async_node< tbb::flow::continue_msg, BufferMsg > async_file_reader_node;
typedef tbb::flow::async_node< BufferMsg, tbb::flow::continue_msg > async_file_writer_node;

class AsyncNodeActivity {
public:

    AsyncNodeActivity(IOOperations& io)
        : m_io(io), m_fileWriterThread(&AsyncNodeActivity::writingLoop, this) {}

    ~AsyncNodeActivity() {
        m_fileReaderThread.join();
        m_fileWriterThread.join();
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
            BufferMsg bufferMsg = BufferMsg::createBufferMsg(m_io.chunksRead(), m_io.chunkSize());
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
        gateway.try_put(lastMsg);
    }

    IOOperations& m_io;

    tbb::concurrent_bounded_queue< BufferMsg > m_writeQueue;

    std::thread m_fileReaderThread;
    std::thread m_fileWriterThread;
};

void fgCompressionAsyncNode(IOOperations& io, uint32_t num_rcrd, int sort_type) {
    tbb::flow::graph g;

    AsyncNodeActivity asyncNodeActivity(io);

    /*async_file_reader_node file_reader(g, tbb::flow::unlimited, [&asyncNodeActivity](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
        asyncNodeActivity.submitRead(gateway);
    });*/

    async_file_reader_node file_reader(g, 40, [&asyncNodeActivity](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
        asyncNodeActivity.submitRead(gateway);
    });

    //tbb::flow::function_node< BufferMsg, BufferMsg > compressor(g, tbb::flow::unlimited, BufferCompressor(num_rcrd, sort_type));

    tbb::flow::function_node< BufferMsg, BufferMsg > compressor(g, 40, BufferCompressor(num_rcrd, sort_type));

    tbb::flow::sequencer_node< BufferMsg > ordering(g, [](const BufferMsg& bufferMsg)->size_t {
        return bufferMsg.seqId;
    });

    // The node is serial to preserve the right order of buffers set by the preceding sequencer_node
    async_file_writer_node output_writer(g, tbb::flow::serial, [&asyncNodeActivity](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
        asyncNodeActivity.submitWrite(bufferMsg);
    });

    make_edge(file_reader, compressor);
    make_edge(compressor, ordering);
    make_edge(ordering, output_writer);

    file_reader.try_put(tbb::flow::continue_msg());

    g.wait_for_all();
}

//-----------------------------------------------------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    try {
        tbb::tick_count mainStartTime = tbb::tick_count::now();

        const std::string archiveExtension = ".dat";
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
        if (!inputStream.is_open()) {
            throw std::invalid_argument("Cannot open " + inputFileName + " file.");
        }

        std::string outputFileName(inputFileName + archiveExtension);

        std::ofstream outputStream(outputFileName.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
        if (!outputStream.is_open()) {
            throw std::invalid_argument("Cannot open " + outputFileName + " file.");
        }

        // General interface to work with I/O buffers operations
        size_t chunkSize = Kbytes * 1000;
        IOOperations io(inputStream, outputStream, chunkSize);


        if (verbose) 
            //std::cout << "Running flow graph based compression algorithm with async_node based asynchronious IO operations." << std::endl;    
        fgCompressionAsyncNode(io, num_record, sort_type);

        inputStream.close();
        outputStream.close();

        utility::report_elapsed_time((tbb::tick_count::now() - mainStartTime).seconds());

        std::ofstream std_time_file, std_rate_file, radix_time_file, radix_rate_file;

        uint64_t sorted_bits= 8*Kbytes*1000;
        double sort_rate=0;
        uint64_t sum_radix_time=0;
        uint64_t sum_std_time=0; 

        std::string extension = ".txt";
    if(sort_type == 0){
        std::string std_time_str = "std_time_iter_" + std::to_string(iteration) + 
            "_buffer_" + std::to_string(Kbytes) + extension;

        std::string std_rate_str = "std_rate_iter_" + std::to_string(iteration) + 
            "_buffer_" + std::to_string(Kbytes) + extension; 

        std_time_file.open(std_time_str);
        std_rate_file.open(std_rate_str); 
        
        for(int i=0; i < std_lantency_list.size(); i++){
            uint64_t sort_time = std_lantency_list.at(i);
            //std::cout << "std sort time:" << sort_time << "\n";
            std_time_file << sort_time << "\n";
            sum_std_time = sum_std_time + sort_time;
            double sort_rate=(double)sorted_bits/(double)sort_time; 
            std_rate_file << sort_rate << "\n";
            //std_rate_list.push_back(sort_rate)
        }   
        double mean_std_time = (double) sum_std_time / (double) std_lantency_list.size();
        std::cout<< "mean std sort time: " << mean_std_time << "\n";

        std_time_file.close();
        std_rate_file.close();
    }
    else{
        std::string radix_time_str = "radix_time_iter_" + std::to_string(iteration) + 
            "_buffer_" + std::to_string(Kbytes) + extension; 
        std::string radix_rate_str = "radix_rate_iter_" + std::to_string(iteration) + 
            "_buffer_" + std::to_string(Kbytes) + extension;  

        radix_time_file.open(radix_time_str);
        radix_rate_file.open(radix_rate_str);

        for(int i=0; i < radix_lantency_list.size(); i++){
            uint64_t sort_time = radix_lantency_list.at(i);
            //std::cout << "radix sort time:" << sort_time << "\n";
            sum_radix_time = sum_radix_time + sort_time;
            radix_time_file << sort_time << "\n";
            double sort_rate=(double)sorted_bits/(double)sort_time; 
            //std_rate_list.push_back(sort_rate)
            radix_rate_file << sort_rate << "\n";
        }
        double mean_radix_time = (double) sum_radix_time / (double) radix_lantency_list.size();
        std::cout<< "mean radix sort time: " << mean_radix_time << "\n";

        radix_time_file.close();
        radix_rate_file.close();
    }

        return 0;
    } catch (std::exception& e) {
        std::cerr << "Error occurred. Error text is : \"" << e.what() << "\"\n";
        return -1;
    }
}

