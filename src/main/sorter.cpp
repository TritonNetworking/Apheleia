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
#include "AsyncWriter.h"
#include "AsyncReader.h"
#include <utility> 


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
        BaseBuffer* basebuf = new BaseBuffer();
        basebuf->createRecords(num_record);
        basebuf->setAllRecords(buffer.inputBuffer.b);
        //buffer.basebuf
    if(!buffer.isLast){
        struct timespec ts1,ts2;
        if(sort_type == 0){
                std::vector<KeyValueRecord*> ptr_list;

                for(uint32_t i = 0; i < num_record; i++){
                    KeyValueRecord* kvr = basebuf->getRecords(i);
                    ptr_list.push_back(kvr);
                }

                uint64_t t1 =getNanoSecond(ts1);
                std::sort(ptr_list.begin(), ptr_list.end(), compareKey);
                uint64_t t2 =getNanoSecond(ts2);
                std_lantency_list.push_back(t2-t1);

                uint32_t ksize = basebuf->getKeySize();
                uint32_t vsize = basebuf->getValueSize();

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
                radix_sort(basebuf, output, num_record);
                uint32_t ksize = basebuf->getKeySize();
                uint32_t vsize = basebuf->getValueSize();

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

//typedef tbb::flow::async_node< tbb::flow::continue_msg, BufferMsg > async_file_reader_node;
//typedef tbb::flow::async_node< BufferMsg, tbb::flow::continue_msg > async_file_writer_node;
using json = nlohmann::json;
//int node_status; //-1= error 0=send 1= recv

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
        int node_status = 1;
        std::string self_IP = "127.0.0.1";
        std::string machine_name = "b09_31";
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
        std::cout <<"input port"<< in_port << "\n";
        /*for (json::iterator it = config_json.begin(); it != config_json.end(); ++it) {
            json json_obj = *it;
            std::string itr_ip;
            std::string itr_port;
            for (json::iterator itr = json_obj.begin(); itr != json_obj.end(); ++itr) {
                //std::cout << itr.key()<<":"<< itr.value()<< "\n";
                if(itr.key() == "ip"){
                    itr_ip = itr.value();
                }
                else if(itr.key() == "in_port"){
                    itr_port = itr.value();
                }
            }
            dst_port.emplace(itr_ip, itr_port); //insert the key-value pair in-place
        }*/   


        if (verbose) std::cout << "Input file name: " << inputFileName << std::endl;    

        std::string inputName("/tmp/"+inputFileName);
        std::ifstream inputStream(inputName.c_str(), std::ios::in | std::ios::binary);

        if (!inputStream.is_open()) {
            throw std::invalid_argument("Cannot open " + inputFileName + " file.");
        }

        std::string outputname[4];
        for(int i=0; i < 4; i++){
            std::string outputFileName("/tmp/"+outputFilePrefix + "_" + std::to_string(i)+ archiveExtension);
            outputname[i] = outputFileName;
        }

        std::ofstream outputStream0(outputname[0].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);        
        std::ofstream outputStream1(outputname[1].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
        //std::ofstream outputStream2(outputname[2].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
        //std::ofstream outputStream3(outputname[3].c_str(), std::ios::out | std::ios::binary | std::ios::trunc);        

        // General interface to work with I/O buffers operations
        size_t chunkSize = Kbytes * 1000;
        IOOperations io_0(inputStream, outputStream0, chunkSize);
        IOOperations io_1(inputStream, outputStream1, chunkSize);
        //IOOperations io_2(inputStream, outputStream1, chunkSize);
        //IOOperations io_3(inputStream, outputStream1, chunkSize);

        // receiver Section, node_status == 1
        tbb::flow::graph g;

        Socket* r_sock_0 = new Socket();
        //Socket* r_sock_1 = new Socket();
        //Socket* r_sock_2 = new Socket();
        //Socket* r_sock_3 = new Socket();
        AsyncReader asyncreader_0(io_0, num_record, r_sock_0, node_status);
        asyncreader_0.setNetworkConfig(self_IP,in_port);
        /*AsyncReader asyncreader_1(io_0, num_record, r_sock_1, node_status);
        AsyncReader asyncreader_2(io_0, num_record, r_sock_2, node_status);
        AsyncReader asyncreader_3(io_0, num_record, r_sock_3, node_status);
        asyncreader_0.setNetworkConfig(self_IP,in_port);
        asyncreader_1.setNetworkConfig(self_IP,in_port);
        asyncreader_2.setNetworkConfig(self_IP,in_port);
        asyncreader_3.setNetworkConfig(self_IP,in_port);*/

        async_file_reader_node file_reader_0(g, 1, [&asyncreader_0](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
            asyncreader_0.submitRead(gateway);
        });

       /* async_file_reader_node file_reader_1(g, 1, [&asyncreader_1](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
            asyncreader_1.submitRead(gateway);
        });

        async_file_reader_node file_reader_2(g, 1, [&asyncreader_2](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
            asyncreader_2.submitRead(gateway);
        });

        async_file_reader_node file_reader_3(g, 1, [&asyncreader_3](const tbb::flow::continue_msg& msg, async_file_reader_node::gateway_type& gateway) {
            asyncreader_3.submitRead(gateway);
        });*/

        tbb::flow::function_node<BufferMsg, BufferMsg > sorter_0(g, 4, BufferSorter(num_record, sort_type));

        /*tbb::flow::sequencer_node< BufferMsg > ordering_0 (g, [](const BufferMsg& bufferMsg)->size_t {
            return bufferMsg.seqId;
        });*/

        Socket* w_sock = new Socket();
        AsyncWriter asyncwriter_0(io_1, num_record, w_sock,  node_status);
        //AsyncWriter asyncwriter_1(io_1, num_record, w_sock,  node_status);
        //AsyncWriter asyncwriter_2(io_1, num_record, w_sock,  node_status);
        //AsyncWriter asyncwriter_3(io_1, num_record, w_sock,  node_status);

        asyncwriter_0.startWriterThread(); 

        async_file_writer_node output_writer_0(g, 1, [&asyncwriter_0](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
            asyncwriter_0.submitWrite(bufferMsg);
        });
        //async_file_writer_node output_writer_1(g, 4, [&asyncwriter_1](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
        //    asyncwriter_1.submitWrite(bufferMsg);
        //});
        //async_file_writer_node output_writer_2(g, 4, [&asyncwriter_2](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
        //    asyncwriter_2.submitWrite(bufferMsg);
        //});
        //async_file_writer_node output_writer_3(g, 4, [&asyncwriter_3](const BufferMsg& bufferMsg, async_file_writer_node::gateway_type& gateway) {
        //    asyncwriter_3.submitWrite(bufferMsg);
        //});

        //node_status=0;
       if(node_status == 1){
            //make_edge(file_reader_0, output_writer_0);
            make_edge(file_reader_0, sorter_0);
            //make_edge(sorter_0, ordering_0);
            make_edge(sorter_0, output_writer_0);
            //trigger the file reader and 
            file_reader_0.try_put(tbb::flow::continue_msg());
            g.wait_for_all();
        }

        //asyncwriter_0.threadJoin();
        //asyncreader_0.threadJoin();
        
        inputStream.close();
        outputStream0.close();  
        outputStream1.close();  
        //outputStream2.close();  
        //outputStream3.close();  

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




