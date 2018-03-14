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
#include "Histogram.h"
#include "BucketTable.h"
#include <vector>

std::vector<uint64_t> std_lantency_list;
std::vector<uint64_t> radix_lantency_list;
std::vector<double> std_rate_list;
std::vector<double> radix_rate_list;

class BaseBuffer{
  public: 
    BaseBuffer(){}

    //TODO: fix deconstructors
    ~BaseBuffer(){
       //delete [] buffer;
        std::cout << " BaseBuffer destructor" <<"\n";
        for(uint32_t i = 0; i < buffer.size() ; i++){
            KeyValueRecord* kvr = buffer.back();
            //kvr->initRecord(ksize, vsize); 
            delete kvr; //de-alloc bug
            buffer.pop_back();
        }
    }

    uint32_t getBufferSize(){
      return buf_size;
    }

    void createRecords(uint32_t num){ //(uint32_t _ks, uint32_t _vs){
      //ksize = _ks;
      //vsize = _vs;
      buf_size= num;
      //buffer = new KeyValueRecord[num];
      buffer.reserve(buf_size);
      ksize = 10;
      vsize= 90;
      //buffer = new KeyValueRecord[num];
      for(uint32_t i = 0; i < buf_size; i++){
        KeyValueRecord* kvr = new KeyValueRecord();
        kvr->initRecord(ksize, vsize); //BUG??
        buffer.push_back(kvr);
      }
    }

    void setRecord(KeyValueRecord* kvr, int index){
      //for(int j=0; j< buf_size; j++){
      buffer[index]=kvr;
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
        buffer[j]->setKey(kbase);
        buffer[j]->setValue(vbase);
      }
    }

    /*void dumpSingleRecord(char* key, char* value, char* rawBuffer){
        char* kbase=rawBuffer+100*j;
        char* vbase=kbase+10;
        buffer[j].setKey(kbase);
        buffer[j].setValue(vbase);
    }*/

    KeyValueRecord* getRecords(uint32_t index){
      return buffer[index];
    }

    uint32_t getKeySize(){
        return ksize;
    }

    uint32_t getValueSize(){
        return vsize;
    }

  private:
  //KeyValueRecord* buffer;
  std::vector<KeyValueRecord*> buffer;
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
    BufferMsg(Buffer& inputBuffer, Buffer& outputBuffer, BaseBuffer* _basebuf, size_t seqId, bool isLast = false)
        : inputBuffer(inputBuffer), outputBuffer(outputBuffer), basebuf(_basebuf), seqId(seqId), isLast(isLast){}

    //TODO add one more argument for num_record  
    static BufferMsg createBufferMsg(size_t seqId, size_t chunkSize, uint32_t num_rcrd) {
        //num_record = num_rcrd;

        Buffer _inputBuffer;
        _inputBuffer.b = new char[chunkSize];
        _inputBuffer.len = chunkSize;

        Buffer _outputBuffer;
        _outputBuffer.b = new char[chunkSize];
        _outputBuffer.len = chunkSize;

        BaseBuffer* _basebuf = new BaseBuffer(); // if basebuf is hust a reference, then its underlying pointers can be an issue
        _basebuf->createRecords(num_rcrd);
        _basebuf->setAllRecords(_inputBuffer.b);

        return BufferMsg(_inputBuffer, _outputBuffer, _basebuf, seqId);
    }

    static void destroyBufferMsg(const BufferMsg& destroyMsg) {
        delete[] destroyMsg.inputBuffer.b;
        delete[] destroyMsg.outputBuffer.b;
        delete destroyMsg.basebuf;
    }

    void markLast(size_t lastId) {
        isLast = true;
        seqId = lastId;
    }

    size_t seqId;
    Buffer inputBuffer;
    Buffer outputBuffer;
    BaseBuffer* basebuf;
    bool isLast;
    uint32_t num_record;
};

typedef tbb::flow::multifunction_node<BufferMsg, tbb::flow::tuple<BufferMsg,BufferMsg,BufferMsg,BufferMsg> > Mapper;

class MapperOp{

    public:
    MapperOp(uint32_t num_rcrd) : num_record(num_rcrd), counter(0), nway(4) {}

    void operator() (BufferMsg buffer, Mapper::output_ports_type &op){
        //std::cout << "MapperOp"<< "-";
        //std::cout << "counter: " << counter << "\n";   

        BufferMsg bufmsg_array[nway];
        std::vector<BaseBuffer*> basebuf_array;
        //BaseBuffer* basebuf_array = new BaseBuffer[nway];
        uint32_t basebuf_index[nway];
        BaseBuffer basebuf;//= new BaseBuffer(num_record);
        basebuf.createRecords(num_record);
        basebuf.setAllRecords(buffer.inputBuffer.b);

        //1. create BaseBuffer _basebuf
        for(int i = 0; i < nway; i++){
            BaseBuffer* b = new BaseBuffer();
            b->createRecords( uint32_t(num_record/nway) );
            basebuf_array.push_back(b);
            basebuf_index[i]=0;
        }

        //2. distribute records to 
        for(uint32_t i = 0; i < num_record; i++){
            KeyValueRecord* kvr = basebuf.getRecords(i);
            int split_way = kvr->getKey(0)%nway; // split it into four
            basebuf_array[split_way]->setRecord(kvr,basebuf_index[split_way]);
            basebuf_index[split_way]++;
        }


        for(int i = 0; i < nway; i++){
            //TODO!! pointer or ref? for bufmsg_array.
            if(!buffer.isLast){
                bufmsg_array[i] = BufferMsg(buffer.inputBuffer, buffer.outputBuffer, basebuf_array[i], buffer.seqId);
            }
            else{
                bufmsg_array[i] = BufferMsg(buffer.inputBuffer, buffer.outputBuffer, basebuf_array[i], buffer.seqId);   
                bufmsg_array[i].markLast(buffer.seqId);
            }
            if(i==0){
                    std::get<0>(op).try_put(bufmsg_array[i]);
            }
            else if(i==1){
                    std::get<1>(op).try_put(bufmsg_array[i]);
            }
            else if(i==2){
                    std::get<2>(op).try_put(bufmsg_array[i]);
            }
            else{
                    std::get<3>(op).try_put(bufmsg_array[i]);
            }
        }
        counter++; 
            /*  
            BufferMsg bufmsg_array[nway];
            BaseBuffer basebuf_array[nway];
            uint32_t basebuf_index[nway];
            BaseBuffer basebuf;
            basebuf.createRecords(num_record);
            basebuf.setAllRecords(buffer.inputBuffer.b);

            //1. create BaseBuffer _basebuf
            for(int i = 0; i < nway; i++){
                basebuf_array[i].createRecords(num_record/nway)
            }

            //2. distribute records to 
            for(uint32_t i = 0; i < num_record; i++){
                KeyValueRecord* kvr = basebuf.getRecords(i);
                int split_way = kvr->getKey(0)%nway; // split it into four
                basebuf_array[split_way].setRecord(kvr,basebuf_index[split_way]);
                basebuf_index[split_way]++;
            }
            // new outputBuffer
            size_t length = buffer.outputBuffer.len;
            for(int i = 0; i < nway; i++){
                Buffer output;
                output.b = new char[length];
                output.len = length;
                //TODO!! pointer or ref? for bufmsg_array.
                bufmsg_array[i] = BufferMsg(buffer.inputBuffer, output, basebuf_array[i], buffer.seqId);
                std::get<i>(op).try_put(bufmsg_array[i]);
            }*/              
    }
    private:

    uint32_t nway;
    uint32_t counter;
    uint32_t num_record;
};




class BufferCompressor {
public:

    BufferCompressor(uint32_t num_rcrd, int _sorttype) : num_record(num_rcrd), sort_type(_sorttype), counter(0) {}

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
                    //char MSBytes=kvr->getKey(0);
                    //output_port = MSBytes%4;
                    //TODO assign buffer to port out
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
    Mapper mapper_0(g, 1, MapperOp(num_rcrd));
    Mapper mapper_1(g, 1, MapperOp(num_rcrd));
    Mapper mapper_2(g, 1, MapperOp(num_rcrd));
    Mapper mapper_3(g, 1, MapperOp(num_rcrd));

    tbb::flow::function_node<BufferMsg, BufferMsg > sorter_0(g, 4, BufferCompressor(num_rcrd, sort_type));
    tbb::flow::function_node<BufferMsg, BufferMsg > sorter_1(g, 4, BufferCompressor(num_rcrd, sort_type));
    tbb::flow::function_node<BufferMsg, BufferMsg > sorter_2(g, 4, BufferCompressor(num_rcrd, sort_type));
    tbb::flow::function_node<BufferMsg, BufferMsg > sorter_3(g, 4, BufferCompressor(num_rcrd, sort_type));

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

    //make_edge(mapper, sorter);
    //make_edge(sorter_0, ordering_0);
    //make_edge(sorter_1, ordering_1);
    //make_edge(sorter_2, ordering_2);
    //make_edge(sorter_3, ordering_3);
    //make_edge(ordering, output_writer);
    //make_edge(ordering_0, output_writer_0);
    //make_edge(ordering_1, output_writer_1);
    //make_edge(ordering_2, output_writer_2);
    //make_edge(ordering_3, output_writer_3);

    file_reader_0.try_put(tbb::flow::continue_msg());
    file_reader_1.try_put(tbb::flow::continue_msg());
    file_reader_2.try_put(tbb::flow::continue_msg());
    file_reader_3.try_put(tbb::flow::continue_msg());

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


