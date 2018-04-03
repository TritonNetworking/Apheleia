#include "BufferSorter.h"
#include <algorithm>
//#include <memory>
//#include "Histogram.h"
std::vector<uint64_t> std_lantency_list;
std::vector<uint64_t> radix_lantency_list;
std::vector<double> std_rate_list;
std::vector<double> radix_rate_list;

BufferSorter::BufferSorter(uint32_t num_rcrd, int _sorttype) : num_record(num_rcrd), sort_type(_sorttype), counter(0) {}

BufferMsg BufferSorter::operator()(BufferMsg buffer){
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

uint64_t BufferSorter::getNanoSecond(struct timespec tp){
	clock_gettime(CLOCK_MONOTONIC, &tp);
	return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
}
/*static bool BufferSorter::compareKey(KeyValueRecord* kvr1, KeyValueRecord* kvr2){
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
}*/

void BufferSorter::radix_sort(BaseBuffer* buf, BaseBuffer* output, uint32_t num_record){
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
}

