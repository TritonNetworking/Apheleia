#include <iostream>
#include <random>
#include <string>

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <inttypes.h>
#include <algorithm>
#include <getopt.h>
#include <fstream>

#include "KeyValueRecord.h"
#include "Histogram.h"
#include "BucketTable.h"
#include <vector>
#define RADIX 0


class BaseBuffer{
  public: 
    BaseBuffer(uint32_t num){
      buf_size= num;
      buffer = new KeyValueRecord[num];
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

    void genRecordKeys(){
      std::random_device rd;
      std::default_random_engine gen = std::default_random_engine(rd());
      std::uniform_int_distribution<char> dis(0,255);
      
      for(int j=0; j< buf_size; j++){
        for (int index=0; index<ksize; index++){
          buffer[j].setKeyWithIndex(dis(gen),index);
        }
      }
      //std::cout << "some random numbers between 1 and 255: ";
    }

    void genRecordValues(){
      std::random_device rd;
      std::default_random_engine gen = std::default_random_engine(rd());
      std::uniform_int_distribution<char> dis(0,255);
      
      for(int j=0; j< buf_size; j++){
        for (int index=0; index<vsize; index++){
          buffer[j].setValueWithIndex(dis(gen),index);
        }
      }
    }

    KeyValueRecord* getRecords(uint32_t index){
      return &(buffer[index]);
    }

  private:
  KeyValueRecord* buffer;
  uint32_t ksize;
  uint32_t vsize;  
  uint32_t buf_size;
};

bool compareKey(KeyValueRecord* kvr1, KeyValueRecord* kvr2){
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

inline uint64_t getNanoSecond(struct timespec tp){
  clock_gettime(CLOCK_MONOTONIC, &tp);
  return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
}

inline uint64_t radix_sort(BaseBuffer* buf, BaseBuffer* output, uint32_t num_record){
//#ifndef RADIX
  uint32_t keysize = 10;
  uint32_t valuesize = 90;
  struct timespec ts1,ts2;
  //std::cout << "-Radix-Sort-\n";
  //std::cout << "records are unsorted" << "\n";
  for(uint32_t i = 0; i < num_record; i++){
    KeyValueRecord* kvr = buf->getRecords(i);
    /*for(uint32_t j = 0; j < keysize; j++){
      uint8_t k = kvr->getKey(j);
      //printf("%" PRIu8 " ",k);
      std::cout << +k << " ";
    }
    std::cout << "\n";*/
  }

  // simple sort test with Bucket and BT.
  Histogram histo;
  uint32_t radix = 256;
  BucketTable* BT = new BucketTable(&histo, radix);
  BT->setKeySize(keysize);
  BT->setValueSize(valuesize);
  BT->allocBuckets();

  for(uint32_t i = 0; i < num_record; i++){
    //std::cout << "unsorted record " << i << "\n";
    KeyValueRecord* kvr = buf->getRecords(i);
    //std::cout << "kvr_addr " << kvr << "\n";
    BT->distributeBuffer(kvr); // digit = keysize-1 used here
    //std::cout <<"bucketlength: "<<+BT->getBucketLength() << "\n";
  }

  //uint32_t digit = keysize - 1;
  //KeyValueRecord* pop_kvr = new KeyValueRecord();
  //pop_kvr->initRecord(10, 90);
  //std::cout << pop_kvr->getKey(digit) << "\n";
  //std::cout <<"bucketlength: "<<+BT->getBucketLength() << "\n";

uint64_t t1 =getNanoSecond(ts1);  

for(int digit = keysize - 2; digit >= 0; digit--){ 
  //std::cout << "digit:" << digit << "\n";
  for(uint32_t b_index=0; b_index < radix; b_index++){
    Bucket* bckt = BT->getBucket(b_index);
    bckt->setRecordCount();
    uint32_t num= bckt->getRecordCount();
    //std::cout << "Num " << +num << " ";
    // Iterate over Records in a bucket
    for(uint32_t r_index=0; r_index < num; r_index++){
        //std::cout << "bucket " << +b_index << ":";
        //std::cout << "digit " << +digit << "\n";
        KeyValueRecord* pop_kvr = bckt->popEntry();
        //std::cout << "digit:" << digit << "\n";
        uint8_t key = pop_kvr->getKey(digit);
        //***printf("digit: %d, key %" PRIu8 "\n", digit, key);
        Bucket* new_bckt = BT->getBucket(key);
        new_bckt->pushEntry(pop_kvr);
    }
  }
 }

 uint64_t t2 =getNanoSecond(ts2);   

//TODO: move semantics for concurrent_vector and concurrent_queue
// Verify sorted result
int index=0;
for(uint32_t b_index=0; b_index < radix; b_index++){
    Bucket* bckt = BT->getBucket(b_index);
    bckt->setRecordCount();
    uint32_t num= bckt->getRecordCount();
    // Iterate over Records in a bucket
    //std::cout << "bucket " << +b_index << ":\n";
    for(uint32_t r_index=0; r_index < num; r_index++){
        //std::cout << "Num " << +num << " ";
        //std::cout << +r_index << ": ";
        //std::cout << "digit " << +digit << "\n";
        KeyValueRecord* pop_kvr = bckt->getEntry(r_index);
        /*for(uint32_t j = 0; j < keysize; j++){
        uint8_t k = pop_kvr->getKey(j);
          std::cout << +k << " ";
        }
        std::cout << "\n";*/
        output->setRecord(pop_kvr, index);
        index++;
    }
    //std::cout << "-----------------\n";
}

return t2-t1;

std::cout << "records are sorted"<< "\n";
for(uint32_t i = 0; i < num_record - 1; i++){
    //std::cout << "sorted record " << +i << "\n";
    KeyValueRecord* kvr = output->getRecords(i);
    KeyValueRecord* kvr2 = output->getRecords(i+1);
    if( compareKey(kvr,kvr2) == false){
      //std::cout << "unsorted" << " ";
      std::cout << "unsorted record " << +i << "\n";
      //std::cout << "unsorted record " << +i << "\n";
      for(uint32_t j = 0; j < keysize; j++){
        uint8_t k = kvr->getKey(j);
        std::cout << +k << " ";
      }
      std::cout << "\n";
    }
}
//std::cout << "sorted radix sort record ends"<< "\n";


}

inline uint64_t std_sort(BaseBuffer* buf, BaseBuffer* output, uint32_t num_record){
  //std::cout << "-Quick-Sort-\n";
  struct timespec ts1,ts2;
  std::vector<KeyValueRecord*> ptr_list;
  for(uint32_t i = 0; i < num_record; i++){
    //std::cout << "unsorted record " << i << "\n";
    KeyValueRecord* kvr = buf->getRecords(i);
    ptr_list.push_back(kvr);
  }
  uint64_t t1 =getNanoSecond(ts1);
  std::sort(ptr_list.begin(), ptr_list.end(), compareKey);
  uint64_t t2 =getNanoSecond(ts2);

  for(uint32_t index = 0; index < num_record; index++){
    //std::cout << "sorted std::sort record " << index << "\n";
    KeyValueRecord* kvr =ptr_list.at(index);
    output->setRecord(kvr, index);
    //for(uint32_t j = 0; j < keysize; j++){
    //  std::cout << +ptr_list.at(i)->getKey(j) << " ";
    //}
    //std::cout << "\n";
  }
  //std::cout << "sorted std::sort record ends"<< "\n";
  return t2-t1;
}


int main(int argc, char** argv) {
  char *p;
  uint32_t iteration;
  uint64_t Kbytes;
  int option=0;
  std::ofstream std_time_file, std_rate_file, radix_time_file, radix_rate_file;
  //bytes={64KB, 128KB, 256KB, 512KB, 768KB, 1024KB}

  while ((option = getopt(argc, argv,"i:b:")) != -1) {
        switch (option) {
            case 'i' : 
                iteration = strtol(optarg,&p, 10);
                std::cout << "iteration: " << iteration<<"\n";
                break;
            case 'b': 
                Kbytes = strtol(optarg,&p, 10);
                std::cout << "Kbytes: " << Kbytes<<"\n";
                break;
            default: 
                //print_usage(); 
                exit(EXIT_FAILURE);
        }
  }

  uint32_t keysize = 10;
  uint32_t valuesize = 90;
  //uint32_t num_record = 1024;
  uint32_t num_record = 1000*Kbytes/(keysize+valuesize);
  std::cout<< "num_record: " << num_record << "\n";
  BaseBuffer* buf = new BaseBuffer(num_record);
  buf->createRecords();
  buf->genRecordKeys();
  buf->genRecordValues();

  BaseBuffer* output = new BaseBuffer(num_record);
  output->createRecords();

  std::vector<uint64_t> std_lantency_list;
  std::vector<uint64_t> radix_lantency_list;
  std::vector<double> std_rate_list;
  std::vector<double> radix_rate_list;

  //uint64_t mean_std_time=0;
  //uint64_t mean_std_rate=0;
  uint64_t sum_radix_time=0;
  uint64_t sum_std_time=0;
  
  struct timespec ts1,ts2,ts3,ts4;
  uint64_t sorted_bits= 8*Kbytes*1000;
  double sort_rate=0; 

  /*std::string extension = ".txt";
  std::string std_time_str = "std_time_iter_" + std::to_string(iteration) + 
    "_buffer_" + std::to_string(Kbytes) + extension;

  std::string std_rate_str = "std_rate_iter_" + std::to_string(iteration) + 
    "_buffer_" + std::to_string(Kbytes) + extension;  

  std::string radix_time_str = "radix_time_iter_" + std::to_string(iteration) + 
    "_buffer_" + std::to_string(Kbytes) + extension; 

  std::string radix_rate_str = "radix_rate_iter_" + std::to_string(iteration) + 
    "_buffer_" + std::to_string(Kbytes) + extension; */


  /*std_time_file.open(std_time_str);
  std_rate_file.open(std_rate_str);
  radix_time_file.open(radix_time_str);
  radix_rate_file.open(radix_rate_str);*/

  /*for(uint64_t i = 0; i < iteration; i ++){
    uint64_t t1 =getNanoSecond(ts1);
    uint64_t sort_time=std_sort(buf, output, num_record);
    uint64_t t2 =getNanoSecond(ts2);
    uint64_t all_time = t2 - t1;
    sum_std_time = sum_std_time + sort_time;
    std_time_file << sort_time << "\n";
    sort_rate=(double)sorted_bits/(double)sort_time; 
    std_rate_file << sort_rate << "\n";
    //std_lantency_list.push_back(sort_time);
    //std_rate_list.push_back(sort_rate);
  }*/

  for(uint64_t i = 0; i < iteration; i ++){
    std::cout << " radix sort\n";
    uint64_t t1 =getNanoSecond(ts3);
    uint64_t sort_time=radix_sort(buf, output, num_record);
    uint64_t t2 =getNanoSecond(ts4);
    uint64_t all_time = t2 - t1;
    //sum_radix_time = sum_radix_time + sort_time;
    //radix_time_file << sort_time << "\n";
    //sort_rate=(double)sorted_bits/(double)sort_time; 
    //radix_rate_file << sort_rate << "\n";
    //radix_rate_list.push_back(sort_rate);
    //radix_lantency_list.push_back(sort_time);
  }

  /*double mean_std_time = (double) sum_std_time / (double) iteration;
  double mean_radix_time = (double) sum_radix_time / (double) iteration;
  std::cout<< "mean std sort time: " << mean_std_time << "\n";
  std::cout<< "mean radix sort time: " << mean_radix_time << "\n";

  std_time_file.close();
  std_rate_file.close();
  radix_time_file.close();
  radix_rate_file.close();*/

  return 0;
}