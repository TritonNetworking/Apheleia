#ifndef RADIXSORT_BUCKET_H
#define RADIXSORT_BUCKET_H

#include <string.h>
#include "KeyValueRecord.h"
#include "Histogram.h"
#include <deque>
//#include "tbb/concurrent_queue.h"  //<-- for concurrent_queue

class Bucket {
public:
  /// Constructor
  Bucket(uint32_t _radix, uint32_t ks, uint32_t vs){
     radix=_radix; 
     //currentdigit=_currentdigit;
     keySize=ks;
     valueSize=vs;
     record_count=0;
  }

  /*void advanceDigit(){
    currentdigit--;
  }

  uint32_t getDigit(){
    return currentdigit;
  }
  bool checkSortingEnds(){
    // e.g. keySize= 10 , current digit = 0 to (10-1). 
    // It ends when current digit==9
    if(currentdigit < keySize-1){
      return false;
    }
    else{
      return true;
    }
  }*/

  void setRecordCount(){
    //record_count=kv_safelist.unsafe_size(); //<-- for concurrent_queue
    record_count=kv_safelist.size();
  }

  uint32_t getRecordCount(){
    return record_count;
  }

  bool checkRoundEnds(){
    if(record_count==0){
      return true;
    }
    else{
      return false;
    }
  }

  inline void pushEntry(KeyValueRecord* entry){
    kv_safelist.push_back(entry);
    /*for (auto addr : kv_safelist) // access by value, the type of i is int
        std::cout << addr << ' ';
    std::cout << '\n';*/
  }

  //inline void popEntry(KeyValueRecord* entry){ //<-- for concurrent_queue
  inline KeyValueRecord* popEntry(){
    KeyValueRecord* kvr = kv_safelist.front();
    /*for (auto addr : kv_safelist) // access by value, the type of i is int
        std::cout << addr << ' ';
    std::cout << '\n';*/
    kv_safelist.pop_front();
    return kvr;
    /*
    if(record_count > 0){
      //kv_safelist.try_pop(entry); //<-- for concurrent_queue
     }
     else{
      std::cout << "no more elements" << "\n";
     }*/ 
  }

  inline KeyValueRecord* getEntry(uint32_t index){
    KeyValueRecord* kvpair = kv_safelist.at(index);
    return kvpair;
  }

private:
  //tbb::concurrent_queue<KeyValueRecord*> kv_safelist; //<-- for concurrent_queue
  std::deque<KeyValueRecord*> kv_safelist;
  uint32_t keySize;
  uint32_t valueSize;
  uint32_t record_count;
  uint32_t radix;
  //uint32_t currentdigit; // current digit ptr to BucketTable's current digit
};

#endif // RADIXSORT_BUCKET_H
