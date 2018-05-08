#ifndef BUFFER_SORTER_H
#define BUFFER_SORTER_H

//#include <stdint.h>
//#include <stdlib.h>
#include <vector>
#include "KeyValueRecord.h"
#include "BufferMsg.h"
#include "BaseBuffer.h"
#include "BucketTable.h"
//#include <vector>
#include <cstring>
#include <memory>

//extern std::vector<uint64_t> std_lantency_list;
//extern std::vector<uint64_t> radix_lantency_list;
//extern std::vector<double> std_rate_list;
//extern std::vector<double> radix_rate_list;

class BufferSorter {

public:
    BufferSorter(uint32_t num_rcrd, int _sorttype);
    BufferMsg operator()(BufferMsg buffer);
  //std::vector<uint64_t> std_lantency_list;
	//std::vector<uint64_t> radix_lantency_list;
	//std::vector<double> std_rate_list;
	//std::vector<double> radix_rate_list;


	uint64_t getNanoSecond(struct timespec tp);
	void radix_sort(BaseBuffer* buf, BaseBuffer* output, uint32_t num_record);
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
  
private:
	uint32_t num_record;
    int sort_type;
    uint32_t counter;
};

#endif
