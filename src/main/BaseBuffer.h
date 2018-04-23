#ifndef BASE_BUFFER_H
#define BASE_BUFFER_H

//#include <stdint.h>
#include <cstdint>
#include <vector>
#include "KeyValueRecord.h"
//#include "tbb/tbb_allocator.h"

class BaseBuffer{
  public: 
    BaseBuffer();

    //TODO: fix deconstructors
    ~BaseBuffer();

    void createRecords(uint32_t num);

    void setAllRecords(char* rawBuffer);

	  inline uint32_t getBufferSize(){
      return buf_size;
    }

    inline void setRecord(KeyValueRecord* kvr, int index){
      buffer[index]=kvr;
    }

    inline KeyValueRecord* getRecords(uint32_t index){
      return buffer[index];
    }

    inline uint32_t getKeySize(){
        return ksize;
    }

    inline uint32_t getValueSize(){
        return vsize;
    }

  	private:
  	//KeyValueRecord* buffer;
  	std::vector<KeyValueRecord*> buffer;
  	uint32_t ksize;
  	uint32_t vsize;  
  	uint32_t buf_size;
};

#endif //BASE_BUFFER
