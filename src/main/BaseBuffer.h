#ifndef BASE_BUFFER_H
#define BASE_BUFFER_H

#include <stdint.h>
#include <stdlib.h>

class BaseBuffer{
	public:
	BaseBuffer(uint8_t* buffer_, uint64_t size_){
	  size= size_;
    buffer = (uint8_t*)malloc(size_*sizeof(uint8_t));
    written=false;
    //buffer=buffer_;
	}

	uint8_t* getBuffer(){
    	return buffer;
  }

  uint64_t getSize() const {
    	return size;
  }

  void setWritten(bool w){
      written=w;
  }

  bool isWritten(){
      return written;
  }
	private:
  	uint8_t* buffer;
  	uint64_t size;
    bool written;
	
};

#endif //BASE_BUFFER
