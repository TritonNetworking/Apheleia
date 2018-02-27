#ifndef BASE_BUFFER_H
#define BASE_BUFFER_H

#include <stdint.h>
#include <stdlib.h>
#include "tbb/tbb_allocator.h"

class BaseBuffer{
	public:
	BaseBuffer(uint64_t size_){
		size= size_;
		buffer = new uint8_t[size];
		memset(buffer,0,size);
    	//buffer = (uint8_t*)malloc(size_*sizeof(uint8_t));
    	//written=false;
	}

	~BaseBuffer(){
		if(buffer)
			delete [] buffer;
	}	
	
	/*
	static BaseBuffer* allocate( size_t size_ ) {
        // +1 leaves room for a terminating null character.
        BaseBuffer* b = (BaseBuffer*)tbb::tbb_allocator<uint8_t>().allocate( sizeof(BaseBuffer)*size_);
		//size = size_;
        return b;
    }
    //! Free a TextSlice object 
    void dealloc(size_t size_) {
        tbb::tbb_allocator<uint8_t>().deallocate((uint8_t*)this,sizeof(BaseBuffer)*size_);
    }*/
 
	uint8_t* getBuffer(){
    	return buffer;
  	}

	uint64_t getSize() const {
    	return size;
  	}
	/*
  	void setWritten(bool w){
		written=w;
  	}

  	bool isWritten(){
      return written;
	}*/

	private:
  	uint8_t* buffer;
  	uint64_t size;
    //bool written;
	
};

#endif //BASE_BUFFER
