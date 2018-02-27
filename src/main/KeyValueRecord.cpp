#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "KeyValueRecord.h"

KeyValueRecord::KeyValueRecord(){	
}

void KeyValueRecord::initRecord(uint32_t ksize_, uint32_t vsize_){
	//keySize= ksize_;
	//valueSize= vsize_;
	keyBuffer = new uint8_t[ksize_];
	valueBuffer = new uint8_t[vsize_];
	memset(keyBuffer,0,ksize_*sizeof(uint8_t));
	memset(valueBuffer,0,vsize_*sizeof(uint8_t));
}

KeyValueRecord::~KeyValueRecord(){
	if(keyBuffer && valueBuffer){
		delete [] valueBuffer;
		delete [] keyBuffer;
	}
}

void KeyValueRecord::setKey(uint32_t value, uint32_t index){
	keyBuffer[index] = value;
}

void KeyValueRecord::setValue(uint32_t value, uint32_t index){
	valueBuffer[index] = value;
}

//TODO to be safer, use reference instead of pointer?

void KeyValueRecord::setProcessed(bool setting){
	processed=setting;
}

bool KeyValueRecord::getProcessed(){
	return processed;
}

//[ uint64_t keySize, uint32_t valueSize, uint8_t* key, uint8_t* value ]
