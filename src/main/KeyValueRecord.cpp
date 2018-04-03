//#include <stdint.h>
//#include <stdlib.h>
//#include <unistd.h>
#include <iostream>
#include <cstdint>
#include <cstring>
#include "KeyValueRecord.h"

KeyValueRecord::KeyValueRecord(uint32_t ksize_, uint32_t vsize_){	
	keyBuffer = new char[ksize_];
	valueBuffer = new char[vsize_];
}

void KeyValueRecord::initRecord(uint32_t ksize_, uint32_t vsize_){
	//keySize= ksize_;
	//valueSize= vsize_;
	memset(keyBuffer,0,ksize_*sizeof(char));
	memset(valueBuffer,0,vsize_*sizeof(char));
}

KeyValueRecord::~KeyValueRecord(){
	//std::cout << "KV destructor" <<"\n";
	if(keyBuffer && valueBuffer){
		delete [] valueBuffer;
		delete [] keyBuffer;
	}
}

/*void KeyValueRecord::setKey(char* key){
	keyBuffer=key;
}

void KeyValueRecord::setValue(char* value){
	valueBuffer=value;
}*/

//TODO to be safer, use reference instead of pointer?

void KeyValueRecord::setProcessed(bool setting){
	processed=setting;
}

bool KeyValueRecord::getProcessed(){
	return processed;
}

//[ uint64_t keySize, uint32_t valueSize, uint8_t* key, uint8_t* value ]
