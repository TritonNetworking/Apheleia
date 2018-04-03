#ifndef KV_RECORD_H
#define KV_RECORD_H

//#include <stdint.h>
#include <cstdint>
//#include <stdlib.h>

class KeyValueRecord{
	public:
	KeyValueRecord(uint32_t ksize_, uint32_t vsize_);
	~KeyValueRecord();	
	void initRecord(uint32_t ksize_, uint32_t vsize_);
	//TODO to be safer, use reference instead of pointer
	inline char getKey(int index){
		return keyBuffer[index];
	}

	inline char getValue(int index){
		return valueBuffer[index];
	}

	inline char* getKeyBuffer(){
		return keyBuffer;
	}

	inline char* getValueBuffer(){
		return valueBuffer;
	}

	inline void setKey(char* key){
		keyBuffer=key;
	}
	inline void setValue(char* value){
		valueBuffer=value;
	}
	//inline uint32_t getKeySize() const;
	//inline uint32_t getValueSize() const;
	void setProcessed(bool setting);
	bool getProcessed();

  	//[ uint64_t keySize, uint32_t valueSize, uint8_t* key, uint8_t* value ]
	private:
	char * keyBuffer;
  	char * valueBuffer;
  	//uint32_t keySize;
  	//uint32_t valueSize;
    bool processed;
	
};

#endif //KV_RECORD_H
