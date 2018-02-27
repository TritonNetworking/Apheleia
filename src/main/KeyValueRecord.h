#ifndef KV_RECORD_H
#define KV_RECORD_H

#include <stdint.h>
#include <stdlib.h>

class KeyValueRecord{
	public:
	KeyValueRecord();
	~KeyValueRecord();	
	void initRecord(uint32_t ksize_, uint32_t vsize_);
	//TODO to be safer, use reference instead of pointer
	inline uint8_t getKey(int index){
		return keyBuffer[index];
	}

	inline uint8_t getValue(int index){
		return valueBuffer[index];
	}

	void setKey(uint32_t value, uint32_t index);
	void setValue(uint32_t value, uint32_t index);
	//inline uint32_t getKeySize() const;
	//inline uint32_t getValueSize() const;
	void setProcessed(bool setting);
	bool getProcessed();

  	//[ uint64_t keySize, uint32_t valueSize, uint8_t* key, uint8_t* value ]
	private:
	uint8_t* keyBuffer;
  	uint8_t* valueBuffer;
  	//uint32_t keySize;
  	//uint32_t valueSize;
    bool processed;
	
};

#endif //KV_RECORD_H
