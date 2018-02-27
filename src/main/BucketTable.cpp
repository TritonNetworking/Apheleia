#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include "tbb/concurrent_queue.h"

#include "BucketTable.h"

BucketTable::BucketTable(Histogram* histo, uint32_t _radix)
:histogram(histo),
radix(_radix)
{
}

uint32_t BucketTable::getCurrentDigit(){
 return currentdigit;
}

uint32_t BucketTable::getValueSize() const{
	return valueSize;
}

uint32_t BucketTable::getKeySize() const{
	return keySize;
}

void BucketTable::setKeySize(uint32_t ks){
	keySize = ks;
}
void BucketTable::setValueSize(uint32_t vs){
	valueSize = vs;
}

void BucketTable::allocBuckets(){	
	currentdigit = keySize-1;
	//std::cout <<"allocBuckets: "<<radix << "\n";
	for(int i=0; i < radix; i++){
		Bucket* bckt = new Bucket(radix, keySize, valueSize);
		bucket_safelist.push_back(bckt);
		//std::cout<<"bucket size"<< bucket_safelist.size() << "\n";
	}
}

/*void BucketTable::advanceDigit(){
    currentdigit--;
}

uint32_t BucketTable::getDigit(){
    return currentdigit;
}

bool BucketTable::checkSortingEnds(){
    // e.g. keySize= 10 , current digit = 0 to (10-1). 
    // It ends when current digit==9
    if(currentdigit < keySize-1){
      return false;
    }
    else{
      return true;
    }
}*/




/*bb::concurrent_vector<Bucket*> BucketTable::getBuckets(){
	return bucket_safelist;
}*/ 

void BucketTable::distributeBuffer(KeyValueRecord* kv){
	uint8_t LSDkey= kv->getKey(currentdigit);
	//std::cout << +LSDkey << "\n";
	// take the least significant byte as LSDkey, so we don't need to do mod since we use a 256 radix ine the first place
	Bucket* bckt = bucket_safelist.at(LSDkey);
	bckt->pushEntry(kv);
}

bool BucketTable::clearHistogram(){
    histogram->reset();
}

