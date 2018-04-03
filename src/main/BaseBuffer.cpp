#include "BaseBuffer.h"
#include <iostream>

BaseBuffer::BaseBuffer(){}

BaseBuffer::~BaseBuffer(){
	//std::cout << " BaseBuffer destructor" <<"\n";
	buffer.erase(buffer.begin(), buffer.end());
}

void BaseBuffer::createRecords(uint32_t num){ //(uint32_t _ks, uint32_t _vs){
	   //ksize = _ks;
      //vsize = _vs;
      buf_size= num;
      //buffer.reserve(buf_size);
      ksize = 10;
      vsize= 90;
      //buffer = new KeyValueRecord[num];
      for(uint32_t i = 0; i < buf_size; i++){
      	//BUG
        KeyValueRecord* kvr = new KeyValueRecord(ksize, vsize);
        //buffer.emplace_back(); //KeyValueRecord* kvr = buffer.back();
        kvr->initRecord(ksize, vsize); //BUG??
        buffer.push_back(kvr);
        //std::cout << "yoo\n";
      }
}

void BaseBuffer::setAllRecords(char* rawBuffer){
      for(int j=0; j< buf_size; j++){
        char* kbase=rawBuffer+100*j;
        char* vbase=kbase+10;
        buffer[j]->setKey(kbase);
        buffer[j]->setValue(vbase);
      }
}