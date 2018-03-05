#define IO_OPERATION_H

#include <stdint.h>
#include <inttypes.h>

#include <iostream>
#include <fstream>
#include <string>
#include <memory>
#include <random>
#include "KeyValueRecord.h"

class BaseBuffer{
  public: 
    BaseBuffer(uint32_t num){
      buf_size= num;
      buffer = new KeyValueRecord[num];
    }

    uint32_t getBufferSize(){
      return buf_size;
    }

    void createRecords(){ //(uint32_t _ks, uint32_t _vs){
      //ksize = _ks;
      //vsize = _vs;
      ksize = 10;
      vsize= 90;
      for(uint32_t i = 0; i < buf_size; i++){
        buffer[i].initRecord(ksize, vsize);
      }
    }

    void setRecord(KeyValueRecord* kvr, int index){
      //for(int j=0; j< buf_size; j++){
      buffer[index]=*kvr;
      //}
    }

    void genRecordKeys(){
      std::random_device rd;
      std::default_random_engine gen = std::default_random_engine(rd());
      std::uniform_int_distribution<uint8_t> dis(0,255);
      
      for(int j=0; j< buf_size; j++){
        for (int index=0; index<ksize; index++){
          buffer[j].setKey(dis(gen),index);
        }
      }
      //std::cout << "some random numbers between 1 and 255: ";
    }

    void genRecordValues(){
      std::random_device rd;
      std::default_random_engine gen = std::default_random_engine(rd());
      std::uniform_int_distribution<uint8_t> dis(0,255);
      
      for(int j=0; j< buf_size; j++){
        for (int index=0; index<vsize; index++){
          buffer[j].setValue(dis(gen),index);
        }
      }
    }

    KeyValueRecord& getRecords(uint32_t index){
      return &(buffer[index]);
    }

  private:
  KeyValueRecord& buffer;
  uint32_t ksize;
  uint32_t vsize;  
  uint32_t buf_size;
};

class IOOperations {
public:

    IOOperations(std::ifstream& inputStream, std::ofstream& outputStream, size_t chunkSize)
        : m_inputStream(inputStream), m_outputStream(outputStream), m_chunkSize(chunkSize), m_chunksRead(0) {}

    void readChunk(Buffer& buffer) {
        m_inputStream.read(buffer.b, m_chunkSize);
        buffer.len = static_cast<size_t>(m_inputStream.gcount());
        m_chunksRead++;
    }

    void writeChunk(const Buffer& buffer) {
        m_outputStream.write(buffer.b, buffer.len);
    }

    size_t chunksRead() const {
        return m_chunksRead;
    }

    size_t chunkSize() const {
        return m_chunkSize;
    }

    bool hasDataToRead() const {
        return m_inputStream.is_open() && !m_inputStream.eof();
    }

private:

    std::ifstream& m_inputStream;
    std::ofstream& m_outputStream;

    size_t m_chunkSize;
    size_t m_chunksRead;
};

#endif //IO_OPERATION_H