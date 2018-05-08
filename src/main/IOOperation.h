#ifndef IO_OPERATION_H
#define IO_OPERATION_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/stat.h>

#include <iostream>
#include <fstream>
#include <string>
#include <memory>
#include <random>
#include "BufferMsg.h"
//#include "KeyValueRecord.h"

class IOOperations {
public:

    IOOperations(std::ifstream& inputStream, std::ofstream& outputStream, size_t chunkSize, std::string inputName, std::string outputName)
        : m_inputStream(inputStream), m_outputStream(outputStream), m_chunkSize(chunkSize), m_chunksRead(0) 
        , inputFilename(inputName), outputFilename(outputName){}


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

    void incrementChunks(){
        m_chunksRead++;
    }

    size_t chunkSize() const {
        return m_chunkSize;
    }

    //BUG can happen if there's no \n at the last line,
    //it would read the last line twice
    //https://softwareengineering.stackexchange.com/questions/318081/why-does-ifstream-eof-not-return-true-after-reading-the-last-line-of-a-file
    bool hasDataToRead() const {
        return m_inputStream.is_open() && !m_inputStream.eof(); 
    }

    size_t getReadmIters(){
        struct stat stat_buf;
        int rc = stat(inputFilename.c_str(), &stat_buf);
        if(rc == 0){            
            m_fileSize= (size_t) stat_buf.st_size;
            m_iteration = m_fileSize/m_chunkSize;
            return m_iteration;
        }
        else{
            return 0;
        }
        //return rc == 0 ? stat_buf.st_size : -1;
    }


private:
    std::string inputFilename;
    std::string outputFilename;
    std::ifstream& m_inputStream;
    std::ofstream& m_outputStream;

    size_t m_chunkSize;
    size_t m_chunksRead;
    size_t m_fileSize;
    size_t m_iteration;
};

#endif //IO_OPERATION_H