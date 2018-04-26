#ifndef IO_OPERATION_H
#define IO_OPERATION_H

#include <stdint.h>
#include <inttypes.h>

#include <iostream>
#include <fstream>
#include <string>
#include <memory>
#include <random>
#include "BufferMsg.h"
//#include "KeyValueRecord.h"

class IOOperations {
public:

    IOOperations(std::ifstream& inputStream, std::ofstream& outputStream, size_t chunkSize)
        : m_inputStream(inputStream), m_outputStream(outputStream), m_chunkSize(chunkSize), m_chunksRead(0) {}

    void readChunk(Buffer& buffer) {
        m_inputStream.read(buffer.b, m_chunkSize);
        buffer.len = static_cast<size_t>(m_inputStream.gcount());
        m_chunksRead++;
    }

    void readSocketChunk(Buffer& buffer){   
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