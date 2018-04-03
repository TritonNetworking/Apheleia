#ifndef BUFFER_MSG_H
#define BUFFER_MSG_H

#include <cstdint>
#include <iostream>
#include <memory>
#include "BaseBuffer.h"

struct Buffer {
    size_t len;
    char* b;
};

struct BufferMsg {

    BufferMsg() {}
    BufferMsg(Buffer& inputBuffer, Buffer& outputBuffer, BaseBuffer* _basebuf, size_t seqId, bool isLast = false)
        : inputBuffer(inputBuffer), outputBuffer(outputBuffer), basebuf(_basebuf), seqId(seqId), isLast(isLast){}

    //TODO add one more argument for num_record  
    static BufferMsg createBufferMsg(size_t seqId, size_t chunkSize, uint32_t num_rcrd) {
        //num_record = num_rcrd;

        Buffer _inputBuffer;
        _inputBuffer.b = new char[chunkSize];
        _inputBuffer.len = chunkSize;

        Buffer _outputBuffer;
        _outputBuffer.b = new char[chunkSize];
        _outputBuffer.len = chunkSize;

        BaseBuffer* _basebuf = new BaseBuffer(); // if basebuf is hust a reference, then its underlying pointers can be an issue
        _basebuf->createRecords(num_rcrd);
        _basebuf->setAllRecords(_inputBuffer.b);

        return BufferMsg(_inputBuffer, _outputBuffer, _basebuf, seqId);
    }

    static void destroyBufferMsg(const BufferMsg& destroyMsg) {
        delete[] destroyMsg.inputBuffer.b;
        delete[] destroyMsg.outputBuffer.b;
        delete destroyMsg.basebuf;
    }

    void markLast(size_t lastId) {
        isLast = true;
        seqId = lastId;
    }

    size_t seqId;
    Buffer inputBuffer;
    Buffer outputBuffer;
    BaseBuffer* basebuf;
    bool isLast;
    uint32_t num_record;
};

#endif
