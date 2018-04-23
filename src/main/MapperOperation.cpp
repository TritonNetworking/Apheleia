#include "KeyValueRecord.h"
#include "BaseBuffer.h"
#include "MapperOperation.h"

MapperOperation::MapperOperation(uint32_t num_rcrd) : num_record(num_rcrd), counter(0), nway(4) {}

void MapperOperation::operator() (BufferMsg buffer, Mapper::output_ports_type &op){
        //std::cout << "MapperOp"<< "-";
        //std::cout << "counter: " << counter << "\n";   
    if(!buffer.isLast){
        BufferMsg bufmsg_array[nway];
        std::vector<Buffer> basebuf_array;
        size_t chunkSize = buffer.inputBuffer.len;
        //BaseBuffer* basebuf_array = new BaseBuffer[nway];
        uint32_t basebuf_index[nway];
        //BaseBuffer* basebuf = new BaseBuffer();
        //basebuf->createRecords(num_record);
        //basebuf->setAllRecords(buffer.inputBuffer.b);


        //1. create BaseBuffer _basebuf
        for(int i = 0; i < nway; i++){
            Buffer outputBuffer;
            outputBuffer.b = new char[chunkSize/nway];
            outputBuffer.len = chunkSize/nway;
            //BaseBuffer* b = new BaseBuffer();
            //b->createRecords( uint32_t(num_record/nway) );
            //basebuf_array.push_back(b);
            basebuf_array.push_back(outputBuffer);
            basebuf_index[i]=0;
        }

        /*for(int j=0; j< num_record; j++){
            char* kbase=rawBuffer+100*j;
            char* vbase=kbase+10;
            //buffer[j]->setKey(kbase);
            //buffer[j]->setValue(vbase);
        }*/

        //2. distribute records to 4 buffers
        for(uint32_t i = 0; i < num_record; i++){
            char* rawBuffer = buffer.inputBuffer.b;
            char* kbase=rawBuffer+100*i;
            int split_way = kbase[0]%nway;
            //KeyValueRecord* kvr = basebuf->getRecords(i);
            //int split_way = kvr->getKey(0)%nway; // split it into four
            //basebuf_array[split_way]//->setRecord(kvr,basebuf_index[split_way]);
            char* dst_base= basebuf_array[split_way].b;
            std::memcpy(dst_base+basebuf_index[split_way], kbase, 100);
            basebuf_index[split_way]++;
        }


        for(int i = 0; i < nway; i++){
            bufmsg_array[i] = BufferMsg(buffer.inputBuffer, basebuf_array[i], buffer.seqId);
            if(i==0){
                std::get<0>(op).try_put(bufmsg_array[i]);
            }
            else if(i==1){
                std::get<1>(op).try_put(bufmsg_array[i]);
            }
            else if(i==2){
                std::get<2>(op).try_put(bufmsg_array[i]);
            }
            else{
                std::get<3>(op).try_put(bufmsg_array[i]);
            }
        }
        counter++; 
    }
    else{ // isLast == true
        BufferMsg bufmsg_array[nway];
        for(int i = 0; i < nway; i++){
            bufmsg_array[i] = BufferMsg(buffer.inputBuffer, buffer.outputBuffer, buffer.seqId);
            bufmsg_array[i].markLast(buffer.seqId);
            if(i==0){
                std::get<0>(op).try_put(bufmsg_array[i]);
            }
            else if(i==1){
                std::get<1>(op).try_put(bufmsg_array[i]);
            }
            else if(i==2){
                std::get<2>(op).try_put(bufmsg_array[i]);
            }
            else{
                std::get<3>(op).try_put(bufmsg_array[i]);
            }        
        }
    }             
}


