#include "KeyValueRecord.h"
#include "BaseBuffer.h"
#include "MapperOperation.h"

MapperOperation::MapperOperation(uint32_t num_rcrd) : num_record(num_rcrd), counter(0), nway(4) {}

void MapperOperation::operator() (BufferMsg buffer, Mapper::output_ports_type &op){
        //std::cout << "MapperOp"<< "-";
        //std::cout << "counter: " << counter << "\n";   
    if(!buffer.isLast){
        BufferMsg bufmsg_array[nway];
        std::vector<BaseBuffer*> basebuf_array;
        //BaseBuffer* basebuf_array = new BaseBuffer[nway];
        uint32_t basebuf_index[nway];
        BaseBuffer* basebuf = new BaseBuffer();
        basebuf->createRecords(num_record);
        basebuf->setAllRecords(buffer.inputBuffer.b);

        //1. create BaseBuffer _basebuf
        for(int i = 0; i < nway; i++){
            BaseBuffer* b = new BaseBuffer();
            b->createRecords( uint32_t(num_record/nway) );
            basebuf_array.push_back(b);
            basebuf_index[i]=0;
        }

        //2. distribute records to 
        for(uint32_t i = 0; i < num_record; i++){
            KeyValueRecord* kvr = basebuf->getRecords(i);
            int split_way = kvr->getKey(0)%nway; // split it into four
            basebuf_array[split_way]->setRecord(kvr,basebuf_index[split_way]);
            basebuf_index[split_way]++;
        }


        for(int i = 0; i < nway; i++){
            bufmsg_array[i] = BufferMsg(buffer.inputBuffer, buffer.outputBuffer, basebuf_array[i], buffer.seqId);
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
            bufmsg_array[i] = BufferMsg(buffer.inputBuffer, buffer.outputBuffer, buffer.basebuf, buffer.seqId);
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


