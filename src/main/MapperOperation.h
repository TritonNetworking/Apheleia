#ifndef MAPPER_OP_H
#define MAPPER_OP_H

#include <cstdint>
#include "BufferMsg.h"
#include "tbb/flow_graph.h"

typedef tbb::flow::multifunction_node<BufferMsg, tbb::flow::tuple<BufferMsg,BufferMsg,BufferMsg,BufferMsg> > Mapper;

class MapperOperation{

    public:
    MapperOperation(uint32_t num_rcrd);
    void operator() (BufferMsg buffer, Mapper::output_ports_type &op);

	private:
    uint32_t nway;
    uint32_t counter;
    uint32_t num_record;
};

#endif
