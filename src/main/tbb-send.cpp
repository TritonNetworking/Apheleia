#include <iostream>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <inttypes.h>
#include "tbb/tbb_allocator.h"
#include "tbb/flow_graph.h"
#include "tbb/concurrent_queue.h"
#include "tbb/tick_count.h"
#include "Socket.h"

using namespace std;
using namespace tbb;
using namespace tbb::flow;

struct TupleBuffer {
	uint8_t * _data;
	uint64_t _capacity;
	uint64_t _size;

	TupleBuffer(uint64_t capacity = (1024 * 1024 * 1))
		: _capacity(capacity), _size(capacity) {
		_data = new uint8_t[_capacity];
		memset(_data, 0, _capacity);
	}

	~TupleBuffer() {
		if (_data) {
			delete [] _data;
		}
	}
	
	/*bool markLast(){
   		capacity=0;
	}*/
	
	uint8_t* getBuffer(){
		return _data;
	}

	inline uint64_t size() const { return _size; }
	inline uint64_t capacity() const { return _capacity; }
};

struct Sourcer {
	const int LIMIT = 10000;
	int count;

	Sourcer() {
		count = 0;		
	}

	bool operator()(TupleBuffer *&v) {
		if (count < LIMIT) {
			++count;
			v = new TupleBuffer();
			return true;
		} else {
			cout << "Source emitted " << LIMIT << " buffers" << endl;
			return false;
		}
	}
};

struct Sender {
	const int LIMIT = 10000;
	int count;
	uint64_t sent_bytes;
	std::string ipaddr= "10.1.100.31"; // server
    std::string port="8000";
    uint64_t Retry=100;
    uint64_t retryDelayInMicros=1000;
	Socket* send_socket;

	Sender() {
		count = 0;	
		sent_bytes = 0;
		Socket* send_socket=new Socket();	
		send_socket->tcpConnect(ipaddr, port, 1024*1024, retryDelayInMicros, Retry);
	}

	/*	
	~Sender(){
		send_socket->tcpClose();
	}*/

	TupleBuffer * operator()(TupleBuffer *v) {
		if (count < LIMIT) {
			//send_socket->tcpConnect(ipaddr, port, 1024*1024, retryDelayInMicros, Retry);
			++count;
			uint64_t num_bytes = send_socket->tcpSend(v->getBuffer(), v->size());
			sent_bytes = sent_bytes + num_bytes;
			return v;
		} else {
			send_socket->tcpClose();
			cout << "send " << sent_bytes << " bytes" << endl;
			return v;
		}
	}
};

struct Worker_NOP {
	TupleBuffer * operator()(TupleBuffer * v) {
		return v;
	}
};

struct PrintIt {
	continue_msg operator()(TupleBuffer * v) {
		delete v;
		return continue_msg();
	}
};

struct PrintIt2 {
	continue_msg operator()(TupleBuffer * v) {
		delete v;
		return continue_msg();
	}
};

int main(int argc, char* argv[])
{
	graph producer_side;
	//graph consumer_side;

	source_node<TupleBuffer*> src(producer_side, Sourcer(), /* is_active */ false );
	//function_node<TupleBuffer*, TupleBuffer*> func1(producer_side, unlimited, Worker_NOP());
	function_node<TupleBuffer*, TupleBuffer*> send(producer_side, 1 , Sender());

	make_edge(src, send);
	//make_edge(func1, send);

	//source_node<TupleBuffer*> src2(consumer_side, MySource(), /* is_active */ false );
	//function_node<TupleBuffer*,TupleBuffer*> func2(consumer_side, unlimited, Worker_NOP());
	//function_node<TupleBuffer*> sink2(consumer_side, unlimited, PrintIt());

	//make_edge(src2, func2);
	//make_edge(func2, sink2);

	tick_count t_start = tick_count::now();
	src.activate();
	//src2.activate();
	producer_side.wait_for_all();
	//consumer_side.wait_for_all();
	tick_count t_end = tick_count::now();

	cout << "Total runtime: " << (t_end - t_start).seconds() << " sec" << endl;

	return 0;
}
