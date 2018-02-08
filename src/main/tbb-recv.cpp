#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <inttypes.h>
#include "tbb/tbb_allocator.h"
#include "tbb/flow_graph.h"
#include "tbb/compat/thread"
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
	uint8_t* getBuffer(){
		return _data;
	}

	inline uint64_t size() const { return _size; }
	inline uint64_t capacity() const { return _capacity; }
};


struct Receiver {
	const int LIMIT = 10000;
	int count;
	//std::string ipaddr ("10.1.100.31"); // server
    std::string port="8000";
	uint64_t timeoutInMicros= 1000*1000*60;
	int backlogSize=10;
	Socket* tcp_socket;
	uint64_t recv_bytes;

	Receiver() {
		count = 0;	
		tcp_socket=new Socket();
		cout << "Receiver"<<"\n";	
		//tcp_socket->tcpListen(port, backlogSize);
		//sleep(10);
	}
	
	
	/*~Receiver() {	
		tcp_socket->tcpClose();
	}*/

	//bool operator()(TupleBuffer *&v) {
	TupleBuffer* operator()(Socket* sock_ptr) {
		tcp_socket=sock_ptr;
		TupleBuffer* v = new TupleBuffer();
		//Socket* new_socket=tcp_socket->tcpAccept(timeoutInMicros, v->size());
		if (count < LIMIT) {
			cout << "Recv-op"<<"\n";
			++count;
			uint64_t num_bytes = tcp_socket->tcpReceive(v->getBuffer(), v->size());
			recv_bytes = recv_bytes + num_bytes;
			cout << count << "\n";
			return v;
		} else {
			//tcp_socket->tcpClose();
			cout << "recv " << recv_bytes << " bytes" << endl;
			return v;
		}
	}
};


struct Listener {
	const int LIMIT = 10000;
	int count;
	//std::string ipaddr ("10.1.100.31"); // server
    std::string port="8000";
	uint64_t timeoutInMicros= 1000*1000*60;
	int backlogSize=100;
	Socket* tcp_socket;
	uint64_t recv_bytes;

	Listener() {
		cout << "Listener"<<"\n";
		count = 0;	
		tcp_socket=new Socket();	
		tcp_socket->tcpListen(port, backlogSize);
		//sleep(10);
	}
	
	
	//~Listener() {	
	//	tcp_socket->tcpClose();
	//}

	bool operator()(Socket *&new_socket) {
	//bool TupleBuffer* operator()() {
		//TupleBuffer* v = new TupleBuffer();
		//Socket* new_socket=tcp_socket->tcpAccept(timeoutInMicros, v->size());
		if (count < LIMIT) {
			++count;
			cout << "Listener_Operator"<<"\n";
			tick_count t_start = tick_count::now();
			Socket* new_socket=tcp_socket->tcpAccept(timeoutInMicros, 1024*1024);
			tick_count t_end = tick_count::now();
			cout << "Total runtime: " << (t_end - t_start).seconds() << " sec" << endl;
			return true;
		} 
		else {
			cout << "Listener_false"<<"\n";
			//tcp_socket->tcpClose();
			//cout << "recv " << recv_bytes << " bytes" << endl;
			return false;
		}
	}
};



struct Worker_NOP {
	TupleBuffer * operator()(TupleBuffer * v) {
		return v;
	}
};

struct Sinker {
	continue_msg operator()(TupleBuffer * v) {
		delete v;
		return continue_msg();
	}
};


/*
typedef async_node<continue_msg, Socket* > async_socket_reader_node;

class AsyncNodeActivity {
public:
	AsyncNodeActivity(){}

    ~AsyncNodeActivity() {
        m_fileReaderThread.join();
        //m_fileWriterThread.join();
    }

    void submitRead(async_socket_reader_node::gateway_type& gateway) {
        gateway.reserve_wait();
        std::thread(&AsyncNodeActivity::readingLoop, this, std::ref(gateway)).swap(m_fileReaderThread);
    }
	
private:	
	void readingLoop(async_socket_reader_node::gateway_type& gateway) {
		tcp_socket=new Socket();
        tcp_socket->tcpListen(port, backlogSize);
        //while (m_io.hasDataToRead()) {
        //    BufferMsg bufferMsg = BufferMsg::createBufferMsg(m_io.chunksRead(), m_io.chunkSize());
        //    m_io.readChunk(bufferMsg.inputBuffer);
        //    gateway.try_put(bufferMsg);
        }
		Socket* new_socket=tcp_socket->tcpAccept(6000000, 1024*1024*1 );
        sendLastMessage(gateway, new_socket);
        gateway.release_wait();
	}
	void sendLastMessage(async_socket_reader_node::gateway_type& gateway, Socket* sock) {
        //BufferMsg lastMsg;
        //lastMsg.markLast(m_io.chunksRead());
        gateway.try_put(sock);
    }

	//IOOperations& m_io;
	Socket* tcp_socket;
	std::string port="8000";
	int backlogSize=10;
    uint64_t timeoutInMicros= 1000*1000*60;
	std::thread m_fileReaderThread;

};*/

/*struct read_sock {
	
	AsyncNodeActivity* async;
	read_sock(AsyncNodeActivity* asyncNodeActivity){	
	async = asyncNodeActivity;
	}
	
    Socket * operator()() { 
		async_socket_reader_node::gateway_type& gateway;       	
		async->submitRead(gateway);
		//return ;
    }
};*/


int main(int argc, char* argv[])
{
	//graph producer_side;
	graph consumer_side;

	/*function_node< int, int > n( consumer_side, 1, []( int v ) -> int { 
        cout << v;
        spin_for( v );
        cout << v;
        return v;
    } );*/
	/*	
	AsyncNodeActivity* asyncNodeActivity = new AsyncNodeActivity();
	async_socket_reader_node file_reader(consumer_side, unlimited,
		[&asyncNodeActivity](const continue_msg& msg, async_socket_reader_node::gateway_type& gateway) {
        asyncNodeActivity->submitRead(gateway);
		}
	);*/

	/*
	std::string port="8000";
    int backlogSize=10;
    uint64_t timeoutInMicros= 1000*1000*60;	
	Socket* tcp_socket=new Socket();
    tcp_socket->tcpListen(port, backlogSize);
	Socket* new_socket=tcp_socket->tcpAccept(timeoutInMicros, 1024*1024*1 );
	*/
	
	source_node<Socket*> listen(consumer_side, Listener(), false );
	function_node<Socket*,TupleBuffer*> recv(consumer_side, 1, Receiver());   //, /* is_active */ false);
	//function_node<TupleBuffer*,TupleBuffer*> func1(consumer_side, unlimited, Worker_NOP());
	function_node<TupleBuffer*> sink(consumer_side, 1, Sinker());

	make_edge(listen,recv);
	//make_edge(file_reader, recv);
	make_edge(recv, sink);
	//make_edge(func1, sink);

	//recv.try_put(new_socket);	

	tick_count t_start = tick_count::now();
	listen.activate();
	//src2.activate();
	//n.try_put(10);
	//recv.try_get()
	consumer_side.wait_for_all();
	//consumer_side.wait_for_all();
	tick_count t_end = tick_count::now();

	cout << "Total runtime: " << (t_end - t_start).seconds() << " sec" << endl;

	return 0;
}
