/*
    Copyright (c) 2005-2017 Intel Corporation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.




*/

//
// Example program that reads a file of decimal integers in text format
// and changes each to its square.
// 
#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cctype>
#include "../utility/utility.h"
#include "BaseBuffer.h"
#include "Socket.h"

using namespace std;

size_t MAX_BYTES_PER_BUFFER = 1024*1024*1;
string InputFileName = "input.txt";
string OutputFileName = "output.txt";
const uint64_t COUNT_MAX = 100; 

class MyInputFilter: public tbb::filter {
public:
    MyInputFilter();
    ~MyInputFilter();
	void* operator()(void*) /*override*/;
private:
    BaseBuffer* buf;
	uint64_t count = COUNT_MAX;
};

MyInputFilter::MyInputFilter() : 
    filter(serial_in_order),
	count(0)
{ 
}

MyInputFilter::~MyInputFilter() {
}
 
void* MyInputFilter::operator()(void*) {
    // Read characters into space that is available in the next slice.
    if( count>=COUNT_MAX) {
        // No more characters to pse
		//next_buffer = Buffer::allocate( MAX_BYTES_PER_BUFFER );
        return NULL;
    } 
	else {
        // Have more characters to process.
        //BaseBuffer& tç = *next_buffer;
		count++;
		buf = new BaseBuffer(MAX_BYTES_PER_BUFFER);
        return buf;
    }
}
         
//! Filter that writes each buffer to a file.
class MyOutputFilter: public tbb::filter {
public:
    MyOutputFilter();
	~MyOutputFilter();	
    void* operator()( void* item ) /*override*/;
private:
	const int LIMIT = 10000;
	int sock_count;
	uint64_t sent_bytes;
	std::string ipaddr= "10.1.100.31"; // server
    std::string port="8000";
    uint64_t Retry=100;
    uint64_t retryDelayInMicros=1000;
	Socket* send_socket;

};

MyOutputFilter::MyOutputFilter() : 
    tbb::filter(serial_in_order),
	sent_bytes(0),
	sock_count(0)
{
	send_socket=new Socket();
}

MyOutputFilter::~MyOutputFilter(){
	send_socket->tcpClose();
	cout << "send " << sent_bytes << " bytes" << endl;
}

void* MyOutputFilter::operator()( void* item ) {
    //BaseBuffer* buf = *static_cast<BaseBuffer*>(item);
	BaseBuffer* buf = (BaseBuffer*)item;
	if(sock_count <= 20){
		send_socket->tcpConnect(ipaddr, port, MAX_BYTES_PER_BUFFER, retryDelayInMicros, Retry);
		uint64_t num_bytes = send_socket->tcpSend(buf->getBuffer(), buf->getSize());	
		sock_count++;
		sent_bytes = sent_bytes + num_bytes;
	}
	delete buf;	
    return NULL;
}

bool silent = false;

int run_pipeline( int nthreads )
{
    // Create the pipeline
    tbb::pipeline pipeline;

    // Create file-reading writing stage and add it to the pipeline
    MyInputFilter input_filter;
    pipeline.add_filter( input_filter );

    // Create squaring stage and add it to the pipeline
    //MyTransformFilter transform_filter; 
    //pipeline.add_filter( transform_filter );

    // Create file-writing stage and add it to the pipeline
    MyOutputFilter output_filter;
    pipeline.add_filter( output_filter );

    // Run the pipeline
    tbb::tick_count t0 = tbb::tick_count::now();
    // Need more than one token in flight per thread to keep all threads 
    // busy; 2-4 works
    pipeline.run(nthreads*4);
    tbb::tick_count t1 = tbb::tick_count::now();

    if ( !silent ) printf("time = %g\n", (t1-t0).seconds());

    return 1;
}

int main( int argc, char* argv[] ) {
    try {
        tbb::tick_count mainStartTime = tbb::tick_count::now();

        // The 1st argument is the function to obtain 'auto' value; the 2nd is the default value
        // The example interprets 0 threads as "run serially, then fully subscribed"
        utility::thread_number_range threads( tbb::task_scheduler_init::default_num_threads, 0 );

        utility::parse_cli_arguments(argc,argv,
            utility::cli_argument_pack()
            //"-h" option for displaying help is present implicitly
            .positional_arg(threads,"n-of-threads",utility::thread_number_range_desc)
            //.positional_arg(MAX_CHAR_PER_INPUT_SLICE, "max-slice-size","the maximum number of characters in one slice")
            .arg(silent,"silent","no output except elapsed time")
            );
        //generate_if_needed( InputFileName.c_str() );

        if ( threads.first ) {
            for(int p = threads.first;  p <= threads.last; p=threads.step(p) ) {
                if ( !silent ) printf("threads = %d ", p);
                tbb::task_scheduler_init init(p);
                if(!run_pipeline (p))
                    return 1;
            }
        } else { // Number of threads wasn't set explicitly. Run serial and parallel version
            { // serial run
                if ( !silent ) printf("serial run   ");
                tbb::task_scheduler_init init_serial(1);
                if(!run_pipeline (1))
                    return 1;
            }
            { // parallel run (number of threads is selected automatically)
                if ( !silent ) printf("parallel run ");
                tbb::task_scheduler_init init_parallel;
                if(!run_pipeline (init_parallel.default_num_threads()))
                    return 1;
            }
        }

        //utility::report_elapsed_time((tbb::tick_count::now() - mainStartTime).seconds());

        return 0;
    } catch(std::exception& e) {
        std::cerr<<"error occurred. error text is :\"" <<e.what()<<"\"\n";
        return 1;
    }
}
