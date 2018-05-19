#ifndef SIMPLE_SOCKET_H
#define SIMPLE_SOCKET_H

#include <stdint.h>
#include <string>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include "spdlog/spdlog.h"

class Socket{

	public:
	enum Mode {
		NONE,
		LISTENING,
		RECEIVING,
		SENDING
  	};

	Socket();
	~Socket();
	void tcpListen(const std::string& port, int backlogSize);
	Socket* tcpAccept(uint64_t timeout, uint64_t socketBufferSize);
	void tcpConnect(const std::string& address, const std::string& port, uint64_t socketBufferSize, uint64_t retryDelayInMicros, uint64_t maxRetry);
	void tcpClose();
	uint64_t tcpSend(char* buffer, uint64_t buf_size);
	uint64_t tcpReceive(char *buffer, uint64_t buf_size);
	const std::string& getAddress() const;
  	int getFD() const;
  	int64_t getSocketBytes();
  	inline uint64_t getNanoSecond(struct timespec tp){
  		clock_gettime(CLOCK_MONOTONIC, &tp);
    	return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
  	}
	//bool closed() const;

	private:
	static const int YES_REUSE;
	std::shared_ptr<spdlog::logger> _logger;
	Mode mode;
	std::string address;
	int fd;
	fd_set acceptFDSet;	
	// temp var
	int64_t sock_bytes;
};

typedef std::vector<Socket*> SocketArray;

#endif // SIMPLE_SOCKET_H
