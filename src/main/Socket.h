#ifndef SIMPLE_SOCKET_H
#define SIMPLE_SOCKET_H

#include <stdint.h>
#include <string>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

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
	uint64_t tcpSend(uint8_t *buffer, uint64_t buf_size);
	uint64_t tcpReceive(uint8_t *buffer, uint64_t buf_size);
	const std::string& getAddress() const;
  	int getFD() const;
	//bool closed() const;

	private:
	static const int YES_REUSE;
	Mode mode;
	std::string address;
	int fd;
	fd_set acceptFDSet;	
};

typedef std::vector<Socket*> SocketArray;

#endif // SIMPLE_SOCKET_H
