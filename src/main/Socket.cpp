#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/time.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <vector>
#include <inttypes.h>
#include <iostream>

#include "Socket.h"

const int Socket::YES_REUSE = 1;

Socket::Socket()
  : mode(NONE),
    address(""),
    fd(-1) {
}

Socket::~Socket() {
  if(mode != NONE){
  	std::cout << "Socket initialized at destruction time." << "\n" ;
  	exit(1);
  }
  if(fd != -1){
  	std::cout << "File descriptor" << fd << "open at destruction time" << "\n";
  	exit(1);
  }
}

void Socket::tcpListen(const std::string& port, int backlogSize) {
	if(mode != NONE){
		std::cout << "Cannot call listen() on an open socket." << "\n";
		exit(1);
	}
	mode = LISTENING;

	fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

	if (fd < 0) {
    	printf("socket() failed with status %d: %s", errno, strerror(errno));
    	exit(1);
  	}

  	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&YES_REUSE, sizeof(YES_REUSE))) {
  		printf("setsockopt(REUSEADDR) failed with status %d: %s", errno, strerror(errno));
		exit(1);
  	}

  	struct sockaddr_in listenAddr;
  	memset(&listenAddr, 0, sizeof(listenAddr));
  	listenAddr.sin_family = AF_INET;
  	listenAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  	listenAddr.sin_port = htons(atoi(port.c_str()));
	
	if (bind(fd, (struct sockaddr *) &listenAddr, sizeof(listenAddr)) < 0) {
    	printf("bind(%s) failed with status %d: %s", port.c_str(), errno, strerror(errno));
    	exit(1);
	}
	  // Listen for new connections with the appropriate backlog size.
	  // the listen here is the linux socket listen
  	if (listen(fd, backlogSize) < 0) {
    	printf("listen() failed with status %d: %s", errno, strerror(errno));
      exit(1);
  	}

	FD_ZERO(&acceptFDSet);
	FD_SET(fd, &acceptFDSet);
  std::cout << " listen_socket fd:"<< fd << " \n";
}

Socket* Socket::tcpAccept(uint64_t timeoutInMicros, uint64_t socketBufferSize) {
  if(mode != LISTENING){
  	printf("Cannot call accept() until after calling listen()");
  	exit(1);
  }

  if (timeoutInMicros > 0) {
    // Issue a select() on the listening socket.
    fd_set readFDSet;
    int fdMax = fd;
    int status;
    struct timeval timeout;

    readFDSet = acceptFDSet;

    timeout.tv_sec = timeoutInMicros / 1000000;
    timeout.tv_usec = timeoutInMicros % 1000000;

    status = select(fdMax + 1, &readFDSet, NULL, NULL, &timeout);

    if(status == -1){
    	printf("select() failed with status %d: %s", errno, strerror(errno));
    	exit(1);
    }

    // If the select() call timed out, return NULL.
    if (!FD_ISSET(fd, &readFDSet)) {
      return NULL;
    }
  }

  // Issue a blocking accept() call on the listening socket.
  struct sockaddr_in clientAddress;
  unsigned int addressLength = sizeof(clientAddress);

  int clientSocket = -1;

  if ((clientSocket = accept(fd, (struct sockaddr *) & clientAddress, &addressLength)) < 0) {
    printf("accept() failed with status %d: %s", errno, strerror(errno));
    exit(1);
  }

  std::string clientIPAddress = inet_ntoa(clientAddress.sin_addr);

  // Set the TCP receive buffer size
  if (socketBufferSize > 0 && setsockopt(
        clientSocket, SOL_SOCKET, SO_RCVBUF, (char *)&socketBufferSize,
        sizeof(socketBufferSize))) {
    //printf("setsocktopt SO_RCVBUF with size %" PRIu64 "failed with status %d: %s", socketBufferSize, errno, strerror(errno));
	exit(1);
  }

  // Create a new Socket object for this client connection.
  Socket* socket = new Socket();
  socket->address.assign(clientIPAddress);
  socket->fd = clientSocket;
  socket->mode = RECEIVING;

  std::cout << " accept_socket fd:"<< clientSocket << " \n";

  return socket;
}

void Socket::tcpConnect( const std::string& serverAddress, const std::string& serverPort,
  uint64_t socketBufferSize, uint64_t retryDelayInMicros, uint64_t maxRetries) {
  /*if(mode != NONE){
  	printf("Cannot call connect() on an open socket");
  	exit(1);
  }*/

  mode = SENDING;

  // Get network address information.
  struct addrinfo hints, *res;

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  int status = getaddrinfo(serverAddress.c_str(), serverPort.c_str(), &hints, &res);
  if(status == -1){
  	printf("getaddrinfo() failed with error code %d: %s", errno, strerror(errno));
  	exit(1);
  }
  // Create a socket for this connection and set socket options.
  fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if(fd == -1){
  	printf("socket() failed with error code %d: %s", errno, strerror(errno));
  	exit(1);
  }

  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&YES_REUSE, sizeof(YES_REUSE))) {
    printf("setsockopt(REUSEADDR) failed with status %d: %s", errno, strerror(errno));
    exit(1);
  }

  // Set the TCP send buffer size
  /*if (socketBufferSize > 0 && setsockopt(
        fd, SOL_SOCKET, SO_SNDBUF, (char *)&socketBufferSize,
        sizeof(socketBufferSize))) {
    //printf("setsocktopt SO_SNDBUF  with size %" PRIu64 "failed with status %d: %s", socketBufferSize, errno, strerror(errno));
	exit(1);
  }*/

  //int set = 1;
  //setsockopt(fd, SOL_SOCKET, SO_NOSIGNA, (void *)&set, sizeof(int));

  // Try to connect to the address some number of times before giving up.
  uint32_t retries = 0;
  status = -1;
  do {
    printf("Connecting to %s:%s on socket %d", serverAddress.c_str(), serverPort.c_str(), fd);

    status = ::connect(fd, res->ai_addr, res->ai_addrlen);

    if (status == -1) {
      printf("connect() to %s:%s failed with error %d: %s",
                         serverAddress.c_str(), serverPort.c_str(), errno,
                         strerror(errno));
      usleep(retryDelayInMicros);
      retries++;
    }
  } while (status == -1 && retries < maxRetries);

  freeaddrinfo(res);
  if(retries == maxRetries && status == -1){
    /*printf("Retried opening connection to %s:%s %" PRIu64 "times, and failed each "
           "time. Giving up.", serverAddress.c_str(), serverPort.c_str(),
           maxRetries);*/
    exit(1);
  }
  address.assign(serverAddress);

}


void Socket::tcpClose() {
  if(mode == NONE){
    printf("Cannot call close() on an uninitialized socket.");
    exit(1);
  }
  if(fd < 0) {
    printf("Cannot call close() on a closed socket.");
    exit(1);
  }
  int status = ::close(fd);
  if(status == -1){
    printf("close() failed with error %d: %s", errno, strerror(errno));
    exit(1);
  }
  fd = -1;
  mode = NONE;
}

uint64_t Socket::tcpSend(uint8_t *buffer, uint64_t buf_size) {
  int64_t totalBytes = 0;
  int64_t  recv_size  = buf_size;
  uint8_t* recvBuffer;
  int64_t numBytes;
  while(recv_size > 0){
    recvBuffer = (uint8_t*) (buffer + totalBytes);
    numBytes = send(fd, recvBuffer, recv_size, MSG_DONTWAIT);

    if(numBytes > 0){
      totalBytes = totalBytes + numBytes;
      recv_size = recv_size - numBytes;
      //printf("%p <- %p + %" PRId64 "\n", recvBuffer, buffer, numBytes);
    }
    else{
    //printf("send() error %d: %s", errno, strerror(errno));   
      //errno ==11 -> Resource temporarily unavailable
      if (errno == 11) 
        continue; 
      else
        printf("send() error %d: %s\n", errno, strerror(errno)); 
        break;
    } 

  }
  //ssize_t numBytes = recv(fd, buffer, buf_size, MSG_DONTWAIT);
  if(totalBytes > 0){
    return (uint64_t) totalBytes;
  }
  else{
    return 0;
  } 
}

uint64_t Socket::tcpReceive(uint8_t* buffer, uint64_t buf_size) {
  int64_t totalBytes = 0;
  int64_t  recv_size  = buf_size;
  uint8_t* recvBuffer;
  int64_t numBytes;
  while(recv_size > 0){
    recvBuffer = (uint8_t*) (buffer + totalBytes);
    numBytes = recv(fd, recvBuffer, recv_size, MSG_DONTWAIT);

    if(numBytes > 0){
      totalBytes = totalBytes + numBytes;
      recv_size = recv_size - numBytes;
      //printf("%p <- %p + %" PRId64 "\n", recvBuffer, buffer, numBytes);
    }    
  }
  //ssize_t numBytes = recv(fd, buffer, buf_size, MSG_DONTWAIT);
  if(totalBytes > 0){
    return (uint64_t) totalBytes;
  }
  else{
    return 0;
  } 
  
}


const std::string& Socket::getAddress() const {
  return address;
}

int Socket::getFD() const {
  return fd;
}






