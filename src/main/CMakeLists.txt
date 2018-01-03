
set(Main_Sockets_SRCS
    main-sockets.cpp
)

add_executable(apheleia-sockets
    ${Main_Sockets_SRCS}
)

set(Tbb_Sockets_SRCS
    tbb-sockets.cpp
)

add_executable(tbb-sockets
    ${Tbb_Sockets_SRCS}
)

TARGET_LINK_LIBRARIES(tbb-sockets
    tbb
	tbbmalloc
	rt
	proto
	${PROTOBUF_LIBRARY}
)
