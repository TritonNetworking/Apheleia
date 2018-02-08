 	cmake_minimum_required(VERSION 3.0.0 FATAL_ERROR)
	project(tbb_stages CXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")	
	add_executable(tbb_send tbb-send.cpp Socket.cpp)	
	add_executable(tbb_recv tbb-recv.cpp Socket.cpp)	

    include(${TBB_ROOT}/cmake/TBBBuild.cmake)

    # Build Intel TBB with enabled Community Preview Features (CPF).
    tbb_build(TBB_ROOT ${TBB_ROOT} CONFIG_DIR TBB_DIR MAKE_ARGS)

    find_package(TBB REQUIRED)

    # Link Intel TBB imported targets to the executable;
    # "TBB::tbb_preview" can be used instead of "${TBB_IMPORTED_TARGETS}".
    target_link_libraries(tbb_send ${TBB_IMPORTED_TARGETS})
    target_link_libraries(tbb_recv ${TBB_IMPORTED_TARGETS})	
	
