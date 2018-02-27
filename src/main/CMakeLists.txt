 	cmake_minimum_required(VERSION 3.0.0 FATAL_ERROR)
	project(tbb_stages CXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread -g")	
	#add_executable(tbb_send tbb-send.cpp Socket.cpp)	
	#add_executable(tbb_recv tbb-recv.cpp Socket.cpp)	
	#add_executable(pipe_send pipeline_send.cpp Socket.cpp)	
	#add_executable(pipe_recv pipeline_recv.cpp Socket.cpp)
	#add_executable(pipe_test pipe_test.cpp gen_input.cpp)	

	add_executable(main_radix main_radix.cpp KeyValueRecord.cpp BucketTable.cpp)	

    include(${TBB_ROOT}/cmake/TBBBuild.cmake)

    # Build Intel TBB with enabled Community Preview Features (CPF).
    tbb_build(TBB_ROOT ${TBB_ROOT} CONFIG_DIR TBB_DIR MAKE_ARGS)

    find_package(TBB REQUIRED)

    # Link Intel TBB imported targets to the executable;
    # "TBB::tbb_preview" can be used instead of "${TBB_IMPORTED_TARGETS}".
    #target_link_libraries(tbb_send ${TBB_IMPORTED_TARGETS})
    #target_link_libraries(tbb_recv ${TBB_IMPORTED_TARGETS})	
	#target_link_libraries(pipe_send ${TBB_IMPORTED_TARGETS})
	#target_link_libraries(pipe_recv ${TBB_IMPORTED_TARGETS})
	#target_link_libraries(pipe_test ${TBB_IMPORTED_TARGETS})
	
	target_link_libraries(main_radix ${TBB_IMPORTED_TARGETS})
	
	

