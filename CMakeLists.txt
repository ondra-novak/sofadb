cmake_minimum_required(VERSION 2.8)
project (sofadb)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin/)
if ( CMAKE_COMPILER_IS_GNUCC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Wextra  -Wno-noexcept-type")
endif()
if ( MSVC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} /W4")
endif()

include_directories(BEFORE src/imtjson/src src src/simpleServer/src)
add_compile_options(-std=c++14)

add_subdirectory (src/imtjson/src/imtjson EXCLUDE_FROM_ALL)
add_subdirectory (src/simpleServer/src/simpleServer  EXCLUDE_FROM_ALL)
add_subdirectory (src/simpleServer/src/rpc EXCLUDE_FROM_ALL)
add_subdirectory (src/libsofa)
add_subdirectory (src/main)
 