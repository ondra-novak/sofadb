cmake_minimum_required(VERSION 2.8) 
add_compile_options(-std=c++17)
file(GLOB sofaserver_SRC "*.cpp" )
file(GLOB sofaserver_HDR "*.h" "*.tcc")

add_executable (sofadb ${sofaserver_SRC} )
target_link_libraries (sofadb LINK_PUBLIC sofa leveldb simpleRpcServer simpleServer imtjson ssl crypto pthread)
  