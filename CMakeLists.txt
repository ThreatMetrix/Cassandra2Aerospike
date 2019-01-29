cmake_minimum_required(VERSION 3.1)
project(cassandra2aerospike C CXX)

find_library(AEROSPIKE_LIBRARIES aerospike)
find_library(LZ4_LIBRARIES lz4)
find_library(SNAPPY_LIBRARIES snappy)
find_library(EV_LIBRARIES ev)

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)

find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

set(CMAKE_CXX_STANDARD 11)

add_executable(cassandra2aerospike
                Cassandra2Aerospike.cpp
                Utilities.cpp
                Buffer.cpp
                CassandraParser.cpp
                Partitioners.cpp
                SSTable.cpp
                SSTableSchema.cpp
                AerospikeWriter.cpp
                DryRun.cpp
                Utilities.hpp
                Buffer.hpp
                CassandraParser.hpp
                Partitioners.hpp
                SSTable.hpp
                SSTableSchema.hpp
                AerospikeWriter.hpp
                DryRun.hpp)

target_include_directories(cassandra2aerospike PUBLIC "/usr/local/include/" ${ZLIB_INCLUDE_DIRS} ${OPENSSL_INCLUDE_DIR})
target_link_libraries(cassandra2aerospike Threads::Threads OpenSSL::SSL OpenSSL::Crypto ${AEROSPIKE_LIBRARIES} ${AEROSPIKE_LIBRARIES} ${LZ4_LIBRARIES} ${SNAPPY_LIBRARIES} ${ZLIB_LIBRARIES} ${EV_LIBRARIES})

target_compile_definitions(cassandra2aerospike PRIVATE AS_USE_LIBEV)
