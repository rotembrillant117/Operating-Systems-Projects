cmake_minimum_required(VERSION 3.23)
project(ex3)

set(CMAKE_CXX_STANDARD 17)

add_executable(ex3 SampleClient.cpp MapReduceFramework.cpp Barrier.cpp)
