cmake_minimum_required(VERSION 3.16)
project(dkv LANGUAGES CXX)

option(DKV_BUILD_TESTS "Build unit tests" ON)
option(DKV_BUILD_BENCHMARKS "Build benchmarks" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

file(GLOB_RECURSE DKV_SOURCES CONFIGURE_DEPENDS src/*.cc)

add_library(dkv STATIC ${DKV_SOURCES})
target_include_directories(dkv
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

if(MSVC)
  target_compile_options(dkv PRIVATE /W4 /permissive-)
else()
  target_compile_options(dkv PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(DKV_BUILD_TESTS)
  enable_testing()
  add_executable(dkv_tests tests/test_main.cpp)
  target_link_libraries(dkv_tests PRIVATE dkv)
  add_test(NAME dkv_tests COMMAND dkv_tests)
endif()

if(DKV_BUILD_BENCHMARKS)
  find_package(SQLite3 QUIET)
  add_executable(dkv_bench benchmarks/bench_put_get.cpp)
  target_link_libraries(dkv_bench PRIVATE dkv)
  if(SQLite3_FOUND)
    target_link_libraries(dkv_bench PRIVATE SQLite::SQLite3)
    target_compile_definitions(dkv_bench PRIVATE DKV_HAVE_SQLITE=1)
  else()
    target_compile_definitions(dkv_bench PRIVATE DKV_HAVE_SQLITE=0)
  endif()
endif()
