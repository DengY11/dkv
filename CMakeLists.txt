cmake_minimum_required(VERSION 3.16)
project(dkv LANGUAGES CXX)

option(DKV_BUILD_TESTS "Build unit tests" ON)
option(DKV_BUILD_BENCHMARKS "Build benchmarks" ON)

include(GNUInstallDirs)

if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type selected, defaulting to Release")
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()

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

install(TARGETS dkv
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

if(DKV_BUILD_TESTS)
  enable_testing()
  add_executable(dkv_tests tests/test_main.cpp)
  target_link_libraries(dkv_tests PRIVATE dkv)
  add_test(NAME dkv_tests COMMAND dkv_tests)
endif()

if(DKV_BUILD_BENCHMARKS)
  find_package(SQLite3 QUIET)
  find_path(LEVELDB_INCLUDE_DIR leveldb/db.h)
  find_library(LEVELDB_LIBRARY leveldb)
  add_executable(dkv_bench benchmarks/bench.cc)
  target_link_libraries(dkv_bench PRIVATE dkv)
  if(LEVELDB_INCLUDE_DIR AND LEVELDB_LIBRARY)
    target_compile_definitions(dkv_bench PRIVATE DKV_HAVE_LEVELDB=1)
    target_include_directories(dkv_bench PRIVATE ${LEVELDB_INCLUDE_DIR})
    target_link_libraries(dkv_bench PRIVATE ${LEVELDB_LIBRARY})
  else()
    target_compile_definitions(dkv_bench PRIVATE DKV_HAVE_LEVELDB=0)
  endif()
  add_executable(dkv_bench_put_only benchmarks/bench_put_only.cpp)
  target_link_libraries(dkv_bench_put_only PRIVATE dkv)
  if(SQLite3_FOUND)
    target_link_libraries(dkv_bench PRIVATE SQLite::SQLite3)
    target_compile_definitions(dkv_bench PRIVATE DKV_HAVE_SQLITE=1)
  else()
    target_compile_definitions(dkv_bench PRIVATE DKV_HAVE_SQLITE=0)
  endif()
endif()

add_executable(dkv_example examples/example.cc)
target_link_libraries(dkv_example PRIVATE dkv)
