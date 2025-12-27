cmake_minimum_required(VERSION 3.16)
project(dkv LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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

# select compression backend
# use like this cmake -B build -DDKV_COMPRESSION=snappy ..
# or cmake -B build -DDKV_COMPRESSION=zstd ..
# or cmake -B build -DDKV_COMPRESSION=lz4 ..
set(DKV_COMPRESSION "auto" CACHE STRING "Compression backend: auto|snappy|zstd|lz4|none")
set_property(CACHE DKV_COMPRESSION PROPERTY STRINGS auto snappy zstd lz4 none)

set(DKV_USE_SNAPPY 0)
set(DKV_USE_ZSTD 0)
set(DKV_USE_LZ4 0)

if(DKV_COMPRESSION STREQUAL "snappy")
  find_package(Snappy QUIET)
  if(Snappy_FOUND)
    set(DKV_USE_SNAPPY 1)
    target_link_libraries(dkv PUBLIC Snappy::snappy)
    message(STATUS "DKV compression backend: snappy")
  else()
    message(FATAL_ERROR "Snappy requested via DKV_COMPRESSION=snappy but package not found")
  endif()
elseif(DKV_COMPRESSION STREQUAL "zstd")
  find_package(ZSTD QUIET)
  if(ZSTD_FOUND)
    set(DKV_USE_ZSTD 1)
    target_link_libraries(dkv PUBLIC ZSTD::ZSTD)
    message(STATUS "DKV compression backend: zstd")
  else()
    message(FATAL_ERROR "ZSTD requested via DKV_COMPRESSION=zstd but package not found")
  endif()
elseif(DKV_COMPRESSION STREQUAL "lz4")
  find_package(LZ4 QUIET)
  if(LZ4_FOUND)
    set(DKV_USE_LZ4 1)
    target_link_libraries(dkv PUBLIC LZ4::lz4)
    message(STATUS "DKV compression backend: lz4")
  else()
    message(FATAL_ERROR "LZ4 requested via DKV_COMPRESSION=lz4 but package not found")
  endif()
elseif(DKV_COMPRESSION STREQUAL "none")
  message(STATUS "DKV compression backend: none")
else() # auto
  find_package(Snappy QUIET)
  find_package(ZSTD QUIET)
  find_package(LZ4 QUIET)
  if(Snappy_FOUND)
    set(DKV_USE_SNAPPY 1)
    target_link_libraries(dkv PUBLIC Snappy::snappy)
    message(STATUS "DKV compression backend (auto): snappy")
  elseif(ZSTD_FOUND)
    set(DKV_USE_ZSTD 1)
    target_link_libraries(dkv PUBLIC ZSTD::ZSTD)
    message(STATUS "DKV compression backend (auto): zstd")
  elseif(LZ4_FOUND)
    set(DKV_USE_LZ4 1)
    target_link_libraries(dkv PUBLIC LZ4::lz4)
    message(STATUS "DKV compression backend (auto): lz4")
  else()
    message(STATUS "DKV compression backend (auto): none")
  endif()
endif()

target_compile_definitions(dkv PRIVATE
  DKV_USE_SNAPPY=${DKV_USE_SNAPPY}
  DKV_USE_ZSTD=${DKV_USE_ZSTD}
  DKV_USE_LZ4=${DKV_USE_LZ4})

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
