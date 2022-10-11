cmake_minimum_required(VERSION 3.16)

project(project2)

# configure_file(DbConfig.h.in DbConfig.h)
include_directories(include)

include(CTest)


# C++ settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(THREADS_PREFER_PTHREAD_FLAG ON)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})


find_package(Threads REQUIRED)
# Options for libraries
option(USE_GOOGLE_TEST "Use GoogleTest for testing" ON)

# GoogleTest
if(USE_GOOGLE_TEST)
  add_subdirectory(test)
endif()

add_executable(run main.cc)
target_link_libraries(run Threads::Threads)

# target_link_libraries(${CMAKE_PROJECT_NAME} PUBLIC ${EXTRA_LIBS})

# target_include_directories(${CMAKE_PROJECT_NAME} PUBLIC
#   "${PROJECT_BINARY_DIR}"
#   )

