cmake_minimum_required(VERSION 3.20)
project(InMemoryDB VERSION 1.0 LANGUAGES CXX)

# Enforce C++23 standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Header search path
include_directories(${CMAKE_SOURCE_DIR}/include)

# Threading and platform-specific network libraries
find_package(Threads REQUIRED)
if(WIN32)
    set(NETWORK_LIBS ws2_32)
endif()

# Fetch Google Test for unit testing
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)

# Collect all library sources (everything under src/ except main.cpp)
file(GLOB_RECURSE LIB_SRC_FILES
    "src/memory/*.cpp"
    "src/engine/*.cpp"
    "src/core/*.cpp"
    "src/network/*.cpp"
    "src/daemons/*.cpp"
)

# --- TARGET: Main Database Server ---
add_executable(ServerApp src/main.cpp ${LIB_SRC_FILES})
target_link_libraries(ServerApp PRIVATE Threads::Threads ${NETWORK_LIBS})

# --- TARGET: Unit Tests ---
add_executable(RunTests
    tests/test_memory.cpp
    tests/test_engine.cpp
    tests/test_eviction.cpp
    ${LIB_SRC_FILES}
)
target_link_libraries(RunTests PRIVATE gtest_main Threads::Threads ${NETWORK_LIBS})

include(GoogleTest)
gtest_discover_tests(RunTests)
