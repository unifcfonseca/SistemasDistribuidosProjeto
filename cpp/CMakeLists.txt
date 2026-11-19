
cmake_minimum_required(VERSION 3.16)
project(ProjetoSD)

set(CMAKE_CXX_STANDARD 17)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ZMQ REQUIRED libzmq)
find_package(nlohmann_json REQUIRED)

add_executable(cliente cliente.cpp)
add_executable(servidor servidor.cpp)

target_link_libraries(cliente ${ZMQ_LIBRARIES} nlohmann_json::nlohmann_json)
target_link_libraries(servidor ${ZMQ_LIBRARIES} nlohmann_json::nlohmann_json)
