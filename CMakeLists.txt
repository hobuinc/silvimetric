cmake_minimum_required(VERSION 3.5)

project(bolepoll LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PDAL REQUIRED)
find_package(GDAL REQUIRED)

add_executable(bolepoll src/main.cpp)

target_include_directories(bolepoll
    PRIVATE
        ${PDAL_INCLUDE_DIRS}
        ${GDAL_INCLUDE_DIR}
)
target_link_libraries(bolepoll
    PRIVATE
        ${PDAL_LIBRARIES}
        ${GDAL_LIBRARY}
)
