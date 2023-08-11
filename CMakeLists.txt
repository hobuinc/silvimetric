cmake_minimum_required(VERSION 3.5)

project(bolepoll LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PDAL REQUIRED)
find_package(GDAL REQUIRED)
find_package(TileDB REQUIRED)

add_executable(bolepoll
    src/main.cpp
    src/stats.cpp
)

target_link_libraries(bolepoll
    PRIVATE
        pdalcpp
        GDAL::GDAL
        TileDB::tiledb_shared
)
