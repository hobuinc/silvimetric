cmake_minimum_required(VERSION 3.5)

project(fusion_util LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PDAL REQUIRED)
find_package(GDAL REQUIRED)

add_executable(fusion_util src/main.cpp)

target_include_directories(fusion_util
    PRIVATE
        ${PDAL_INCLUDE_DIRS}
        ${GDAL_INCLUDE_DIR}
)

target_link_libraries(fusion_util
    PRIVATE
        ${PDAL_LIBRARIES}
        ${GDAL_LIBRARY}
)
