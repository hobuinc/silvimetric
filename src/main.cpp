#include <iostream>
#include <string>
#include <exception>

#include <pdal/StageFactory.hpp>
#include <pdal/PipelineManager.hpp>
#include <pdal/Stage.hpp>
#include <pdal/Reader.hpp>
#include <tiledb/tiledb>

using namespace pdal;

int main(int argc, char *argv[])
{
    std::string eptPath = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/MD_GoldenBeach_2012/ept.json";
    StageFactory sf = StageFactory();
    PipelineManagerPtr pm = std::make_unique<PipelineManager>();

    std::string driver = sf.inferReaderDriver(eptPath);
    if (driver.empty())
        throw ("File not recognized");

    Stage *s = &pm->makeReader(eptPath, driver);

    Stage *info = &pm->makeFilter("filters.info", *s);
    pm->execute(ExecMode::PreferStream);
    bool stream = pm->pipelineStreamable();
    if (stream)
        std::cout << "Streamable."  << std::endl;
    else
        std::cout << "Not streamable."  << std::endl;
    // std::cout << "Info: " << pdal::Utils::toJSON(info->getMetadata()) << std::endl;

    return 0;
}

tiledb::Array& createArray()
{

}