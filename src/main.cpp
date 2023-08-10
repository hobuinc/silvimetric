#include <iostream>
#include <string>
#include <exception>

#include <pdal/PipelineManager.hpp>
#include <pdal/StageFactory.hpp>
#include <pdal/Reader.hpp>
#include <pdal/Stage.hpp>

using namespace pdal;

int main(int argc, char *argv[])
{
    std::string eptPath = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/MD_GoldenBeach_2012/ept.json";
    StageFactory sf = StageFactory();
    PipelineManagerPtr pm = std::make_unique<PipelineManager>();

    std::string driver = sf.inferReaderDriver(eptPath);
    if (driver.empty())
        throw ("File not recognized");

    Stage *s = &pm->addReader(driver);

    // PointTable table;
    Options o;
    o.add("filename", eptPath);
    s->addOptions(o);
    // s->prepare(table);

    // Stage *info = &pm->addFilter("filters.info");
    pm->execute(ExecMode::PreferStream);
    // std::cout <<"info metadata? " << pdal::Utils::toJSON(info->getMetadata()) << std::endl;
    std::cout <<"reader metadata? " << pdal::Utils::toJSON(s->getMetadata()) << std::endl;

    return 0;
}