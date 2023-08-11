#include <iostream>
#include <string>
#include <exception>

#include "stats.hpp"

#include <pdal/StageFactory.hpp>
#include <pdal/PipelineManager.hpp>
#include <pdal/Stage.hpp>
#include <pdal/Reader.hpp>
#include <tiledb/tiledb>

using namespace pdal;
using namespace tiledb;

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
    pm->execute();
    PointViewSet set = pm->views();
    MetadataNode n = info->getMetadata();
    MetadataNode bbox = n.findChild("bbox");
    double maxx = std::stod(bbox.findChild("maxx").value());
    double minx = std::stod(bbox.findChild("minx").value());
    double maxy = std::stod(bbox.findChild("maxy").value());
    double miny = std::stod(bbox.findChild("miny").value());
    BOX2D b(minx, miny, maxx, maxy);
    fusion::Stats stats(b, 10);
    stats.init();
    stats.addView(*set.begin());

    return 0;
}



// tiledb::Array& createArray(MetadataNode &node)
// {
//     Context ctx;
//     Domain domain(ctx);
// }