#include <iostream>
#include <string>
#include <exception>

#include <pdal/StageFactory.hpp>
#include <pdal/PipelineManager.hpp>
#include <pdal/Stage.hpp>
#include <pdal/Reader.hpp>
#include <tiledb/tiledb>

using namespace pdal;
using namespace tiledb;

int findCell(double x, double y, double z)
{
    return 0;
}

void viewStats(PointViewPtr view)
{
    // tiledb::Array a;
    for (PointId i = 1; i < view->size(); ++i)
    {
        double x = view->getFieldAs<double>(pdal::Dimension::Id::X, i);
        double y = view->getFieldAs<double>(pdal::Dimension::Id::Y, i);
        double z = view->getFieldAs<double>(pdal::Dimension::Id::Z, i);
        int cellNum = findCell(x, y, z);

    }
}

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

    return 0;
}



// tiledb::Array& createArray(MetadataNode &node)
// {
//     Context ctx;
//     Domain domain(ctx);
// }