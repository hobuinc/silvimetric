#pragma once

#include <pdal/pdal.hpp>
#include <tiledb/tiledb>

namespace fusion
{
typedef std::function<void (pdal::PointViewPtr )> StatLoader;

class Stats
{
public:
    Stats(pdal::BOX2D &b, double cs);
    ~Stats();

    void init();
    pdal::BOX2D bbox();
    void addToCell(double x, double y, double z);
    void addView(pdal::PointViewPtr pv);
    void addLoader(StatLoader l);
    void execute();


private:

    std::vector<StatLoader> loaders;
    // StatLoader loader;

    pdal::BOX2D m_box;
    double m_cellsize;
    double m_ysize;
    int m_xcells;
    int m_ycells;
    std::vector<int> m_attData;

    std::unique_ptr<tiledb::Context> m_ctx;
    std::unique_ptr<tiledb::Array> m_array;

};

}