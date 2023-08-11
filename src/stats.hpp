#pragma once

#include <pdal/pdal.hpp>
#include <tiledb/tiledb>

namespace fusion
{

class Stats
{
public:
    Stats(pdal::BOX2D &b, double cs);
    ~Stats();

    void init();
    pdal::BOX2D bbox();
    void addToCell(double x, double y, double z);
    void addView(pdal::PointViewPtr pv);

private:

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