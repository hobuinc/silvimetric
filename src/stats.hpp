#pragma once

#include <pdal/pdal.hpp>
#include <tiledb/tiledb>

namespace fusion
{

class Stats
{
public:

    Stats(pdal::BOX2D b, double xs, double ys);
    ~Stats();
    void init();
    pdal::BOX2D bbox();
    void addToCell(double x, double y, double z);

private:

    pdal::BOX2D m_box;
    double m_xsize;
    double m_ysize;
    std::vector<pdal::BOX2D> cells;

    tiledb::Context m_ctx;
    std::unique_ptr<tiledb::Array> m_array;

    int findCell(double x, double y, double z);

};

}