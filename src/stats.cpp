#include "stats.hpp"

#include <cmath>

namespace fusion
{

using namespace pdal;
using namespace tiledb;

// TODO figure out good default for x and y cell size. using 10 for now
Stats::Stats(BOX2D &b, double cs = 10):
m_box(b), m_cellsize(cs)
{ };

Stats::~Stats() { };

void Stats::init()
{
    m_ctx = std::make_unique<Context>();
    // TODO validate bbox

    // TODO initialize cells in tiledb array
    // - split up m_box by m_xsize and m_ysize
    // - add each box to m_array as metadata?
    // - otherwise create vector of boxes and find correct cell that way
    // - as we iterate through cells, create tiledb dimension and add to array
    //      with the extent associated with them
    // assuming same units
    // round up to include anything
    m_xcells = std::ceil((m_box.maxx - m_box.minx) / m_cellsize);
    m_ycells = std::ceil((m_box.maxy - m_box.miny) / m_cellsize);

    Domain domain(*m_ctx);
    auto rows = tiledb::Dimension::create<int>(*m_ctx, "rows",
        {{1, m_xcells}});
    auto cols = tiledb::Dimension::create<int>(*m_ctx, "cols",
        {{1, m_ycells}});

    domain.add_dimension(rows);
    domain.add_dimension(cols);

    ArraySchema s(*m_ctx, TILEDB_DENSE);
    s.set_domain(domain);
    s.add_attribute(Attribute::create<int>(*m_ctx, "count"));

    // TODO tighten up usage of pointer
    // Make sure we can't use any other method before this is created.


    Array::create("stats", s);
    m_array = std::make_unique<Array>(*m_ctx, "stats", TILEDB_WRITE);
    m_attData = std::vector<int>(m_xcells * m_ycells, 0);
}

void Stats::addView(PointViewPtr view)
{
    Query q(*m_ctx, *m_array, TILEDB_WRITE);
    for (PointId i = 1; i < view->size(); ++i)
    {
        double x = view->getFieldAs<double>(pdal::Dimension::Id::X, i);
        double y = view->getFieldAs<double>(pdal::Dimension::Id::Y, i);
        double z = view->getFieldAs<double>(pdal::Dimension::Id::Z, i);
        addToCell(x, y, z);
    }
    q.set_layout(TILEDB_ROW_MAJOR);
    q.set_data_buffer<int>("count", m_attData);
    q.submit();
    m_array->close();
}

void Stats::addToCell(double x, double y, double z)
{
    //TODO find which cell this point fits in
    //indexes start from 1 instead of 0
    int xIndex = std::floor((x - m_box.minx) / m_cellsize);
    int yIndex = std::floor((y - m_box.miny) / m_cellsize);
    int cellNumber = (m_xcells * yIndex) + xIndex;
    //TODO add stats to tiledb array index value
    int yo = m_attData[cellNumber];
    m_attData[cellNumber]++;
    int yo1 = m_attData[cellNumber];
}


}