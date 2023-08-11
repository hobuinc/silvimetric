#include "stats.hpp"

namespace fusion
{

using namespace pdal;
using namespace tiledb;

// TODO figure out good default for x and y cell size. using 10 for now
Stats::Stats(BOX2D b, double xs = 10, double ys = 10):
m_box(b), m_xsize(xs), m_ysize(ys)
{ };

Stats::~Stats() { };

void Stats::init()
{
    // TODO validate bbox

    // TODO initialize cells in tiledb array
    // - split up m_box by m_xsize and m_ysize
    // - add each box to m_array as metadata?
    // - otherwise create vector of boxes and find correct cell that way
    // - as we iterate through cells, create tiledb dimension and add to array
    //      with the extent associated with them
    // assuming same units
    int x_cells = (m_box.maxx - m_box.minx) / m_xsize;
    int y_cells = (m_box.maxy - m_box.miny) / m_ysize;
    Domain domain(m_ctx);
    auto dim = tiledb::Dimension::create<int>(m_ctx, "count", {{x_cells, y_cells}});
    domain.add_dimension(dim);


    // TODO tighten up usage of pointer
    // Make sure we can't use any other method before this is created.

    ArraySchema s(m_ctx, TILEDB_DENSE);
    Domain domain(m_ctx);
    domain.add_dimension(tiledb::Dimension::create<int>(m_ctx, "rows", {{1,4}}));
    m_array = std::make_unique<Array>("stats", s);
}

void Stats::addToCell(double x, double y, double z)
{
    //TODO find which cell this point fits in
    //TODO add stats to tiledb array index value

}

int Stats::findCell(double x, double y, double z)
{

}



}