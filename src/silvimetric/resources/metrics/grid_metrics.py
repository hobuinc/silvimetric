import copy
import numpy as np

from ..attribute import Attributes as A
from ..metric import Metric
from .percentiles import percentiles
from .l_moments import l_moments
from .stats import statistics
from .p_moments import product_moments
from .aad import aad


def make_elev_filter(val, elev_key):
    def f_z_gt_val(data):
        return data[data[elev_key] > val]

    return f_z_gt_val


# TODO example for cover using all returns and a height threshold
# the threshold must be a parameter and not hardcoded
def all_cover(data, *args):
    count = args[0]
    return data.count() / count * 100


deps = []
count_metric = Metric(
    'count', np.float32, lambda x: x.count(), attributes=[A['ReturnNumber']]
)
allcover_metric = Metric(
    'all_cover',
    np.float32,
    all_cover,
    dependencies=[count_metric],
    attributes=[A['ReturnNumber']],
)


def _get_grid_metrics(elev_key='Z'):
    """
    Return FUSION GridMetrics Metrics dependent on an Elevation key.
    GridMetrics operate upon Intensity and an Elevation attribute.
    For PDAL in SilviMetric, elevation will be Z or HeightAboveGround.
    https://pdal.io/en/latest/dimensions.html#dimensions
    """
    gr_perc = copy.deepcopy(percentiles)
    gr_l_moments = copy.deepcopy(l_moments)
    gr_stats = copy.deepcopy(statistics)
    gr_p_moments = copy.deepcopy(product_moments)
    gr_aad = copy.deepcopy(aad)

    assert elev_key in ['Z', 'HeightAboveGround']
    for p in gr_perc.values():
        p.attributes = [A[elev_key], A['Intensity']]
    for l in gr_l_moments.values():  # noqa: E741
        l.attributes = [A[elev_key], A['Intensity']]

    gr_stats['cumean'].attributes = [A[elev_key]]
    gr_stats['sqmean'].attributes = [A[elev_key]]
    gr_stats['abovemean'].attributes = [A[elev_key]]
    gr_stats['abovemode'].attributes = [A[elev_key]]
    gr_stats['profile_area'].attributes = [A[elev_key]]

    gr_stats['iq'].attributes = [A[elev_key], A['Intensity']]
    gr_stats['crr'].attributes = [A[elev_key], A['Intensity']]
    gr_stats['min'].attributes = [A[elev_key], A['Intensity']]
    gr_stats['max'].attributes = [A[elev_key], A['Intensity']]
    gr_stats['mode'].attributes = [A[elev_key], A['Intensity']]
    gr_stats['median'].attributes = [A[elev_key], A['Intensity']]
    gr_stats['stddev'].attributes = [A[elev_key], A['Intensity']]
    gr_stats['cv'].attributes = [A[elev_key], A['Intensity']]

    gr_aad['aad'].attributes = [A[elev_key], A['Intensity']]
    gr_aad['mad_median'].attributes = [A[elev_key]]
    gr_aad['mad_mode'].attributes = [A[elev_key]]

    grid_metrics: dict[str, Metric] = dict(
        gr_perc
        | gr_l_moments
        | gr_stats
        | gr_p_moments
        | gr_aad
        | {allcover_metric.name: allcover_metric}
    )
    return grid_metrics


def get_grid_metrics(elev_key='Z', min_ht=2, ht_break=3):
    """
    Get GridMetric Metrics with filters applied.
    """
    # cover metrics use the ht_break, all others use min_ht
    cover_list = [allcover_metric.name]
    grid_metrics = _get_grid_metrics(elev_key)
    for gm in grid_metrics.values():
        filter_val = ht_break if gm.name in cover_list else min_ht
        method = make_elev_filter(filter_val, elev_key)
        for d in gm.dependencies:
            if not d.filters:
                d.filters.append(method)
        gm.add_filter(method)

    return grid_metrics


default = get_grid_metrics()