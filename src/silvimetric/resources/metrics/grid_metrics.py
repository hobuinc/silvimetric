import copy
import numpy as np

from ..attribute import Attributes as A
from ..metric import Metric
from .percentiles import percentiles, pct_base
from .l_moments import l_moments
from .stats import statistics
from .p_moments import product_moments
from .aad import aad

# Make special crr. It relies too heavily on other metrics
# that will have filters applied to them.
def _grid_crr(data, *args):
    mean = data.mean()
    data_min = data.min()
    data_max = data.max()

    den = data_max - data_min
    if den == 0:
        return np.nan

    return (mean - data_min) / den

_grid_crr_metric = Metric('canopy_relief_ratio', np.float32, _grid_crr)

def make_elev_filter(val, elev_key):
    def f_z_gt_val(data):
        return data[data[elev_key] > val]

    return f_z_gt_val


def _get_grid_metrics(elev_key='Z'):
    from .covers import get_cover_metrics
    from .counts import get_count_metrics
    covers = get_cover_metrics(elev_key)
    counts = get_count_metrics(elev_key)

    """
    Return FUSION GridMetrics Metrics dependent on an Elevation key.
    GridMetrics operate upon Intensity and an Elevation attribute.
    For PDAL in SilviMetric, elevation will be Z or HeightAboveGround.
    https://pdal.io/en/latest/dimensions.html#dimensions
    """

    assert elev_key in ['Z', 'HeightAboveGround']
    for m in (percentiles | l_moments | product_moments).values():
        m.attributes = [A[elev_key], A['Intensity']]
        for d in m.dependencies:
            d.attributes = [A[elev_key], A['Intensity']]

    # give profile_area separate pct_base so we can apply separate filters
    percentiles['profile_area'].attributes = [A[elev_key]]
    # pct_base_copy = copy.deepcopy(pct_base)
    # pct_base_copy.name = 'pct_base_profile_area'
    # percentiles['profile_area'].dependencies = [pct_base_copy]

    statistics['cumean'].attributes = [A[elev_key]]
    statistics['sqmean'].attributes = [A[elev_key]]

    statistics['iq'].attributes = [A[elev_key], A['Intensity']]
    statistics['min'].attributes = [A[elev_key], A['Intensity']]
    statistics['max'].attributes = [A[elev_key], A['Intensity']]
    statistics['mode'].attributes = [A[elev_key], A['Intensity']]
    statistics['median'].attributes = [A[elev_key], A['Intensity']]
    statistics['stddev'].attributes = [A[elev_key], A['Intensity']]
    statistics['cv'].attributes = [A[elev_key], A['Intensity']]
    statistics['crr'] = _grid_crr_metric
    statistics['crr'].attributes = [A[elev_key]]

    aad['aad'].attributes = [A[elev_key], A['Intensity']]
    aad['mad_median'].attributes = [A[elev_key]]
    aad['mad_mean'].attributes = [A[elev_key]]
    aad['mad_mode'].attributes = [A[elev_key]]

    grid_metrics: dict[str, Metric] = dict(
        percentiles
        | l_moments
        | statistics
        | product_moments
        | aad
        | counts
        | covers
    )
    return grid_metrics


def get_grid_metrics(elev_key='Z', min_ht=2, ht_break=3):
    """
    Get GridMetric Metrics with filters applied.
    """
    # cover metrics use the ht_break, all others use min_ht
    grid_metrics = _get_grid_metrics(elev_key)
    no_dep_filter_list = [
        'all_cover_above_htbreak',
        'all_cover_above_mean',
        '1st_cover_above_htbreak',
        'all_1st_cover_above_htbreak',
        'all_1st_cover_above_mean',
        'all_1st_cover_above_mode',
        'profile_area',
    ]
    ht_break_list = [
        'all_cover_above_htbreak',
        '1st_cover_above_htbreak',
        '1st_count_above_htbreak',
        'all_count_above_htbreak',
    ]
    no_filter_list = [
        '1st_count_above_mean',
        '1st_count_above_mode',
        '1st_cover_above_mean',
        '1st_cover_above_mode',
        'all_1st_cover_above_htbreak',
        'all_1st_cover_above_mean',
        'all_1st_cover_above_mode',
        'all_cover_above_mean',
        'all_count_above_mean',
        'all_count_above_mode',
        'all_count',
        '1st_count',
        'profile_area',
    ]
    min_ht = make_elev_filter(min_ht, elev_key)
    ht_break = make_elev_filter(ht_break, elev_key)
    for gm in grid_metrics.values():
        if gm.name in no_filter_list:
            continue

        filter_fn = ht_break if gm.name in ht_break_list else min_ht

        if gm.name not in no_dep_filter_list:
            for d in gm.dependencies:
                if filter_fn not in d.filters:
                    d.filters.append(filter_fn)

        if filter_fn not in gm.filters:
            gm.add_filter(filter_fn)

    return grid_metrics
