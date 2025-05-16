import copy
import numpy as np

from ..attribute import Attributes as A
from ..metric import Metric
from .percentiles import percentiles, pct_base
from .l_moments import l_moments
from .stats import statistics
from .p_moments import product_moments
from .aad import aad

_gr_perc = copy.deepcopy(percentiles)
_gr_l_moments = copy.deepcopy(l_moments)
_gr_stats = copy.deepcopy(statistics)
_gr_p_moments = copy.deepcopy(product_moments)
_gr_aad = copy.deepcopy(aad)

# Make special crr it relies too heavily on other metrics
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
    from .covers import covers
    from .counts import counts

    """
    Return FUSION GridMetrics Metrics dependent on an Elevation key.
    GridMetrics operate upon Intensity and an Elevation attribute.
    For PDAL in SilviMetric, elevation will be Z or HeightAboveGround.
    https://pdal.io/en/latest/dimensions.html#dimensions
    """

    assert elev_key in ['Z', 'HeightAboveGround']
    for m in (_gr_perc | _gr_l_moments | _gr_p_moments).values():
        m.attributes = [A[elev_key], A['Intensity']]
        for d in m.dependencies:
            d.attributes = [A[elev_key], A['Intensity']]

    # give profile_area separate pct_base so we can apply separate filters
    _gr_perc['profile_area'].attributes = [A[elev_key]]
    pct_base_copy = copy.deepcopy(pct_base)
    pct_base_copy.name = 'pct_base_profile_area'
    _gr_perc['profile_area'].dependencies = [pct_base_copy]

    _gr_stats['cumean'].attributes = [A[elev_key]]
    _gr_stats['sqmean'].attributes = [A[elev_key]]

    _gr_stats['iq'].attributes = [A[elev_key], A['Intensity']]
    _gr_stats['min'].attributes = [A[elev_key], A['Intensity']]
    _gr_stats['max'].attributes = [A[elev_key], A['Intensity']]
    _gr_stats['mode'].attributes = [A[elev_key], A['Intensity']]
    _gr_stats['median'].attributes = [A[elev_key], A['Intensity']]
    _gr_stats['stddev'].attributes = [A[elev_key], A['Intensity']]
    _gr_stats['cv'].attributes = [A[elev_key], A['Intensity']]
    _gr_stats['crr'] = _grid_crr_metric
    _gr_stats['crr'].attributes = [A[elev_key]]

    _gr_aad['aad'].attributes = [A[elev_key], A['Intensity']]
    _gr_aad['mad_median'].attributes = [A[elev_key]]
    _gr_aad['mad_mode'].attributes = [A[elev_key]]

    grid_metrics: dict[str, Metric] = dict(
        _gr_perc
        | _gr_l_moments
        | _gr_stats
        | _gr_p_moments
        | _gr_aad
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
