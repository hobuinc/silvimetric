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
def cover_fn(data, *args):
    count = args[0]
    return data.count() / count * 100

def cover_above_val(data, *args):
    count = args[0]
    return count / data.count() * 100

def count_fn(data, *args):
    return data.count()


def count_above_mean(data, *args):
    mean = args[0]
    return data[data > mean].count()


def count_above_mode(data, *args):
    mode = args[0]
    return data[data > mode].count()


def first_returns_filter(data):
    return data[data.ReturnNumber == 1]


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
    for m in (gr_perc | gr_l_moments | gr_p_moments).values():
        m.attributes = [A[elev_key], A['Intensity']]
        for d in m.dependencies:
            d.attributes = [A[elev_key], A['Intensity']]

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

    count_metric = Metric(
        'count',
        np.int32,
        count_fn,
        attributes=[A['ReturnNumber']],
    )
    first_cnt_above_htbreak = Metric(
        '1st_count_above_htbreak',
        np.int32,
        count_fn,
        filters=[first_returns_filter],
        attributes=[A['ReturnNumber']],
    )
    first_cnt_above_mean = Metric(
        '1st_count_above_mean',
        np.int32,
        count_above_mean,
        filters=[first_returns_filter],
        attributes=[A['Z']],
        dependencies=[gr_p_moments['mean']],
    )
    first_cnt_above_mode = Metric(
        '1st_count_above_mode',
        np.int32,
        count_above_mean,
        filters=[first_returns_filter],
        attributes=[A['Z']],
        dependencies=[gr_stats['mode']],
    )
    counts = {
        '1st_count_above_htbreak': first_cnt_above_htbreak,
        '1st_count_above_mean': first_cnt_above_mean,
        '1st_count_above_mode': first_cnt_above_mode,
    }


    first_count_metric = Metric(
        '1st_count',
        np.int32,
        count_fn,
        filters = [first_returns_filter],
        attributes=[A['ReturnNumber']],
    )
    all_cover_metric = Metric(
        'all_cover_above_htbreak',
        np.float32,
        cover_fn,
        dependencies=[count_metric],
        attributes=[A['ReturnNumber']],
    )
    first_cover_metric = Metric(
        '1st_cover_above_htbreak',
        np.float32,
        cover_fn,
        dependencies=[first_count_metric],
        filters=[first_returns_filter],
        attributes=[A['ReturnNumber']],
    )
    first_cover_above_mean_metric = Metric(
        '1st_cover_above_mean',
        np.float32,
        cover_above_val,
        dependencies=[first_cnt_above_mean],
        filters=[first_returns_filter],
        attributes=[A['Z']],
    )
    first_cover_above_mode_metric = Metric(
        '1st_cover_above_mode',
        np.float32,
        cover_above_val,
        dependencies=[first_cnt_above_mode],
        filters=[first_returns_filter],
        attributes=[A['Z']],
    )

    covers = {
        'all_cover': all_cover_metric,
        '1st_cover': first_cover_metric,
        '1st_cover_above_mean': first_cover_above_mean_metric,
        '1st_cover_above_mode': first_cover_above_mode_metric
    }

    grid_metrics: dict[str, Metric] = dict(
        gr_perc
        | gr_l_moments
        | gr_stats
        | gr_p_moments
        | gr_aad
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
        '1st_cover_above_htbreak',
    ]
    ht_break_list = [
        'all_cover_above_htbreak',
        '1st_cover_above_htbreak',
        '1st_count_above_htbreak',
    ]
    no_filter_list = [
        '1st_count_above_mean',
        '1st_count_above_mode',
        '1st_cover_above_mean',
        '1st_cover_above_mode',
    ]
    for gm in grid_metrics.values():
        if gm.name in no_filter_list:
            continue

        filter_val = ht_break if gm.name in ht_break_list else min_ht
        method = make_elev_filter(filter_val, elev_key)

        if gm.name not in no_dep_filter_list:
            for d in gm.dependencies:
                if not d.filters:
                    d.filters.append(method)

        gm.add_filter(method)

    return grid_metrics


default = get_grid_metrics()
