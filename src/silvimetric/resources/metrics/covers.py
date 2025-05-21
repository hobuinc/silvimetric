import numpy as np

from ..metric import Metric
from ..attribute import Attributes as A
from .counts import first_returns_filter, get_count_metrics


##### Methods ######
def cover_fn(data, *args):
    count = args[0]
    return data.count() / count * 100

def cover_fn2(data, *args):
    count = args[0]
    return data.count() / count * 100

def cover_above_val(data, *args):
    count = args[0]
    return count / data.count() * 100


def get_cover_metrics(elev_key='Z'):
    counts = get_count_metrics(elev_key)

    """(all returns above ht_break) / (total returns) """
    all_cover = Metric(
        'all_cover_above_htbreak',
        np.float32,
        cover_fn,
        dependencies=[counts['all_count']],
        attributes=[A['ReturnNumber']],
    )
    """(all returns above mean) / (total returns) """
    all_cover_above_mean = Metric(
        'all_cover_above_mean',
        np.float32,
        cover_above_val,
        dependencies=[counts['all_count_above_mean']],
        attributes=[A[elev_key]],
    )
    """(all returns above mode) / (total returns) """
    all_cover_above_mode = Metric(
        'all_cover_above_mode',
        np.float32,
        cover_above_val,
        dependencies=[counts['all_count_above_mode']],
        attributes=[A[elev_key]],
    )
    """(first returns above ht_break) / (total first returns) """
    first_cover = Metric(
        '1st_cover_above_htbreak',
        np.float32,
        cover_fn2,
        dependencies=[counts['1st_count']],
        filters=[first_returns_filter],
        attributes=[A['ReturnNumber']],
    )
    """(first returns above mean) / (total first returns) """
    first_cover_above_mean = Metric(
        '1st_cover_above_mean',
        np.float32,
        cover_above_val,
        dependencies=[counts['1st_count_above_mean']],
        filters=[first_returns_filter],
        attributes=[A[elev_key]],
    )
    """(first returns above mode) / (total first returns) """
    first_cover_above_mode = Metric(
        '1st_cover_above_mode',
        np.float32,
        cover_above_val,
        dependencies=[counts['1st_count_above_mode']],
        filters=[first_returns_filter],
        attributes=[A[elev_key]],
    )
    """(all returns above cover ht) / (total first returns) """
    all_first_cover = Metric(
        'all_1st_cover_above_htbreak',
        np.float32,
        cover_above_val,
        dependencies=[counts['all_count_above_htbreak']],
        filters=[first_returns_filter],
        attributes=[A['ReturnNumber']],
    )
    """(all returns above mean ht) / (total first returns) """
    all_first_cover_above_mean = Metric(
        'all_1st_cover_above_mean',
        np.float32,
        cover_above_val,
        dependencies=[counts['all_count_above_mean']],
        filters=[first_returns_filter],
        attributes=[A[elev_key]],
    )
    """(all returns above mean ht) / (total first returns) """
    all_first_cover_above_mode = Metric(
        'all_1st_cover_above_mode',
        np.float32,
        cover_above_val,
        dependencies=[counts['all_count_above_mode']],
        filters=[first_returns_filter],
        attributes=[A[elev_key]],
    )
    covers = {
        'all_cover': all_cover,
        'all_cover_above_mean': all_cover_above_mean,
        '1st_cover': first_cover,
        '1st_cover_above_mean': first_cover_above_mean,
        '1st_cover_above_mode': first_cover_above_mode,
        'all_1st_cover': all_first_cover,
        'all_1st_cover_above_mean': all_first_cover_above_mean,
        'all_1st_cover_above_mode': all_first_cover_above_mode,
        'all_cover_above_mode': all_cover_above_mode
    }
    return covers

covers = get_cover_metrics()