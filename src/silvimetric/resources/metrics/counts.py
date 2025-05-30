import numpy as np

from ..metric import Metric
from ..attribute import Attributes as A
from .p_moments import product_moments
from .stats import statistics

##### Methods #####
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

def make_returns_filter(val):
    def returns_filter(data):
        return data[data.ReturnNumber == val]
    return returns_filter

def second_returns_filter(data):
    return data[data.ReturnNumber == 2]

def third_returns_filter(data):
    return data[data.ReturnNumber == 3]


##### Counts ######
def get_count_metrics(elev_key='Z'):
    all_count = Metric(
        'all_count',
        np.int32,
        count_fn,
        attributes=[A['ReturnNumber']],
    )
    all_count_above_minht = Metric(
        'all_count_above_minht',
        np.int32,
        count_fn,
        attributes=[A['ReturnNumber']],
    )
    all_count_above_htbreak = Metric(
        'all_count_above_htbreak',
        np.int32,
        count_fn,
        attributes=[A['ReturnNumber']],
    )
    first_count_above_htbreak = Metric(
        '1st_count_above_htbreak',
        np.int32,
        count_fn,
        filters=[first_returns_filter],
        attributes=[A['ReturnNumber']],
    )
    all_count_above_mean=Metric(
        'all_count_above_mean',
        np.int32,
        count_above_mean,
        attributes=[A[elev_key]],
        dependencies=[product_moments['mean']],
    )
    all_count_above_mode=Metric(
        'all_count_above_mode',
        np.int32,
        count_above_mode,
        attributes=[A[elev_key]],
        dependencies=[product_moments['mean']],
    )
    first_count_above_mean = Metric(
        '1st_count_above_mean',
        np.int32,
        count_above_mean,
        filters=[first_returns_filter],
        attributes=[A[elev_key]],
        dependencies=[product_moments['mean']],
    )
    first_count_above_mode = Metric(
        '1st_count_above_mode',
        np.int32,
        count_above_mean,
        filters=[first_returns_filter],
        attributes=[A[elev_key]],
        dependencies=[statistics['mode']],
    )
    """number of first returns"""
    first_count = Metric(
        '1st_count',
        np.int32,
        count_fn,
        filters=[first_returns_filter],
        attributes=[A['ReturnNumber']],
    )
    """number of first returns above min_ht"""
    r1 = Metric(
        'r1_count',
        np.int32,
        count_fn,
        filters=[first_returns_filter],
        attributes=[A['ReturnNumber']],
    )
    """number of second returns above min_ht"""
    r2 = Metric(
        'r2_count',
        np.int32,
        count_fn,
        filters=[make_returns_filter(2)],
        attributes=[A['ReturnNumber']],
    )
    """number of third returns above min_ht"""
    r3 = Metric(
        'r3_count',
        np.int32,
        count_fn,
        filters=[make_returns_filter(3)],
        attributes=[A['ReturnNumber']],
    )
    """number of fourth returns above min_ht"""
    r4 = Metric(
        'r4_count',
        np.int32,
        count_fn,
        filters=[make_returns_filter(4)],
        attributes=[A['ReturnNumber']],
    )
    """number of fifth returns above min_ht"""
    r5 = Metric(
        'r5_count',
        np.int32,
        count_fn,
        filters=[make_returns_filter(5)],
        attributes=[A['ReturnNumber']],
    )
    """number of sixth returns above min_ht"""
    r6 = Metric(
        'r6_count',
        np.int32,
        count_fn,
        filters=[make_returns_filter(6)],
        attributes=[A['ReturnNumber']],
    )
    """number of seventh returns above min_ht"""
    r7 = Metric(
        'r7_count',
        np.int32,
        count_fn,
        filters=[make_returns_filter(7)],
        attributes=[A['ReturnNumber']],
    )
    counts = {
        all_count.name: all_count,
        all_count_above_htbreak.name: all_count_above_htbreak,
        all_count_above_minht.name: all_count_above_minht,
        all_count_above_mean.name: all_count_above_mean,
        all_count_above_mode.name: all_count_above_mode,

        first_count.name: first_count,
        first_count_above_htbreak.name: first_count_above_htbreak,
        first_count_above_mean.name: first_count_above_mean,
        first_count_above_mode.name: first_count_above_mode,
        r1.name: r1,
        r2.name: r2,
        r3.name: r3,
        r4.name: r4,
        r5.name: r5,
        r6.name: r6,
        r7.name: r7,
    }
    return counts

counts = get_count_metrics()
