import numpy as np
import pandas as pd
from ..metric import Metric

percents = [1,5,10,20,25,30,40,50,60,70,75,80,90,95,99]

def percentile_base(data: pd.DataFrame):
    return np.percentile(data, percents).tolist()


def m_p01(data: pd.DataFrame, *args):
    return args[0][0]


def m_p05(data: pd.DataFrame, *args):
    return args[0][1]


def m_p10(data: pd.DataFrame, *args):
    return args[0][2]


def m_p20(data: pd.DataFrame, *args):
    return args[0][3]


def m_p25(data: pd.DataFrame, *args):
    return args[0][4]


def m_p30(data: pd.DataFrame, *args):
    return args[0][5]


def m_p40(data: pd.DataFrame, *args):
    return args[0][6]


def m_p50(data: pd.DataFrame, *args):
    return args[0][7]


def m_p60(data: pd.DataFrame, *args):
    return args[0][8]


def m_p70(data: pd.DataFrame, *args):
    return args[0][9]


def m_p75(data: pd.DataFrame, *args):
    return args[0][10]


def m_p80(data: pd.DataFrame, *args):
    return args[0][11]


def m_p90(data: pd.DataFrame, *args):
    return args[0][12]


def m_p95(data: pd.DataFrame, *args):
    return args[0][13]


def m_p99(data: pd.DataFrame, *args):
    return args[0][14]


def m_90m10(data, *args):
    return args[0][12] - args[0][2]


def m_95m05(data, *args):
    return args[0][13] - args[0][1]

def m_profile_area(data, *args):
    # sanity check...must have valid heights/elevations
    p = np.percentile(data, range(0,100)).tolist()
    # dmax = data.max()
    # dmin = data.min()
    # if dmax <= 0:
    #     return -9999.0

    # p0 = max(dmin, 0.0)
    p0 = p[0]

    # second sanity check...99th percentile must be > 0
    try:
        p99 = p[99]
        if p99 > 0.0:
            # compute area under normalized percentile height curve using composite
            # trapeziod rule
            grid_pa = p0 / p99
            pcts = np.array(p[1:99])
            areas = pcts * 2 / p99
            pa = grid_pa + areas.sum() + 1

            return pa * 0.5
        else:
            return -9999.0
    except Exception:
        return -9999



pct_base = Metric('pct_base', object, percentile_base)

percentiles: dict[str, Metric] = {}
percentiles['p01'] = Metric('p01', np.float32, m_p01, [pct_base])
percentiles['p05'] = Metric('p05', np.float32, m_p05, [pct_base])
percentiles['p10'] = Metric('p10', np.float32, m_p10, [pct_base])
percentiles['p20'] = Metric('p20', np.float32, m_p20, [pct_base])
percentiles['p25'] = Metric('p25', np.float32, m_p25, [pct_base])
percentiles['p30'] = Metric('p30', np.float32, m_p30, [pct_base])
percentiles['p40'] = Metric('p40', np.float32, m_p40, [pct_base])
percentiles['p50'] = Metric('p50', np.float32, m_p50, [pct_base])
percentiles['p60'] = Metric('p60', np.float32, m_p60, [pct_base])
percentiles['p70'] = Metric('p70', np.float32, m_p70, [pct_base])
percentiles['p75'] = Metric('p75', np.float32, m_p75, [pct_base])
percentiles['p80'] = Metric('p80', np.float32, m_p80, [pct_base])
percentiles['p90'] = Metric('p90', np.float32, m_p90, [pct_base])
percentiles['p95'] = Metric('p95', np.float32, m_p95, [pct_base])
percentiles['p99'] = Metric('p99', np.float32, m_p99, [pct_base])
percentiles['90m10'] = Metric('90m10', np.float32, m_90m10, [pct_base])
percentiles['95m05'] = Metric('95m05', np.float32, m_95m05, [pct_base])
percentiles['profile_area'] = Metric('profile_area', np.float32, m_profile_area)