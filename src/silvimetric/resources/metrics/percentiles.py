import numpy as np
import pandas as pd
from ..metric import Metric

def percentile_base(data: pd.DataFrame):
    percentiles = [1,5,10,20,25,30,40,50,60,70,75,80,90,95,99]
    return np.percentile(data, percentiles)

def m_p01(data: pd.DataFrame):
    return np.percentile(data, 1)

def m_p05(data: pd.DataFrame):
    return np.percentile(data, 5)

def m_p10(data: pd.DataFrame):
    return np.percentile(data, 10)

def m_p20(data: pd.DataFrame):
    return np.percentile(data, 20)

def m_p25(data: pd.DataFrame):
    return np.percentile(data, 25)

def m_p30(data: pd.DataFrame):
    return np.percentile(data, 30)

def m_p40(data: pd.DataFrame):
    return np.percentile(data, 40)

def m_p50(data: pd.DataFrame):
    return np.percentile(data, 50)

def m_p60(data: pd.DataFrame):
    return np.percentile(data, 60)

def m_p70(data: pd.DataFrame):
    return np.percentile(data, 70)

def m_p75(data: pd.DataFrame):
    return np.percentile(data, 75)

def m_p80(data: pd.DataFrame):
    return np.percentile(data, 80)

def m_p90(data: pd.DataFrame):
    return np.percentile(data, 90)

def m_p95(data: pd.DataFrame):
    return np.percentile(data, 95)

def m_p99(data: pd.DataFrame):
    return np.percentile(data, 99)

def m_90m10(data):
    p = np.percentile(data, [10,90])
    return p[1] - p[0]

def m_95m05(data):
    p = np.percentile(data, [5,95])
    return p[1] - p[0]

percentiles: dict[str, Metric] = { }
percentiles['p01'] = Metric('p01', np.float32, m_p01)
percentiles['p05'] = Metric('p05', np.float32, m_p05)
percentiles['p10'] = Metric('p10', np.float32, m_p10)
percentiles['p20'] = Metric('p20', np.float32, m_p20)
percentiles['p25'] = Metric('p25', np.float32, m_p25)
percentiles['p30'] = Metric('p30', np.float32, m_p30)
percentiles['p40'] = Metric('p40', np.float32, m_p40)
percentiles['p50'] = Metric('p50', np.float32, m_p50)
percentiles['p60'] = Metric('p60', np.float32, m_p60)
percentiles['p70'] = Metric('p70', np.float32, m_p70)
percentiles['p75'] = Metric('p75', np.float32, m_p75)
percentiles['p80'] = Metric('p80', np.float32, m_p80)
percentiles['p90'] = Metric('p90', np.float32, m_p90)
percentiles['p95'] = Metric('p95', np.float32, m_p95)
percentiles['p99'] = Metric('p99', np.float32, m_p99)
percentiles['90m10'] = Metric('90m10', np.float32, m_90m10)
percentiles['95m05'] = Metric('95m05', np.float32, m_95m05)