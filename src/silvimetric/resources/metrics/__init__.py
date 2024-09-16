from ..metric import Metric
from .percentiles import percentiles
from .l_moments import l_moments
from .stats import statistics
from .p_moments import product_moments

#TODO make each one of these have a version with the NumberOfReturns>2 filter

grid_metrics: dict[str, Metric] = dict(percentiles | l_moments | statistics |
        product_moments)

all_metrics: dict[str, Metric] = dict(percentiles | l_moments | statistics |
        product_moments)