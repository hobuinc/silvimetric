import copy
from ..attribute import Attributes as A

from ..metric import Metric
from .percentiles import percentiles
from .l_moments import l_moments
from .stats import statistics
from .p_moments import product_moments
from .grid_metrics import get_grid_metrics

all_metrics: dict[str, Metric] = dict(percentiles | l_moments | statistics |
        product_moments)
