import copy
from ..attribute import Attributes as A

from ..metric import Metric
from .percentiles import percentiles
from .l_moments import l_moments
from .stats import statistics
from .p_moments import product_moments

#TODO make each one of these have a version with the NumberOfReturns>2 filter

all_metrics: dict[str, Metric] = dict(percentiles | l_moments | statistics |
        product_moments)


gr_perc = copy.deepcopy(percentiles)
gr_l_moments = copy.deepcopy(l_moments)
gr_stats = copy.deepcopy(statistics)
gr_p_moments = copy.deepcopy(product_moments)

for p in gr_perc.values():
    p.attributes = [A['Z'], A['Intensity']]
for l in gr_l_moments.values():
    l.attributes = [A['Z'], A['Intensity']]

gr_stats['cumean'].attributes = [A['Z']]
gr_stats['sqmean'].attributes = [A['Z']]
gr_stats['abovemean'].attributes = [A['Z']]
gr_stats['abovemode'].attributes = [A['Z']]
gr_stats['profilearea'].attributes = [A['Z']]

gr_stats['iq'].attributes = [A['Z'], A['Intensity']]
gr_stats['crr'].attributes = [A['Z'], A['Intensity']]
gr_stats['min'].attributes = [A['Z'], A['Intensity']]
gr_stats['max'].attributes = [A['Z'], A['Intensity']]
gr_stats['mode'].attributes = [A['Z'], A['Intensity']]
gr_stats['median'].attributes = [A['Z'], A['Intensity']]
gr_stats['stddev'].attributes = [A['Z'], A['Intensity']]
gr_stats['cv'].attributes = [A['Z'], A['Intensity']]

for s in gr_stats.values():
    s.attributes = [A['Z'], A['Intensity']]

grid_metrics: dict[str, Metric] = dict(gr_perc | gr_l_moments | gr_stats |
        gr_p_moments)