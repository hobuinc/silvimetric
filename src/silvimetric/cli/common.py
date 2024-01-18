import click
import pyproj
import webbrowser

import dask
from dask.diagnostics import ProgressBar
from dask.distributed import Client

from ..resources import Bounds, Attribute, Metric, Attributes, Metrics


class BoundsParamType(click.ParamType):
    name = "Bounds"

    def convert(self, value, param, ctx):
        try:
            b = Bounds.from_string(value)
            return b
        except ValueError:
            self.fail(f"{value!r} is not a bounds type", param, ctx)

class CRSParamType(click.ParamType):
    name = "CRS"

    def convert(self, value, param, ctx):
        try:
            crs = pyproj.CRS.from_user_input(value)
            return crs
        except Exception as e:
            self.fail(f"{value!r} is not a CRS type with error {e}", param, ctx)

class AttrParamType(click.ParamType):
    name="Attrs"
    def convert(self, value, param, ctx) -> list[Attribute]:
        try:
            return [Attributes[a] for a in value]
        except Exception as e:
            self.fail(f"{value!r} is not available in Attributes, {e}", param, ctx)

class MetricParamType(click.ParamType):
    name="Metrics"
    def convert(self, value, param, ctx) -> list[Metric]:
        try:
            return [Metrics[m] for m in value]
        except Exception as e:
            self.fail(f"{value!r} is not available in Metrics, {e}", param, ctx)

def dask_handle(dasktype, workers, threads, watch):
    dask_config = {
        'n_workers': workers,
        'threads_per_worker': threads
    }
    if dasktype == 'cluster':
        client = Client(n_workers=workers, threads_per_worker=threads)
        client.get_versions(check=True)
        dask_config['scheduler']='distributed'
        dask_config['distributed.client']=client

    else:
        dask_config['scheduler']=dasktype
    dask.config.set(dask_config)

    if watch:
        if dasktype == 'cluster':
            webbrowser.open(client.cluster.dashboard_link)
        else:
            p = ProgressBar()
            p.register()
