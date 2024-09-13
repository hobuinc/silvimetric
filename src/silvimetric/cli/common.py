import click
import pyproj
import webbrowser

import dask
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster
from ..resources.metrics import l_moments, percentiles, statistics, product_moments
from ..resources.metrics import aad, grid_metrics, all_metrics

from .. import Bounds, Attribute, Metric, Attributes, Log


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

    def convert(self, value, param, ctx) -> pyproj.CRS:
        try:
            crs = pyproj.CRS.from_user_input(value)
            return crs
        except Exception as e:
            self.fail(f"{value!r} is not a CRS type with error {e}", param, ctx)

class AttrParamType(click.ParamType):
    name="Attrs"
    #TODO add import similar to metrics
    def convert(self, value, param, ctx) -> list[Attribute]:
        if isinstance(value, list):
            try:
                return [Attributes[a] for a in value]
            except Exception as e:
                self.fail(f"{value!r} is not available in Attributes, {e}", param, ctx)
        elif isinstance(value, str):
            return Attributes[value]
        else:
            self.fail(f"{value!r} is of an invalid type, {e}", param, ctx)

class MetricParamType(click.ParamType):
    name="metrics"
    def convert(self, value, param, ctx) -> list[Metric]:
        if value is None or not value:
            return list(all_metrics.values())
        parsed_values = value.split(',')
        metrics: set[Metric] = set()
        for val in parsed_values:
            if '.py' in val:
                # user imported metrics from external file
                try:
                    import importlib.util
                    import os
                    from pathlib import Path

                    cwd = os.getcwd()
                    p = Path(cwd, val)
                    if not p.exists():
                        self.fail("Failed to find import file for metrics at"
                                f" {str(p)}", param, ctx)

                    spec = importlib.util.spec_from_file_location('user_metrics', str(p))
                    user_metrics = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(user_metrics)
                    ms = user_metrics.metrics()
                except Exception as e:
                    self.fail(f"Failed to import metrics from {str(p)} with error {e}",
                            param, ctx)

                for m in ms:
                    if not isinstance(m, Metric):
                        self.fail(f"Invalid Metric supplied: {m}")

                metrics.update(list(user_metrics.metrics()))
            else:
                # SilviMetric defined metrics
                try:
                    if val == 'stats':
                        metrics.update(list(statistics.values()))
                    elif val == 'p_moments':
                        metrics.update(list(product_moments.values()))
                    elif val == 'l_moments':
                        metrics.update(list(l_moments.values()))
                    elif val == 'percentiles':
                        metrics.update(list(percentiles.values()))
                    elif val == 'aad':
                        metrics.update(list(aad.aad.values()))
                    elif val == 'grid_metrics':
                        metrics.update(list(grid_metrics.values()))
                    elif val == 'all':
                        metrics.update(list(all_metrics.values()))
                    else:
                        m = all_metrics[val]
                        if isinstance(m, Metric):
                            metrics.add(m)
                        else:
                            metrics.udpate(list(m))
                except Exception as e:
                    self.fail(f"{val!r} is not available in Metrics", param, ctx)
        return list(metrics)

def dask_handle(dasktype: str, scheduler: str, workers: int, threads: int,
        watch: bool, log: Log) -> None:
    dask_config = { }

    if dasktype == 'threads':
        dask_config['n_workers'] = threads
        dask_config['threads_per_worker'] = 1
    if dasktype == 'processes':
        dask_config['n_workers'] = workers
        dask_config['threads_per_worker'] = threads

    if scheduler == 'local':
        # fall back to dask type to determine the scheduler type
        dask_config['scheduler'] = dasktype
        if watch:
            p = ProgressBar()
            p.register()

    elif scheduler == 'distributed':
        dask_config['scheduler'] = scheduler
        if dasktype == 'processes':
            cluster = LocalCluster(processes=True, n_workers=workers, threads_per_worker=threads)
        elif dasktype == 'threads':
            cluster = LocalCluster(processes=False, n_workers=workers, threads_per_worker=threads)
        else:
            raise ValueError(f"Invalid value for 'dasktype', {dasktype}")

        client = Client(cluster)
        client.get_versions(check=True)
        dask_config['distributed.client'] = client
        if watch:
            webbrowser.open(client.cluster.dashboard_link)

    elif scheduler == 'single-threaded':
        dask_config['scheduler'] = scheduler

    dask.config.set(dask_config)

def close_dask() -> None:
    client = dask.config.get('distributed.client')
    if isinstance(client, Client):
        client.close()