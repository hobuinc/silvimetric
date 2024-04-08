import click
import pyproj
import webbrowser

import dask
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster

from ..resources import Bounds, Attribute, Metric, Attributes, Metrics, Log


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
    name="Metrics"
    def convert(self, value, param, ctx) -> list[Metric]:
        if '.py' in value:
            try:
                import importlib.util
                import os
                from pathlib import Path

                cwd = os.getcwd()
                p = Path(cwd, value)
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
            return user_metrics.metrics()

        try:
            return Metrics[value]
        except Exception as e:
            self.fail(f"{value!r} is not available in Metrics, {e}", param, ctx)

def dask_handle(dasktype: str, scheduler: str, workers: int, threads: int,
        watch: bool, log: Log):
    dask_config = { }

    if dasktype == 'threads':
        dask_config['n_workers'] = threads
        dask_config['threads_per_worker'] = 1
    if dasktype == 'processes':
        dask_config['n_workers'] = workers
        dask_config['threads_per_worker'] = threads

    if scheduler == 'local':
        if scheduler != 'distributed':
            log.warning("Selected scheduler type does not support continuously"
                            "updated config information.")
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
        if scheduler != 'distributed':
            log.warning("""Selected scheduler type does not support continuously\
                            updated config information.""")
        dask_config['scheduler'] = scheduler

    dask.config.set(dask_config)
