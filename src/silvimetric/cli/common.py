import click
import pyproj
import webbrowser

import dask
import numpy as np
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster

from ..resources.metrics import (
    l_moments,
    percentiles,
    statistics,
    product_moments,
)
from ..resources.metrics import aad, grid_metrics, all_metrics

from .. import Bounds, Attribute, Metric, Attributes, Log


class BoundsParamType(click.ParamType):
    """Click parameter type for the Bounds class.

    Accepts bounds as a stringified JSON object or bbox array. Examples:
    "([1,101],[2,102],[3,103])"
    "{\"minx\": 1,\"miny\": 2,\"maxx\": 101,\"maxy\": 102}"
    "[1,2,101,102]"
    "[1,2,3,101,102,103]"
    """

    name = 'Bounds'

    def convert(self, value, param, ctx):
        try:
            b = Bounds.from_string(value)
            return b
        except ValueError:
            self.fail(f'{value!r} is not a bounds type', param, ctx)


class CRSParamType(click.ParamType):
    """Click parameter type for the Coordinate Reference System of a project.

    Accepts a string and returns an instance of the pyproj.CRS class.
    """

    name = 'CRS'

    def convert(self, value, param, ctx) -> pyproj.CRS:
        try:
            crs = pyproj.CRS.from_user_input(value)
            return crs
        except Exception as e:
            self.fail(f'{value!r} is not a CRS type with error {e}', param, ctx)


class AttrParamType(click.ParamType):
    """Click parameter for SilviMetric Attributes.

    Returns list of PDAL dimensions that match the strings input.
    """

    name = 'Attrs'

    # TODO add import similar to metrics
    def convert(self, value, param, ctx) -> list[Attribute]:
        attrs: set[Attribute] = set()
        if not value:
            return set(
                Attributes[a]
                for a in ['Z', 'ReturnNumber', 'NumberOfReturns', 'Intensity']
            )
        parsed_values = value.split(',')
        for val in parsed_values:
            try:
                if isinstance(value, str):
                    attrs.add(Attributes[val])
                else:
                    self.fail(f'{value!r} is of an invalid type.', param, ctx)

            except KeyError:
                attrs.add(Attribute(val, dtype=np.float32))

            except Exception as e:
                self.fail(
                    f'Failed to add {value!r} as an Attribute, {e}', param, ctx
                )
        return list(attrs)


class MetricParamType(click.ParamType):
    """Custom Click parameter type.

    This param accepts names of metric groups or a path to a file containing
    custom metrics.
    """

    name = 'metrics'

    def convert(self, value, param, ctx) -> list[Metric]:
        if value is None or not value:
            return list(grid_metrics.get_grid_metrics('Z').values())
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
                        self.fail(
                            f'Failed to find import file for metrics at {p}',
                            param,
                            ctx,
                        )

                    spec = importlib.util.spec_from_file_location(
                        'user_metrics', str(p)
                    )
                    user_metrics = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(user_metrics)
                    ms = user_metrics.metrics()
                except Exception as e:
                    self.fail(
                        f'Failed to import metrics from {p} with error {e}',
                        param,
                        ctx,
                    )

                for m in ms:
                    if not isinstance(m, Metric):
                        self.fail(f'Invalid Metric supplied: {m}')

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
                    elif 'grid_metric' in val:
                        args = val.split('_')
                        elev_key = 'Z'
                        min_ht = 2
                        ht_break = 3
                        for idx, a in enumerate(args):
                            if idx == 2:
                                elev_key = a
                            elif idx == 3:
                                min_ht = a
                            elif idx == 4:
                                ht_break = a
                            else:
                                continue
                        metrics.update(
                            list(
                                grid_metrics.get_grid_metrics(
                                    elev_key=elev_key,
                                    min_ht=min_ht,
                                    ht_break=ht_break,
                                ).values()
                            )
                        )
                    elif val == 'all':
                        metrics.update(list(all_metrics.values()))
                    else:
                        m = all_metrics[val]
                        if isinstance(m, Metric):
                            metrics.add(m)
                        else:
                            metrics.udpate(list(m))
                except Exception as e:
                    print(e)
                    self.fail(
                        f'{val!r} is not available in Metrics', param, ctx
                    )
        return list(metrics)


def dask_handle(
    dasktype: str,
    scheduler: str,
    workers: int,
    threads: int,
    watch: bool,
) -> None:
    dask_config = {}

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
            cluster = LocalCluster(
                processes=True, n_workers=workers, threads_per_worker=threads
            )
        elif dasktype == 'threads':
            cluster = LocalCluster(
                processes=False, n_workers=workers, threads_per_worker=threads
            )
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
