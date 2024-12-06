.. _quickstart:



================================================================================
QuickStart
================================================================================


SilviMetric depends upon |Conda| for packaging support. You must first install
all of SilviMetric's dependencies using Conda:

This tutorial shows you how to :ref:`initialize`, :ref:`shatter`, and
:ref:`extract` data in SilviMetric using the :ref:`cli`.

We are going to use the `Autzen Stadium
<https://viewer.copc.io/?copc=https://s3.amazonaws.com/hobu-lidar/autzen-classified.copc.laz>`__
as our test example.

.. note::

    The Autzen Stadium has units in feet, and this can sometimes be a source of confusion
    for tile settings and such.

Installation
--------------------------------------------------------------------------------

Open a Conda terminal and install necessary dependencies

   .. code-block:: shell-session

      conda env create \
          -f https://raw.githubusercontent.com/hobuinc/silvimetric/main/environment.yml  \
          -n silvimetric

  .. note::

     We are installing the list of dependencies as provided by the SilviMetric
     GitHub listing over the internet.

  .. warning::

     If you are using windows, line continuation characters are ``^`` instead of ``\``

2. Activate the environment:

   .. code-block:: shell-session

     conda activate silvimetric

3. Install SilviMetric:

   .. code-block:: shell-session

     pip install silvimetric

Initialization
--------------------------------------------------------------------------------

Initialize a SilviMetric database.  To initialize a SilviMetric database, we
need a bounds and a coordinate reference system.

1. We first need to determine a bounds for our database. In our case,
    we are going to use PDAL and `jq <https://jqlang.github.io/jq/download/>`__ to
    grab our bounds

    .. code-block:: shell-session

        pdal info https://s3.amazonaws.com/hobu-lidar/autzen-classified.copc.laz  \
            --readers.copc.resolution=1 | jq -c '.stats.bbox.native.bbox'

    Our boundary is emitted in expanded form.

    .. code-block:: text

        {"maxx":639003.73,"maxy":853536.21,"maxz":615.26,"minx":635579.2,"miny":848884.83,"minz":406.46}

    .. note::

        You can express bounds in two additional formats for SilviMetric:

        * ``[635579.2, 848884.83, 639003.73, 853536.21]`` – ``[minx, miny, maxx, maxy]``
        * ``([635579.2,848884.83],[639003.73,853536.2])`` – ``([minx, maxx], [miny, maxy])``

    .. note::

        You can install jq by issuing ``conda install jq -y`` in your environment if
        you are on Linux or Mac. On Windows, you will need to download jq from
        the website and put it in your path. https://jqlang.github.io/jq/download/

2. We need a coordinate reference system for the database. We will grab it from
    the PDAL metadata just like we did for the bounds.

    .. code-block:: shell-session

        pdal info --metadata https://s3.amazonaws.com/hobu-lidar/autzen-classified.copc.laz  \
            --readers.copc.resolution=10 | \
            jq -c '.metadata.srs.json.components[0].id.code'

    Our EPSG code is in the ``pdal info --metadata`` output, and after extracted by jq, we
    can use it.

    .. code-block:: text

        2992

.. note::

    Both a bounds and CRS must be set to initialize a database. We can set them
    to whatever we want, but any data we are inserting into the database must
    match the coordinate system of the SilviMetric database.

3. With bounds and CRS in hand, we can now initialize the database

    .. code-block:: shell-session
        crs="EPSG:2992"
        bounds='{"maxx":639003.73,"maxy":853536.21,"maxz":615.26,"minx":635579.2,"miny":848884.83,"minz":406.46}'
        db_name="autzen-smdb.tdb"
        silvimetric -d ${db_name} \
            initialize \
            --bounds ${bounds} \
            --crs ${crs}

.. note::

    Be careful with your shell's quote escaping rules!


Scan
--------------------------------------------------------------------------------

The `scan` command will tell us information about the pointcloud with respect
to the database we already created, including a best guess at the correct number
of cells per tile, or `tile size`.

    .. code-block:: shell-session
        pointcloud="https://s3.amazonaws.com/hobu-lidar/autzen-classified.copc.laz"
        silvimetric -d ${db_name} scan ${pointcloud}


We should see output like the output below, recommending we use a `tile size`
of 185.

::

    2024-12-06 09:08:07,124 - silvimetric - INFO - info:155 - {
        "pc_info": {
            "storage_bounds": [
                635550.0,
                848880.0,
                639030.0,
                853560.0
            ],
            "data_bounds": [
                635550.0,
                848880.0,
                639030.0,
                853560.0
            ],
            "count": [
                10653336
            ]
        },
        "tile_info": {
            "num_cells": 18096,
            "num_tiles": 220,
            "mean": 82.25454545454545,
            "std_dev": 94.76295175221901,
            "recommended": 177
        }
    }

Shatter
--------------------------------------------------------------------------------

We can now insert data into the SMDB.

If we run this command without the argument `--tilesize`, `Silvimetric` will
determine a tile size for you. The method will be the same as the `Scan` method,
but will filter out the tiles that have no data in them.

   .. code-block:: shell-session

     silvimetric -d ${db_name} \
        shatter \
        --date 2008-12-01 \
        ${pointcloud}

If we grab the tile size from the `scan` that we ran earlier, we'll skip the
filtering step.

   .. code-block:: shell-session

     silvimetric -d ${db_name} \
        shatter \
        --tilesize 185 \
        --date 2008-12-01 \
        ${pointcloud}

   .. code-block:: text

     2024-12-06 10:36:59,605 - silvimetric - INFO - info:155 - Beginning shatter process...
     [########################################] | 100% Completed | 62.31 s
     2024-12-06 10:38:05,076 - silvimetric - INFO - info:155 - Consolidated time slot 1.

Extract
--------------------------------------------------------------------------------

After data is inserted, we can extract it into different rasters. When we created
the database we gave it a list of `Attributes` and `Metrics`. When we ran
`Shatter`, we filled in the values for those in each cell. If we have a database
with the `Attributes` Intensity and Z, in combination with the `Metrics` min and
max, each cell will contain values for `min_Intensity`, `max_Intensity`,
`min_Z`, and `max_Z`. This is also the list of available rasters we can extract.

   .. code-block:: shell-session

        silvimetric -d ${db_name} extract -o output-directory

   .. code-block:: text

        2024-12-06 10:42:21,767 - silvimetric - INFO - info:155 - Collecting database information...
        2024-12-06 10:42:23,399 - silvimetric - INFO - info:155 - Writing rasters to output-directory

Info
--------------------------------------------------------------------------------

We can query past shatter processes and the schema for the database with the
Info call.

    .. code-block:: shell-session

        silvimetric -d ${db_name} info --history

This will print out a JSON object containing information about the current state
of the database. We can find the `name` key here, which necessary for
`Delete`, `Restart`, and `Resume`. For the following commands we will have
copied the value of the `name` key in the variable `uuid`.

Delete
--------------------------------------------------------------------------------

We can also remove a `shatter` process by using the `delete` command. This will
remove all data associated with that shatter process from the database, but
will leave an updated config of it in the database config should you want to
reference it later.

    .. code-block:: shell-session

        silvimetric -d autzen-smdb.tdb delete --id $uuid

Restart
--------------------------------------------------------------------------------

If you would like to rerun a `Shatter` process, whether or not it was previously
finished, you can use the `restart` command. This will call the `delete` method
and use the config from that to re-run the `shatter` process.

    .. code-block:: shell-session

        silvimetric -d autzen-smdb.tdb restart --id $uuid

Resume
--------------------------------------------------------------------------------

If a `Shatter` process is cancelled partway through, we can pick up where we
left off with the `Resume` command.

   .. code-block:: shell-session

        silvimetric -d autzen-smdb.tdb resume --id $uuid

.. include:: ./substitutions.txt
