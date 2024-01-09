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
            --readers.copc.resolution=10 | jq -c '.stats.bbox.native.bbox'

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

        silvimetric autzen-smdb.tdb \
            initialize \
            '{"maxx":639003.73,"maxy":853536.21,"maxz":615.26,"minx":635579.2,"miny":848884.83,"minz":406.46}' \
            EPSG:2992

.. note::

    Be careful with your shell's quote escaping rules!


Shatter
--------------------------------------------------------------------------------

We can now insert data into the SMDB

   .. code-block:: shell-session

     silvimetric autzen-smdb.tdb \
        shatter \
        https://s3.amazonaws.com/hobu-lidar/autzen-classified.copc.laz \
        --threads 10 \
        --watch

Extract
--------------------------------------------------------------------------------

After data is inserted, we can extract

   .. code-block:: shell-session

     silvimetric autzen-smdb.tdb extract -o output-directory

.. include:: ./substitutions.txt