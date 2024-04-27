## SilviMetric


SilviMetric is an open source library and set of command line utilities for extracting point cloud metrics into a TileDB database. See https://silvimetric.com for documentation and tutorials.

[<img src="https://github.com/hobuinc/silvimetric/blob/main/docs/source/logo/Logos/PNG/SilviMeteric_Logo_2c.png?raw=true">](https://silvimetric.com/)

### Development

GitHub hosts the project at https://github.com/hobuinc/silvimetric


### Installation
These scripts will install `Silvimetric` dependencies as python libraries to the conda environment silvimetric.

`Silvimetric` requires that we install some packages from `conda` (`TileDB`, and `PDAL`) so it's usually easier to use only `conda` to handle your environment. If this is unavailable to you, you will need to install `TileDB` and `PDAL` from source before installing the python packages that are dependent on those (`python-pdal` and `tiledb-py`).

##### Pip and Conda

```
conda env create -f https://raw.githubusercontent.com/hobuinc/silvimetric/main/environment.yml
conda activate silvimetric
pip install silvimetric
```

##### Source

```
git clone https://github.com/hobuinc/silvimetric.git && cd silvimetric
conda env create -f environment.yml && conda activate silvimetric
pip install .
# To install for development:
# pip install -e .
```