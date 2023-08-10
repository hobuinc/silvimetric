#!/bin/bash
cd $(dirname "${0}")
ARCH=osx-arm64

if [[ "${1}" != "" ]]
then
    ARCH=$1
fi

echo "Building in architecture: ${ARCH}"

CONDA_SUBDIR=$ARCH conda env create -f env.yaml
conda activate fusion