#!/bin/bash
cd $(dirname "${0}")
ARCH=osx-arm64

if [[ "${1}" != "" ]]
then
    ARCH=$1
fi

echo "Building in architecture: ${ARCH}"

CONDA_SUBDIR=$ARCH conda env create -f env.yml
conda activate fusion
conda config --env --set subdir $ARCH