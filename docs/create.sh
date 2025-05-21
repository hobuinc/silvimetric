#!/bin/bash

# This assumes you've already done pip install -r requirements.txt in docs dir
conda run -n silvimetric jupyter-book config sphinx source
python -m sphinx -T -b html -d _build/doctrees -D language=en source html