#!/bin/bash

rm -rf stats
python main.py autzen-classified.copc.laz --stats True --threads 4 --workers 4
rm -rf stats
python main.py autzen-classified.copc.laz --stats True --threads 6 --workers 4
rm -rf stats
python main.py autzen-classified.copc.laz --stats True --threads 8 --workers 4
rm -rf stats
python main.py autzen-classified.copc.laz --stats True --threads 10 --workers 4
rm -rf stats
python main.py autzen-classified.copc.laz --stats True --threads 12 --workers 4

rm -rf stats
python main.py autzen-classified.copc.laz --stats True --threads 4 --workers 6
rm -rf stats
python main.py autzen-classified.copc.laz --stats True --threads 4 --workers 8
rm -rf stats
python main.py autzen-classified.copc.laz --stats True --threads 4 --workers 10
# rm -rf stats
# python main.py autzen-classified.copc.laz --stats True --threads 4 --workers 12