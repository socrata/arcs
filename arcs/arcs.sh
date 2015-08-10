#!/bin/bash

##################################################
# This script is meant to be a wrapper around the
# entire data gathering, processing, playing with
# experience.
#
# TODO: add more functionality
##################################################

dirname=`date +%Y%m%d`

echo "Retrieving logs from lb02"
rsync -avz lb02:/data/nginx-logs/*2.gz $dirname

echo "Parsing logs into json"
for x in `ls $dirname`; do gzcat $dirname/$x | python logparser.py; done > $dirname.logs.concat.json

echo "Gathering data for annotation task"
python collection.py -j $dirname.logs.concat.json
