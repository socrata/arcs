#!/bin/bash

# .env should have a line like
# export CROWDFLOWER_API_KEY="LbcxvIlE3x1M8F6TT5hN"
# you can find (y)ours at http://make.crowdflower.com/account/user
source .env

echo "Crowdflower API Key:" $CROWDFLOWER_API_KEY

python download_crowdflower.py -i 755163 -n
