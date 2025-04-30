#!/usr/bin/env bash

TODAY=$(date +%Y%m%d)
TODAY_DIR="vote_data/${TODAY}"

mkdir -p $TODAY_DIR

curl --output-dir $TODAY_DIR -z "${TODAY_DIR}/ncvoter60.zip" -O "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter60.zip"
unzip "${TODAY_DIR}/ncvoter60.zip" -d $TODAY_DIR
