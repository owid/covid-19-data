#!/bin/bash
set -e

cd ~/covid-19-data/scripts/scripts/testing
git stash push -- automated_sheets/*
git pull
bash collect_data.sh quick
git add automated_sheets/*
git commit -m "Automated testing collection"
git push
