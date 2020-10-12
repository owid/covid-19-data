#!/bin/bash

USER_EMAIL=

cd ~/covid-19-data/scripts/scripts/testing
git stash push -- automated_sheets/*
git pull
bash collect_data.sh quick &> auto_quick_collect.log
cat auto_quick_collect.log | mail -s "Automated testing collection - `date`" $USER_EMAIL
git add automated_sheets/*
git commit -m "Automated testing collection"
git push
