#!/bin/bash

set -e

for arg in "$@"
do
    case $arg in
        -m=*|--mode=*)
        MODE="${arg#*=}"
        shift
        ;;
    esac
done

if [ "$MODE" != "quick" ] && [ "$MODE" != "update" ]; then
    echo "Wrong execution mode ('quick' or 'update')."
    exit
fi

cd ~/Git/covid-19-data
git pull

cd ~/Git/covid-19-data/scripts/scripts/testing
python3 run_python_scripts.py "$MODE"
Rscript run_r_scripts.R "$MODE"
