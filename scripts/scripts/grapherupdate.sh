#!/bin/bash

set -e

BRANCH="master"
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd )"
SCRIPTS_DIR=$ROOT_DIR/scripts

cd $ROOT_DIR

# Activate Python virtualenv
source $SCRIPTS_DIR/venv/bin/activate

# Interpret inline Python script in `scripts` directory
run_python() {
  (cd $SCRIPTS_DIR/scripts; python -c "$1")
}

# Make sure we have the latest commit.
git checkout $BRANCH
git pull

# =====================================================================
# Vaccination data

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import vaccinations; vaccinations.update_db()'

# =====================================================================
# Testing data

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import testing; testing.update_db()'

# =====================================================================
# YouGov Imperial COVID-19 behavior tracker data

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import yougov; yougov.update_db()'
