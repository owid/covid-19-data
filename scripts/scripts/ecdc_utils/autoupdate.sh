#!/bin/bash

set -e

BRANCH="autoupdate"
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../../.. && pwd )"
SCRIPTS_DIR=$ROOT_DIR/scripts
LATEST_CSV_PATH=$ROOT_DIR/scripts/input/ecdc/releases/latest.csv

has_changed() {
  git diff --name-only --exit-code $1 >/dev/null 2>&1
  [ $? -ne 0 ]
}

# Activate Python virtualenv
source $SCRIPTS_DIR/venv/bin/activate

# Make sure we have the latest commit.
# Stash uncommitted changes.
git add .
git stash
git checkout $BRANCH
git pull
# Avoiding the more destructive `git reset --hard origin/$BRANCH`
# for now, which would be more robust, but can easily lead to some
# accidental code loss while testing locally.

# Attempt to download ECDC CSV
python $SCRIPTS_DIR/scripts/ecdc_utils/download_csv.py

# If there are any unstaged changes in the repo, then the
# CSV has changed, and we need to run the update script.
if has_changed $LATEST_CSV_PATH; then
  echo "Generating ECDC files..."
  python $SCRIPTS_DIR/scripts/ecdc.py latest.csv --skip-download
  git add .
  git commit -m "Automated update"
  git push
else
  echo "ECDC CSV is up to date"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
python $SCRIPTS_DIR/scripts/ecdc_utils/update_db.py
