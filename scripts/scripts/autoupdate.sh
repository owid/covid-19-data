#!/bin/bash

set -e

BRANCH="master"
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd )"
SCRIPTS_DIR=$ROOT_DIR/scripts

has_changed() {
  git diff --name-only --exit-code $1 >/dev/null 2>&1
  [ $? -ne 0 ]
}

has_changed_gzip() {
  # Ignore the header because it includes the creation time
  cmp --silent -i 8 $1 <(git show HEAD:$1)
  [ $? -ne 0 ]
}

cd $ROOT_DIR

# Activate Python virtualenv
source $SCRIPTS_DIR/venv/bin/activate

# Interpret inline Python script in `scripts` directory
run_python() {
  (cd $SCRIPTS_DIR/scripts; python -c "$1")
}

# Make sure we have the latest commit.
# Stash uncommitted changes.
git add .
git stash
git checkout $BRANCH
git pull
# Avoiding the more destructive `git reset --hard origin/$BRANCH`
# for now, which would be more robust, but can easily lead to some
# accidental code loss while testing locally.

# =====================================================================
# ECDC

# Attempt to download ECDC CSV
run_python 'import ecdc; ecdc.download_csv()'

# If there are any unstaged changes in the repo, then the
# CSV has changed, and we need to run the update script.
if has_changed ./scripts/input/ecdc/releases/latest.csv; then
  echo "Generating ECDC files..."
  python $SCRIPTS_DIR/scripts/ecdc.py latest.csv --skip-download
  git add .
  git commit -m "Automated ECDC update"
  git push
else
  echo "ECDC export is up to date"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import ecdc; ecdc.update_db()'

# =====================================================================
# Google Mobility

# Download CSV
run_python 'import gmobility; gmobility.download_csv()'

# If there are any unstaged changes in the repo, then the
# CSV has changed, and we need to run the update script.
if has_changed_gzip ./scripts/input/gmobility/latest.csv.gz; then
  echo "Generating Google Mobility export..."
  run_python 'import gmobility; gmobility.export_grapher()'
  git add .
  git commit -m "Automated Google Mobility update"
  git push
else
  echo "Google Mobility export is up to date"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import gmobility; gmobility.update_db()'

# =====================================================================
# Policy responses

# Download CSV
run_python 'import oxcgrt; oxcgrt.download_csv()'

# If there are any unstaged changes in the repo, then the
# CSV has changed, and we need to run the update script.
if has_changed ./scripts/input/bsg/latest.csv; then
  echo "Generating OxCGRT export..."
  run_python 'import oxcgrt; oxcgrt.export_grapher()'
  git add .
  git commit -m "Automated OxCGRT update"
  git push
else
  echo "OxCGRT export is up to date"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import oxcgrt; oxcgrt.update_db()'
