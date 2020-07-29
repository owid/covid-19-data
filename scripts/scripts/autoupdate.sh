#!/bin/bash

set -e

BRANCH="master"
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd )"
SCRIPTS_DIR=$ROOT_DIR/scripts

has_changed() {
  git diff --name-only --exit-code $1 >/dev/null 2>&1
  [ $? -ne 0 ]
}

cd $ROOT_DIR

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

# Switch to scripts/ dir – we will start executing now
cd $SCRIPTS_DIR/scripts

# Attempt to download ECDC CSV
python -c 'import ecdc; ecdc.download_csv()'

# If there are any unstaged changes in the repo, then the
# CSV has changed, and we need to run the update script.
if has_changed $ROOT_DIR/scripts/input/ecdc/releases/latest.csv; then
  echo "Generating ECDC files..."
  python ecdc.py latest.csv --skip-download
  git add .
  git commit -m "Automated update"
  git push
else
  echo "ECDC CSV is up to date"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
python -c 'import ecdc; ecdc.update_db()'
