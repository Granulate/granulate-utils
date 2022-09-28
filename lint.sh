#!/bin/bash
set -e

if [ -f venv/bin/activate ]; then
  source venv/bin/activate
elif [ -f .venv/bin/activate ]; then
  source .venv/bin/activate
fi

check_arg=""
if [[ "$1" = "--ci" ]]; then
    check_arg="--check"
fi

# see also isort --skip and flake8 config.
EXCLUDE_RE='.venv|venv|build|granulate_utils/generated'

isort --settings-path .isort.cfg --skip granulate_utils/generated --skip venv --skip .venv --skip build .
black --line-length 120 $check_arg --exclude $EXCLUDE_RE .
flake8 --config .flake8  --exclude $(echo $EXCLUDE_RE | tr '|' ',') .
mypy --exclude $EXCLUDE_RE .
